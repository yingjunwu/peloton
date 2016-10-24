//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// command_logger.cpp
//
// Identification: src/backend/logging/command_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <algorithm>
#include <dirent.h>
#include <cstdio>
#include <backend/gc/gc_manager_factory.h>
#include <backend/concurrency/transaction_manager_factory.h>

#include "backend/catalog/manager.h"
#include "backend/concurrency/epoch_manager_factory.h"
#include "backend/expression/container_tuple.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"

#include "backend/logging/command_logger.h"

namespace peloton {
namespace logging {

void CommandLogger::RegisterWorker(WorkerContext *command_worker_ctx) {
  worker_map_lock_.Lock();
  worker_map_[command_worker_ctx->worker_id].reset(command_worker_ctx);
  worker_map_lock_.Unlock();
}

void CommandLogger::DeregisterWorker(WorkerContext *command_worker_ctx) {
  worker_map_lock_.Lock();
  worker_map_.erase(command_worker_ctx->worker_id);
  worker_map_lock_.Unlock();
}


void CommandLogger::StartRecovery(const size_t checkpoint_eid, const size_t persist_eid, const size_t recovery_thread_count) {

  GetSortedLogFileIdList(checkpoint_eid, persist_eid);
  printf("recovery_thread_count = %lu\n", recovery_thread_count);
  recovery_pools_.resize(recovery_thread_count);
  recovery_threads_.resize(recovery_thread_count);

  for (size_t i = 0; i < recovery_thread_count; ++i) {
    
    recovery_pools_[i].reset(new VarlenPool(BACKEND_TYPE_MM));
    
    recovery_threads_[i].reset(new std::thread(&CommandLogger::RunRecoveryThread, this, i, checkpoint_eid, persist_eid));
  }
}

void CommandLogger::WaitForRecovery() {
  for (auto &recovery_thread : recovery_threads_) {
    recovery_thread->join();
  }
}

void CommandLogger::GetSortedLogFileIdList(const size_t checkpoint_eid, const size_t persist_eid) {
  // Open the log dir
  struct dirent *file;
  DIR *dirp;
  dirp = opendir(this->log_dir_.c_str());
  if (dirp == nullptr) {
    LOG_ERROR("Can not open log directory %s\n", this->log_dir_.c_str());
    exit(EXIT_FAILURE);
  }


  concurrency::EpochManager &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  size_t file_epoch_count = (size_t)(new_file_interval_ / epoch_manager.GetEpochDurationMilliSecond());

  // Filter out all log files
  std::string base_name = logging_filename_prefix_ + "_" + std::to_string(logger_id_) + "_";

  file_eids_.clear();

  while ((file = readdir(dirp)) != nullptr) {
    if (strncmp(file->d_name, base_name.c_str(), base_name.length()) == 0) {
      // Find one log file
      LOG_TRACE("Logger %d find a log file %s\n", (int) logger_id_, file->d_name);
      // Get the file epoch id
      size_t file_eid = (size_t)std::stoi(std::string(file->d_name + base_name.length()));

      if (file_eid + file_epoch_count > checkpoint_eid && file_eid <= persist_eid) {
        file_eids_.push_back(file_eid);
      }

    }
  }

  // Sort in descending order
  std::sort(file_eids_.begin(), file_eids_.end(), std::greater<size_t>());
  max_replay_file_id_ = file_eids_.size() - 1;

  for (auto &entry : file_eids_) {
    printf("file id = %lu\n", entry);
  }

}

bool CommandLogger::ReplayLogFile(const size_t thread_id, FileHandle &file_handle, size_t checkpoint_eid, size_t persist_eid) {
  PL_ASSERT(file_handle.file != nullptr && file_handle.fd != INVALID_FILE_DESCRIPTOR);

  // Status
  size_t current_eid = INVALID_EPOCH_ID;
  cid_t current_cid = INVALID_CID;
  size_t buf_size = 4096;
  std::unique_ptr<char[]> buffer(new char[buf_size]);
  char length_buf[sizeof(int32_t)];
  int transaction_type = INVALID_TRANSACTION_TYPE;
  OperationSet *operation_set = nullptr;

  // TODO: Need some file integrity check. Now we just rely on the the pepoch id and the checkpoint eid
  while (true) {
    // Read the frame length
    if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) &length_buf, 4) == false) {
      LOG_TRACE("Reach the end of the log file");
      break;
    }
    CopySerializeInputBE length_decode((const void *) &length_buf, 4);
    int length = length_decode.ReadInt();
    // printf("length = %d\n", length);
    // Adjust the buffer
    if ((size_t) length > buf_size) {
      buffer.reset(new char[(int)(length * 1.2)]);
      buf_size = (size_t) length;
    }

    if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) buffer.get(), length) == false) {
      LOG_ERROR("Unexpected file eof");
      // TODO: How to handle damaged log file?
      return false;
    }
    CopySerializeInputBE record_decode((const void *) buffer.get(), length);

    // Check if we can skip this epoch
    if (current_eid != INVALID_EPOCH_ID && (current_eid < checkpoint_eid || current_eid > persist_eid)) {
      // Skip the record if current epoch is before the checkpoint or after persistent epoch
      continue;
    }

    /*
     * Decode the record
     */
    // Get record type
    LogRecordType record_type = (LogRecordType) (record_decode.ReadEnumInSingleByte());
    switch (record_type) {
      case LOGRECORD_TYPE_EPOCH_BEGIN: {
        LOG_TRACE("epoch begin");
        if (current_eid != INVALID_EPOCH_ID) {
          LOG_ERROR("Incomplete epoch in log record");
          return false;
        }
        current_eid = (size_t) record_decode.ReadLong();
        // printf("begin epoch id = %lu\n", current_eid);
        break;
      } case LOGRECORD_TYPE_EPOCH_END: {
        LOG_TRACE("epoch end");
        size_t eid = (size_t) record_decode.ReadLong();
        if (eid != current_eid) {
          LOG_ERROR("Mismatched epoch in log record");
          return false;
        }
        // printf("end epoch id = %lu\n", current_eid);
        current_eid = INVALID_EPOCH_ID;
        break;
      } case LOGRECORD_TYPE_TRANSACTION_BEGIN: {
        LOG_TRACE("transaction begin");
        if (current_eid == INVALID_EPOCH_ID) {
          LOG_ERROR("Invalid txn begin record");
          return false;
        }
        if (current_cid != INVALID_CID) {
          LOG_ERROR("Incomplete txn in log record");
          return false;
        }
        current_cid = (cid_t) record_decode.ReadLong();
        
        // if (current_eid >= checkpoint_eid && current_eid <= persist_eid) {
        //   concurrency::current_txn = new concurrency::Transaction(thread_id, current_cid);
        // }

        break;
      } case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
        LOG_TRACE("transaction end");
        if (current_eid == INVALID_EPOCH_ID) {
          LOG_ERROR("Invalid txn begin record");
          return false;
        }
        cid_t cid = (cid_t) record_decode.ReadLong();
        if (cid != current_cid) {
          LOG_ERROR("Mismatched txn in log record");
          return false;
        }
        current_cid = INVALID_CID;

        // if (current_eid >= checkpoint_eid && current_eid <= persist_eid) {
        //   delete concurrency::current_txn;
        //   concurrency::current_txn = nullptr;
        // }

        if (current_eid >= checkpoint_eid && current_eid <= persist_eid) {
          if (transaction_type == INVALID_TRANSACTION_TYPE) {
            PL_ASSERT(operation_set != nullptr);

            param_wrappers_[thread_id].emplace_back(current_cid, transaction_type, operation_set);

            operation_set = nullptr;
          }
        }


        break;
      } case LOGRECORD_TYPE_TUPLE_UPDATE:
        case LOGRECORD_TYPE_TUPLE_DELETE:
        case LOGRECORD_TYPE_TUPLE_INSERT: {
        if (current_cid == INVALID_CID || current_eid == INVALID_EPOCH_ID) {
          LOG_ERROR("Invalid txn tuple record");
          return false;
        }

        if (current_eid < checkpoint_eid || current_eid > persist_eid) {
          break;
        }

        oid_t database_id = (oid_t) record_decode.ReadLong();
        oid_t table_id = (oid_t) record_decode.ReadLong();

        // XXX: We still rely on an alive catalog manager
        auto table = catalog::Manager::GetInstance().GetTableWithOid(database_id, table_id);
        auto schema = table->GetSchema();

        // Decode the tuple from the record
        std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
        tuple->DeserializeFrom(record_decode, this->recovery_pools_[thread_id].get());

        PL_ASSERT(operation_set != nullptr);
        operation_set->emplace_back(record_type, tuple.get(), table);
        // FILE *fp = fopen("in_file.txt", "a");
        // for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
        //   int value = ValuePeeker::PeekAsInteger(tuple->GetValue(i));
        //   fprintf(stdout, "%d, ", value);
        // }
        // fprintf(stdout, "\n");
        // fclose(fp);

        // Install the record
        // InstallTupleRecord(record_type, tuple.get(), table, current_cid);
        break;
      } case LOGRECORD_TYPE_TRANSACTION_TYPE: {
        if (current_cid == INVALID_CID || current_eid == INVALID_EPOCH_ID) {
          LOG_ERROR("Invalid transaction type");
          return false;
        }
        transaction_type = record_decode.ReadLong();

        if (transaction_type == INVALID_TRANSACTION_TYPE) {
          operation_set = new OperationSet();
        }

        break;
      } case LOGRECORD_TYPE_PARAMETER: {
        if (current_cid == INVALID_CID || current_eid == INVALID_EPOCH_ID) {
          LOG_ERROR("Invalid transaction parameter");
          return false;
        }

        if (transaction_type == INVALID_TRANSACTION_TYPE) {
          LOG_ERROR("Invalid transaction parameter");
          return false;
        }

        if (current_eid < checkpoint_eid || current_eid > persist_eid) {
          break;
        }

        TransactionParameter *param = DeserializeParameter(transaction_type, record_decode);

        param_wrappers_[thread_id].emplace_back(current_cid, transaction_type, param);

        break;
      }
      default:
        LOG_ERROR("Unknown log record type");
        return false;
      }

  }
  printf("param wrapper count = %lu\n", param_wrappers_[thread_id].size());
  return true;
}


void CommandLogger::RunRecoveryThread(const size_t thread_id, const size_t checkpoint_eid, const size_t persist_eid) {

  while (true) {

    int replay_file_id = max_replay_file_id_.fetch_sub(1, std::memory_order_relaxed);
    printf("replay file id = %d\n", replay_file_id);
    if (replay_file_id < 0) {
      break;
    }

    size_t file_eid = file_eids_.at(replay_file_id);
    printf("start replaying file eid = %lu\n", file_eid);
    // Replay a single file
    std::string filename = GetLogFileFullPath(file_eid);
    FileHandle file_handle;
    // std::cout<<"filename = " << filename << std::endl;
    bool res = LoggingUtil::OpenFile(filename.c_str(), "rb", file_handle);
    if (res == false) {
      LOG_ERROR("Cannot open log file %s\n", filename.c_str());
      exit(EXIT_FAILURE);
    }
    ReplayLogFile(thread_id, file_handle, checkpoint_eid, persist_eid);

    // Safely close the file
    res = LoggingUtil::CloseFile(file_handle);
    if (res == false) {
      LOG_ERROR("Cannot close pepoch file");
      exit(EXIT_FAILURE);
    }

  }
}


void CommandLogger::Run() {
  // TODO: Ensure that we have called run recovery before

  concurrency::EpochManager &epoch_manager = concurrency::EpochManagerFactory::GetInstance();

  size_t file_epoch_count = (size_t)(new_file_interval_ / epoch_manager.GetEpochDurationMilliSecond());

  std::list<std::pair<FileHandle*, size_t>> file_handles;

  size_t current_file_eid = 0; //epoch_manager.GetCurrentEpochId();

  FileHandle *new_file_handle = new FileHandle();
  file_handles.push_back(std::make_pair(new_file_handle, current_file_eid));
  
  std::string filename = GetLogFileFullPath(current_file_eid);
  // Create a new file
  if (LoggingUtil::OpenFile(filename.c_str(), "wb", *new_file_handle) == false) {
    LOG_ERROR("Unable to create log file %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }

  /**
   *  Main loop
   */
  while (true) {
    if (is_running_ == false) { break; }

    std::this_thread::sleep_for(
       std::chrono::microseconds(epoch_manager.GetEpochLengthInMicroSecQuarter()));

    // Pull log records from workers per epoch buffer
    {
      worker_map_lock_.Lock();

      size_t min_workers_persist_eid = INVALID_EPOCH_ID;
      for (auto &worker_entry : worker_map_) {
        // For every alive worker, move its buffer to the logger's local buffer
        auto worker_ctx_ptr = worker_entry.second.get();

        size_t last_persist_eid = worker_ctx_ptr->persist_eid;
        size_t worker_current_eid = worker_ctx_ptr->current_commit_eid;

        PL_ASSERT(last_persist_eid <= worker_current_eid);

        if (last_persist_eid == worker_current_eid) {
          // The worker makes no progress
          continue;
        }

        for (size_t epoch_id = last_persist_eid + 1; epoch_id < worker_current_eid; ++epoch_id) {

          size_t epoch_idx = epoch_id % epoch_manager.GetEpochQueueCapacity();
          // get all the buffers that are associated with the epoch.
          auto &buffers = worker_ctx_ptr->per_epoch_buffer_ptrs[epoch_idx];

          if (buffers.empty() == true) {
            // no transaction log is generated within this epoch.
            // or we have logged this epoch before.
            // just skip it.
            continue;
          }

          // if we have something to write, then check whether we need to create new file.
          FileHandle *file_handle = nullptr;
          
          for (auto &entry : file_handles) {
            if (epoch_id >= entry.second && epoch_id < entry.second + file_epoch_count) {
              file_handle = entry.first;
            }
          }
          while (file_handle == nullptr) {
            current_file_eid = current_file_eid + file_epoch_count;
            printf("create new file with epoch id = %lu, last persist eid = %lu, current eid = %lu\n", current_file_eid, last_persist_eid, worker_current_eid);
            FileHandle *new_file_handle = new FileHandle();
            file_handles.push_back(std::make_pair(new_file_handle, current_file_eid));

            std::string filename = GetLogFileFullPath(current_file_eid);
            // Create a new file
            if (LoggingUtil::OpenFile(filename.c_str(), "wb", *new_file_handle) == false) {
              LOG_ERROR("Unable to create log file %s\n", filename.c_str());
              exit(EXIT_FAILURE);
            }
            if (epoch_id >= current_file_eid && epoch_id < current_file_eid + file_epoch_count) {
              file_handle = new_file_handle;
              break;
            }
          }


          PersistEpochBegin(*file_handle, epoch_id);
          // persist all the buffers.
          while (buffers.empty() == false) {
            // Check if the buffer is empty
            // TODO: is it possible to have an empty buffer??? --YINGJUN
            if (buffers.top()->Empty()) {
              worker_ctx_ptr->buffer_pool.PutBuffer(std::move(buffers.top()));
            } else {
              PersistLogBuffer(*file_handle, std::move(buffers.top()));
            }
            PL_ASSERT(buffers.top() == nullptr);
            buffers.pop();
          }
          PersistEpochEnd(*file_handle, epoch_id);
          // Call fsync
          LoggingUtil::FFlushFsync(*file_handle);
        } // end for

        worker_ctx_ptr->persist_eid = worker_current_eid - 1;

        if (min_workers_persist_eid == INVALID_EPOCH_ID || min_workers_persist_eid > (worker_current_eid - 1)) {
          min_workers_persist_eid = worker_current_eid - 1;
        }

      } // end for

      if (min_workers_persist_eid == INVALID_EPOCH_ID) {
        // in this case, it is likely that there's no registered worker or there's nothing to persist.
        worker_map_lock_.Unlock();
        continue;
      }

      PL_ASSERT(min_workers_persist_eid >= persist_epoch_id_);

      persist_epoch_id_ = min_workers_persist_eid;

      auto list_iter = file_handles.begin();

      while (list_iter != file_handles.end()) {
        if (list_iter->second + file_epoch_count <= persist_epoch_id_) {
          FileHandle *file_handle = list_iter->first;
        
          // Safely close the file
          bool res = LoggingUtil::CloseFile(*file_handle);
          if (res == false) {
            LOG_ERROR("Cannot close log file under directory %s", log_dir_.c_str());
            exit(EXIT_FAILURE);
          }
          
          delete file_handle;
          file_handle = nullptr;

          list_iter = file_handles.erase(list_iter);

        } else {
          ++list_iter;
        }
      }


      worker_map_lock_.Unlock();
    }
  }

  // Close the log file
  // TODO: Seek and write the integrity information in the header

  // Safely close all the files

  auto list_iter = file_handles.begin();

  while (list_iter != file_handles.end()) {
    FileHandle *file_handle = list_iter->first;

    bool res = LoggingUtil::CloseFile(*file_handle);
    if (res == false) {
      LOG_ERROR("Cannot close log file under directory %s", log_dir_.c_str());
      exit(EXIT_FAILURE);
    }

    delete file_handle;
    file_handle = nullptr;

    list_iter = file_handles.erase(list_iter);
  }
}

void CommandLogger::PersistEpochBegin(FileHandle &file_handle, const size_t epoch_id) {
  // Write down the epoch begin record  
  LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_BEGIN, epoch_id);

  logger_output_buffer_.Reset();

  size_t start = logger_output_buffer_.Position();
  logger_output_buffer_.WriteInt(0);

  logger_output_buffer_.WriteEnumInSingleByte(LOGRECORD_TYPE_EPOCH_BEGIN);
  logger_output_buffer_.WriteLong((uint64_t) epoch_id);

  logger_output_buffer_.WriteIntAt(start, (int32_t) (logger_output_buffer_.Position() - start - sizeof(int32_t)));

  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle.file);
}

void CommandLogger::PersistEpochEnd(FileHandle &file_handle, const size_t epoch_id) {
  // Write down the epoch end record
  LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_END, epoch_id);

  logger_output_buffer_.Reset();

  size_t start = logger_output_buffer_.Position();
  logger_output_buffer_.WriteInt(0);

  logger_output_buffer_.WriteEnumInSingleByte(LOGRECORD_TYPE_EPOCH_END);
  logger_output_buffer_.WriteLong((uint64_t) epoch_id);

  logger_output_buffer_.WriteIntAt(start, (int32_t) (logger_output_buffer_.Position() - start - sizeof(int32_t)));

  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle.file);

}

void CommandLogger::PersistLogBuffer(FileHandle &file_handle, std::unique_ptr<LogBuffer> log_buffer) {

  fwrite((const void *) (log_buffer->GetData()), log_buffer->GetSize(), 1, file_handle.file);

  // Return the buffer to the worker
  log_buffer->Reset();
  auto itr = worker_map_.find(log_buffer->GetWorkerId());
  if (itr != worker_map_.end()) {
    itr->second->buffer_pool.PutBuffer(std::move(log_buffer));
  } else {
    // In this case, the worker is already terminated and removed
    // Release the buffer
    log_buffer.reset(nullptr);
  }
}


}
}