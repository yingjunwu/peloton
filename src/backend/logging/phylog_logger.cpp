//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// phylog_logger.cpp
//
// Identification: src/backend/logging/phylog_logger.cpp
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

#include "backend/catalog/manager.h"
#include "backend/concurrency/epoch_manager_factory.h"
#include "backend/expression/container_tuple.h"
#include "backend/logging/logging_util.h"
#include "backend/logging/phylog_log_manager.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"

namespace peloton {
namespace logging {

void PhyLogLogger::RegisterWorker(WorkerLogContext *worker_log_ctx) {
  worker_map_lock_.Lock();
  worker_map_[worker_log_ctx->worker_id].reset(worker_log_ctx);
  worker_map_lock_.Unlock();
}

void PhyLogLogger::DeregisterWorker(WorkerLogContext *worker_log_ctx) {
  worker_map_lock_.Lock();
  worker_map_.erase(worker_log_ctx->worker_id);
  worker_map_lock_.Unlock();
}

std::vector<int> PhyLogLogger::GetSortedLogFileIdList() {
  // Open the log dir
  struct dirent *file;
  DIR *dirp;

  dirp = opendir(this->log_dir_.c_str());
  if (dirp == nullptr) {
    LOG_ERROR("Can not open log direcotry %s\n", this->log_dir_.c_str());
    exit(EXIT_FAILURE);
  }

  // Filter out all log files
  std::string base_name = logging_filename_prefix_ + "_" + std::to_string(logger_id_) + "_";
  std::vector<int> file_ids;

  while ((file = readdir(dirp)) != nullptr) {
    if (strncmp(file->d_name, base_name.c_str(), base_name.length()) == 0) {
      // Find one log file
      LOG_TRACE("Logger %d find a log file %s\n", (int) logger_id_, file->d_name);
      // Get the file id
      std::stoi(std::string(file->d_name + base_name.length()));
    }
  }

  // Sort in descending order
  std::sort(file_ids.begin(), file_ids.end(), std::greater<int>());
  return file_ids;
}

void PhyLogLogger::UnlockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset) {
  while (tg_header->SetAtomicTransactionId(tuple_offset, (txn_id_t) (START_TXN_ID + this->logger_id_) == false)) {
    _mm_pause();
  }
}

void PhyLogLogger::UnlockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset) {
  tg_header->SetAtomicTransactionId(tuple_offset, (txn_id_t) (START_TXN_ID + this->logger_id_), INITIAL_TXN_ID);
}

bool PhyLogLogger::InstallTupleRecord(LogRecordType type, storage::Tuple *tuple, storage::DataTable *table, cid_t cur_cid) {
  // First do an index look up, if current version is newer, skip this record
  auto pindex = table->GetIndexWithOid(table->GetPrimaryIndexOid());
  auto pindex_schema = pindex->GetKeySchema();
  PL_ASSERT(pindex);

  std::unique_ptr<storage::Tuple> key(new storage::Tuple(pindex_schema, true));
  key->SetFromTuple(tuple, pindex_schema->GetIndexedColumns(), this->temp_pool_.get());

  std::vector<ItemPointer *> itemptr_ptrs;
  pindex->ScanKey(key.get(), itemptr_ptrs);

  if (itemptr_ptrs.empty()) {
    // Try to insert a new tuple

    // First allocate the tuple in a tile group


    // Initialize the tuple's header

    // Use the conditional insert interface to insert the tuple

    // If failed, fall back to the else branch --> Try to overwrite. Remember to recycle the used itempointer (notify GC)

    // Insert the tuple in all secondary indexes

    // Yield ownership

    return true;
  }

  // Try to overwrite the tuple

  PL_ASSERT(itemptr_ptrs.size() == 1); // Primary index property
  ItemPointer itemptr = *(itemptr_ptrs.front());
  auto tg = catalog::Manager::GetInstance().GetTileGroup(itemptr.block);
  PL_ASSERT(tg);
  auto tg_header = tg->GetHeader();

  // Acquire the ownership of the tuple
  UnlockTuple(tg_header, itemptr.offset);

  // Check if we have a newer version of that tuple
  auto old_cid = tg_header->GetBeginCommitId(itemptr.offset);
  if (old_cid < cur_cid) {
    // TODO: Overwrite the old version, delte/re-insert all secondary indexes
  }

  // Release the ownership
  UnlockTuple(tg_header, itemptr.offset);
  return true;
}

bool PhyLogLogger::ReplayLogFile(FileHandle &file_handle, size_t checkpoint_eid, size_t pepoch_eid) {
  PL_ASSERT(file_handle.file != nullptr && file_handle.fd != INVALID_FILE_DESCRIPTOR);

  // Status
  size_t current_eid = INVALID_EPOCH_ID;
  cid_t current_cid = INVALID_CID;
  size_t buf_size = 4096;
  std::unique_ptr<char[]> buffer(new char[buf_size]);
  char length_buf[sizeof(int32_t)];

  // TODO: Need some file integrity check. Now we just rely on the the pepoch id and the checkpoint eid
  while (true) {
    // Read the frame length
    if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) &length_buf, 4) == false) {
      LOG_TRACE("Reach the end of the log file");
      break;
    }
    CopySerializeInput length_decode((const void *) &length_buf, 4);
    int length = length_decode.ReadInt();

    // Adjust the buffer
    if (length > buf_size) {
      buffer.reset(new char[length * 1.2]);
      buf_size = (size_t) length;
    }

    if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) buffer.get(), length) == false) {
      LOG_ERROR("Unexpected file eof");
      // TODO: How to handle damaged log file?
      return false;
    }
    CopySerializeInput record_decode((const void *) buffer.get(), length);

    // Check if we can skip this epoch
    if (current_eid != INVALID_EPOCH_ID && (current_eid < checkpoint_eid || current_eid > pepoch_eid)) {
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
        if (current_eid != INVALID_EPOCH_ID) {
          LOG_ERROR("Incomplete epoch in log record");
          return false;
        }
        current_eid = (size_t) record_decode.ReadLong();
        break;
      } case LOGRECORD_TYPE_EPOCH_END: {
        size_t eid = (size_t) record_decode.ReadLong();
        if (eid != current_eid) {
          LOG_ERROR("Mismatched epoch in log record");
          return false;
        }
        current_eid = INVALID_EPOCH_ID;
        break;
      } case LOGRECORD_TYPE_TRANSACTION_BEGIN: {
        if (current_eid == INVALID_EPOCH_ID) {
          LOG_ERROR("Invalid txn begin record");
          return false;
        }
        if (current_cid != INVALID_CID) {
          LOG_ERROR("Incomplete txn in log record");
          return false;
        }
        current_cid = (cid_t) record_decode.ReadLong();
        break;
      } case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
        if (current_eid == INVALID_EPOCH_ID) {
          LOG_ERROR("Invalid txn begin record");
          return false;
        }
        cid_t cid = (cid_t) record_decode.ReadLong();
        if (cid != current_cid) {
          LOG_ERROR("Mismatched txn in log record");
          return false;
        }
        break;
      } case LOGRECORD_TYPE_TUPLE_UPDATE:
        case LOGRECORD_TYPE_TUPLE_DELETE:
        case LOGRECORD_TYPE_TUPLE_INSERT: {
        if (current_cid == INVALID_CID || current_eid == INVALID_EPOCH_ID) {
          LOG_ERROR("Invalid txn tuple record");
          return false;
        }

        oid_t database_id = (oid_t) record_decode.ReadLong();
        oid_t table_id = (oid_t) record_decode.ReadLong();

        // XXX: We still rely on an alive catalog manager
        auto table = catalog::Manager::GetInstance().GetTableWithOid(database_id, table_id);
        auto schema = table->GetSchema();

        // Decode the tuple from the record
        std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
        tuple->DeserializeFrom(record_decode, this->recovery_pool_.get());

        // Install the record
        InstallTupleRecord(record_type, tuple.get(), table, current_cid);
        break;
      }
      default:
        LOG_ERROR("Unknown log record type");
        return false;
      }

  }

  return true;
}

void PhyLogLogger::RunRecovery(size_t checkpoint_eid, size_t persist_eid) {
  // Get all log files, replay them in the reverse order
  std::vector<int> file_ids = GetSortedLogFileIdList();

  for (auto fid : file_ids) {
    // Replay a single file
    std::string filename = GetLogFileFullPath(fid);
    FileHandle file_handle;
    bool res = LoggingUtil::OpenFile(filename.c_str(), "rb", file_handle);
    if (res == false) {
      LOG_ERROR("Cannot open log file %s\n", filename.c_str());
      exit(EXIT_FAILURE);
    }
    ReplayLogFile(file_handle, checkpoint_eid, persist_eid);
  }
}


void PhyLogLogger::Run() {
  
  // Get the file name
  // for now, let's assume that each logger uses a single file to record logs. --YINGJUN
  // SILO uses multiple files only to simplify the process of log truncation.
  std::string filename = GetLogFileFullPath(0);

  // Create a new file
  if (LoggingUtil::OpenFile(filename.c_str(), "wb", file_handle_) == false) {
    LOG_ERROR("Unable to create log file %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }

  /**
   *  Main loop
   */
  while (true) {
    if (is_running_ == false) { break; }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    // Pull log records from workers per epoch buffer
    {
      worker_map_lock_.Lock();

      size_t max_workers_persist_eid = INVALID_EPOCH_ID;
      for (auto &worker_entry : worker_map_) {
        // For every alive worker, move its buffer to the logger's local buffer
        auto worker_ctx_ptr = worker_entry.second.get();

        size_t max_persist_eid = worker_ctx_ptr->current_eid - 1;
        size_t current_persist_eid = worker_ctx_ptr->persist_eid;

        PL_ASSERT(current_persist_eid <= max_persist_eid);

        if (current_persist_eid == max_persist_eid) {
          continue;
        }
        for (size_t epoch_id = current_persist_eid + 1; epoch_id <= max_persist_eid; ++epoch_id) {
          size_t epoch_idx = epoch_id % concurrency::EpochManager::GetEpochQueueCapacity();
          // get all the buffers that are associated with the epoch.
          auto &buffers = worker_ctx_ptr->per_epoch_buffer_ptrs[epoch_idx];

          if (buffers.empty() == true) {
            // no transaction log is generated within this epoch.
            // it's fine. simply ignore it.
            continue;
          }
          PersistEpochBegin(epoch_id);
          // persist all the buffers.
          while (buffers.empty() == false) {
            // Check if the buffer is empty
            // TODO: is it possible to have an empty buffer??? --YINGJUN
            if (buffers.top()->Empty()) {
              worker_ctx_ptr->buffer_pool.PutBuffer(std::move(buffers.top()));
            } else {
              PersistLogBuffer(std::move(buffers.top()));
            }
            PL_ASSERT(buffers.top() == nullptr);
            buffers.pop();
          }
          PersistEpochEnd(epoch_id);
          // Call fsync
          LoggingUtil::FFlushFsync(file_handle_);
        } // end for

        worker_ctx_ptr->persist_eid = max_persist_eid;

        if (max_workers_persist_eid == INVALID_EPOCH_ID || max_workers_persist_eid > max_persist_eid) {
          max_workers_persist_eid = max_persist_eid;
        }

      } // end for

      if (max_workers_persist_eid == INVALID_EPOCH_ID) {
        // in this case, it is likely that there's no registered worker or there's nothing to persist.
        worker_map_lock_.Unlock();
        continue;
      }

      PL_ASSERT(max_workers_persist_eid >= persist_epoch_id_);

      persist_epoch_id_ = max_workers_persist_eid;

      worker_map_lock_.Unlock();
    }
  }

  // Close the log file
  // TODO: Seek and write the integrity information in the header

  // Safely close the file
  bool res = LoggingUtil::CloseFile(file_handle_);
  if (res == false) {
    LOG_ERROR("Can not close log file under directory %s", log_dir_.c_str());
    exit(EXIT_FAILURE);
  }
}

void PhyLogLogger::PersistEpochBegin(const size_t epoch_id) {
  // Write down the epoch begin record  
  LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_BEGIN, epoch_id);

  logger_output_buffer_.Reset();
  record.Serialize(logger_output_buffer_);
  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle_.file);
}

void PhyLogLogger::PersistEpochEnd(const size_t epoch_id) {
  // Write down the epoch end record
  LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_END, epoch_id);

  logger_output_buffer_.Reset();
  record.Serialize(logger_output_buffer_);
  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle_.file);

}

void PhyLogLogger::PersistLogBuffer(std::unique_ptr<LogBuffer> log_buffer) {

  fwrite((const void *) (log_buffer->GetData()), log_buffer->GetSize(), 1, file_handle_.file);

  // Return the buffer to the worker
  log_buffer->Reset();
  auto itr = worker_map_.find(log_buffer->GetWorkerId());
  if (itr != worker_map_.end()) {
    // In this case, the worker is already terminated and removed
    itr->second->buffer_pool.PutBuffer(std::move(log_buffer));
  } else {
    // Release the buffer
    log_buffer.reset(nullptr);
  }
}


}
}