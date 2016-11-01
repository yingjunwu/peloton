//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// physical_logger.cpp
//
// Identification: src/backend/logging/physical_logger.cpp
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
#include "backend/gc/gc_manager_factory.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "backend/catalog/manager.h"
#include "backend/concurrency/epoch_manager_factory.h"
#include "backend/expression/container_tuple.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/database.h"

#include "backend/logging/physical_logger.h"

namespace peloton {
namespace logging {

void PhysicalLogger::RegisterWorker(WorkerContext *physical_worker_ctx) {
  worker_map_lock_.Lock();
  worker_map_[physical_worker_ctx->worker_id].reset(physical_worker_ctx);
  worker_map_lock_.Unlock();
}

void PhysicalLogger::DeregisterWorker(WorkerContext *physical_worker_ctx) {
  worker_map_lock_.Lock();
  worker_map_.erase(physical_worker_ctx->worker_id);
  worker_map_lock_.Unlock();
}

void PhysicalLogger::StartIndexRebulding(const size_t logger_count) {
  PL_ASSERT(recovery_threads_.size() != 0);
  recovery_threads_[0].reset(new std::thread(&PhysicalLogger::RunIndexRebuildThread, this, logger_count));
}

void PhysicalLogger::WaitForIndexRebuilding() {
  recovery_threads_[0]->join();
}

void PhysicalLogger::StartRecoverDataTables(const size_t checkpoint_eid, const size_t persist_eid,
                                            const size_t recovery_thread_count) {

  GetSortedLogFileIdList(checkpoint_eid, persist_eid);
  printf("recovery_thread_count = %lu\n", recovery_thread_count);
  recovery_pools_.resize(recovery_thread_count);
  recovery_threads_.resize(recovery_thread_count);

  for (size_t i = 0; i < recovery_thread_count; ++i) {

    recovery_pools_[i].reset(new VarlenPool(BACKEND_TYPE_MM));

    recovery_threads_[i].reset(new std::thread(&PhysicalLogger::RunRecoveryThread, this, i, checkpoint_eid, persist_eid));
  }
}

void PhysicalLogger::WaitForRecovery() {
  for (auto &recovery_thread : recovery_threads_) {
    recovery_thread->join();
  }
}


void PhysicalLogger::GetSortedLogFileIdList(const size_t checkpoint_eid, const size_t persist_eid) {
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
  std::sort(file_eids_.begin(), file_eids_.end(), std::less<size_t>());
  max_replay_file_id_ = file_eids_.size() - 1;

  // for (auto &entry : file_eids_) {
  //   printf("file id = %lu\n", entry);
  // }

}

txn_id_t PhysicalLogger::LockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset) {
  txn_id_t txnid_logger = (START_TXN_ID + this->logger_id_);
  while (true) {
    // We use the txn_id field as a lock. However this field also stored information about whether a tuple is deleted or not.
    // To restore that information, we need to return the old one before overwriting it.
    if (tg_header->SetAtomicTransactionId(tuple_offset, INITIAL_TXN_ID, txnid_logger) == INITIAL_TXN_ID) return INITIAL_TXN_ID;
    if (tg_header->SetAtomicTransactionId(tuple_offset, INVALID_TXN_ID, txnid_logger) == INVALID_TXN_ID) return INVALID_TXN_ID;
    _mm_pause();
  }
}

void PhysicalLogger::UnlockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset, txn_id_t new_txn_id) {
  PL_ASSERT(new_txn_id == INVALID_TXN_ID || new_txn_id == INITIAL_TXN_ID);
  tg_header->SetAtomicTransactionId(tuple_offset, (txn_id_t) (START_TXN_ID + this->logger_id_), new_txn_id);
}

void PhysicalLogger::SetTupleDeletedFlag(storage::TileGroupHeader *tg_header, oid_t tuple_offset, bool deleted) {
  auto reserved_field = tg_header->GetReservedFieldRef(tuple_offset);
  (*reinterpret_cast<bool *>(reserved_field)) = deleted;
}

bool PhysicalLogger::GetTupleDeletedFlag(storage::TileGroupHeader *tg_header, oid_t tuple_offset) {
  auto reserved_field = tg_header->GetReservedFieldRef(tuple_offset);
  return *reinterpret_cast<bool *>(reserved_field);
}

bool PhysicalLogger::InstallTupleRecord(LogRecordType type, ItemPointer new_tuple_pos, ItemPointer old_tuple_pos,
                                        storage::Tuple *tuple, storage::DataTable *table, cid_t cur_cid) {
  auto &manager = catalog::Manager::GetInstance();
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  oid_t old_tg_id = old_tuple_pos.block;
  oid_t old_tuple_offset = old_tuple_pos.offset;
  oid_t new_tg_id = new_tuple_pos.block;
  oid_t new_tuple_offset = new_tuple_pos.offset;

  switch (type) {
    case LOGRECORD_TYPE_TUPLE_DELETE:
    case LOGRECORD_TYPE_TUPLE_UPDATE: {
      table->PrepareTupleSlotForPhysicalRecovery(new_tuple_pos);
      table->PrepareTupleSlotForPhysicalRecovery(old_tuple_pos);

      auto old_tg = manager.GetTileGroup(old_tg_id);
      auto new_tg = manager.GetTileGroup(new_tg_id);
      auto old_tg_header = old_tg->GetHeader();
      auto new_tg_header = new_tg->GetHeader();

      // Delete old version
      auto tid = LockTuple(old_tg_header, old_tuple_offset);
      cid_t old_bcid = old_tg_header->GetBeginCommitId(old_tuple_offset);
      cid_t old_ecid = old_tg_header->GetEndCommitId(old_tuple_offset);

      if ((old_bcid == MAX_CID && old_ecid == MAX_CID) || cur_cid > old_bcid) {
        // Delete the tuple
        if (old_bcid == MAX_CID) {
          old_tg_header->SetBeginCommitId(old_tuple_offset, START_CID);
        }
        old_tg_header->SetEndCommitId(old_tuple_offset, cur_cid);
        UnlockTuple(old_tg_header, old_tuple_offset, INVALID_TXN_ID);
      } else {
        PL_ASSERT(cur_cid <= old_bcid);
        // Another newer update is replayed on this tuple slot
        // Unlock with the original tid
        UnlockTuple(old_tg_header, old_tuple_offset, tid);
      }

      // Update new version
      tid = LockTuple(new_tg_header, new_tuple_offset);
      cid_t new_bcid = new_tg_header->GetBeginCommitId(new_tuple_offset);
      cid_t new_ecid = new_tg_header->GetEndCommitId(new_tuple_offset);

      if ((new_bcid == MAX_CID && new_ecid == MAX_CID) || cur_cid > new_bcid) {
        // Update the tuple
        // Set header
        new_tg_header->SetBeginCommitId(new_tuple_offset, cur_cid);
        new_tg_header->SetEndCommitId(new_tuple_offset, MAX_CID);

        // Copy the tuple content
        new_tg->CopyTuple(tuple, new_tuple_offset);

        // Unlock with tid as initial id on update/ invalid tid on delete
        UnlockTuple(new_tg_header, new_tuple_offset,
                    (type == LOGRECORD_TYPE_TUPLE_UPDATE) ? INITIAL_TXN_ID : INVALID_TXN_ID);
      } else {
        // Another newer update is replayed on this tuple slot
        // Unlock with the original tid
        UnlockTuple(new_tg_header, new_tuple_offset, tid);
      }
      break;
    }
    case LOGRECORD_TYPE_TUPLE_INSERT: {
      table->PrepareTupleSlotForPhysicalRecovery(new_tuple_pos);
      PL_ASSERT(old_tuple_pos == INVALID_ITEMPOINTER);

      auto new_tg = manager.GetTileGroup(new_tg_id);
      auto new_tg_header = new_tg->GetHeader();

      // Try to do insert
      auto tid = LockTuple(new_tg_header, new_tuple_offset);
      cid_t bcid = new_tg_header->GetBeginCommitId(new_tuple_offset);
      cid_t ecid = new_tg_header->GetEndCommitId(new_tuple_offset);

      if ((bcid == MAX_CID && ecid == MAX_CID) || cur_cid > bcid) {
        // Insert the tuple
        // Set header
        new_tg_header->SetBeginCommitId(new_tuple_offset, cur_cid);
        new_tg_header->SetEndCommitId(new_tuple_offset, MAX_CID);

        // Copy the tuple content
        new_tg->CopyTuple(tuple, new_tuple_offset);

        // Unlock with initial tid
        UnlockTuple(new_tg_header, new_tuple_offset, INITIAL_TXN_ID);
      } else {
        // The insert is stale
        UnlockTuple(new_tg_header, new_tuple_offset, tid);
      }

      break;
    }
    default:
      LOG_ERROR("INVALID RECORD TYPE");
      return false;
  }
  return true;
}

bool PhysicalLogger::ReplayLogFile(const size_t thread_id, FileHandle &file_handle, size_t checkpoint_eid, size_t persist_eid) {
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
    CopySerializeInputBE length_decode((const void *) &length_buf, 4);
    int length = length_decode.ReadInt();

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
        if (current_eid != INVALID_EPOCH_ID) {
          LOG_ERROR("Incomplete epoch in log record");
          return false;
        }
        current_eid = (size_t) record_decode.ReadLong();
        // printf("begin epoch id = %lu\n", current_eid);
        break;
      } case LOGRECORD_TYPE_EPOCH_END: {
        size_t eid = (size_t) record_decode.ReadLong();
        if (eid != current_eid) {
          LOG_ERROR("Mismatched epoch in log record");
          return false;
        }
        // printf("end epoch id = %lu\n", current_eid);
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

        if (current_eid >= checkpoint_eid && current_eid <= persist_eid) {
          // TODO: Do we really need this -- Jiexi
          concurrency::current_txn = new concurrency::Transaction(thread_id, current_cid);
        }

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
        current_cid = INVALID_CID;

        if (current_eid >= checkpoint_eid && current_eid <= persist_eid) {
          // TODO: Do we really need this -- Jiexi
          delete concurrency::current_txn;
          concurrency::current_txn = nullptr;
        }

        break;
      }
      case LOGRECORD_TYPE_TUPLE_UPDATE:
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
        ItemPointer new_tuple_pos = INVALID_ITEMPOINTER;
        ItemPointer old_tuple_pos = INVALID_ITEMPOINTER;

        if (record_type != LOGRECORD_TYPE_TUPLE_INSERT) {
          old_tuple_pos.block = (oid_t) record_decode.ReadLong();
          old_tuple_pos.offset = (oid_t) record_decode.ReadLong();
        }

        new_tuple_pos.block = (oid_t) record_decode.ReadLong();
        new_tuple_pos.offset = (oid_t) record_decode.ReadLong();

        // XXX: We still rely on an alive catalog manager
        auto table = catalog::Manager::GetInstance().GetTableWithOid(database_id, table_id);
        auto schema = table->GetSchema();

        // Decode the tuple from the record
        std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
        tuple->DeserializeFrom(record_decode, this->recovery_pools_[thread_id].get());

        // Install the record
        InstallTupleRecord(record_type, new_tuple_pos, old_tuple_pos, tuple.get(), table, current_cid);
        break;
      }
      default:
      LOG_ERROR("Unknown log record type");
        return false;
    }

  }

  return true;
}

void PhysicalLogger::RunRecoveryThread(const size_t thread_id, const size_t checkpoint_eid, const size_t persist_eid) {

  while (true) {

    int replay_file_id = max_replay_file_id_.fetch_sub(1, std::memory_order_relaxed);
    if (replay_file_id < 0) {
      break;
    }

    size_t file_eid = file_eids_.at(replay_file_id);
    printf("start replaying file id = %d, file eid = %lu\n", replay_file_id, file_eid);
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

void PhysicalLogger::RunIndexRebuildThread(const size_t logger_count) {
  auto &manager = catalog::Manager::GetInstance();
  auto db_count = manager.GetDatabaseCount();

  // Loop all databases
  for (oid_t db_idx = 0; db_idx < db_count; db_idx ++) {
    auto database = manager.GetDatabase(db_idx);
    auto table_count = database->GetTableCount();

    // Loop all tables
    for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
      // Get the target table
      storage::DataTable *table = database->GetTable(table_idx);
      RebuildIndexForTable(logger_count, table);
    }
  }
}

void PhysicalLogger::RebuildIndexForTable(const size_t logger_count, storage::DataTable *table) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto &gc_manager = gc::GCManagerFactory::GetInstance();

  size_t tg_count = table->GetTileGroupCount();

  std::function<bool(const void *)> fn =
    std::bind(&concurrency::TransactionManager::IsOccupied,
              &txn_manager, std::placeholders::_1);

  // Loop all the tile groups, shared by thread id
  for (size_t tg_idx = logger_id_; tg_idx < tg_count; tg_idx += logger_count) {
    auto tg = table->GetTileGroup(tg_idx).get();
    auto tg_header = tg->GetHeader();
    auto tg_id = tg->GetTileGroupId();
    // Loop all tuples headers in the tile group
    oid_t tg_capacity = tg_header->GetCapacity();
    for (oid_t tuple_offset = 0; tuple_offset < tg_capacity; ++tuple_offset) {
      // Check if the tuple is valid
      if (tg_header->GetTransactionId(tuple_offset) == INITIAL_TXN_ID) {
        // Insert in into the index
        expression::ContainerTuple<storage::TileGroup> container_tuple(tg, tuple_offset);
        ItemPointer *itemptr = nullptr;

        auto res = table->InsertInIndexes(&container_tuple, ItemPointer(tg_id, tuple_offset), &itemptr);
        if (res == false) {
          LOG_ERROR("Index constraint violation");
        } else {
          PL_ASSERT(itemptr != nullptr);
          txn_manager.InitInsertedTupleForRecovery(tg_header, tuple_offset, itemptr);
        }
      } else {
        // Invalid tuple, register it to the garbage collection manager
        gc_manager.DirectRecycleTuple(table->GetOid(), ItemPointer(tg_id, tuple_offset));
      }
    }
  }
}


void PhysicalLogger::Run() {
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

  auto &epoch_mamager = concurrency::EpochManagerFactory::GetInstance();

  /**
   *  Main loop
   */
  while (true) {
    if (is_running_ == false) { break; }

    std::this_thread::sleep_for(
      std::chrono::microseconds(epoch_mamager.GetEpochLengthInMicroSecQuarter()));

    size_t current_global_eid = epoch_mamager.GetCurrentEpochId();

    // Pull log records from workers per epoch buffer
    {
      worker_map_lock_.Lock();

      size_t min_workers_persist_eid = INVALID_EPOCH_ID;
      for (auto &worker_entry : worker_map_) {
        // For every alive worker, move its buffer to the logger's local buffer
        auto worker_ctx_ptr = worker_entry.second.get();

        size_t last_persist_eid = worker_ctx_ptr->persist_eid;

        // Since idle worker has MAX_EPOCH_ID, we need a std::min here
        size_t worker_current_eid = std::min(epoch_mamager.GetRwTxnWorkerCurrentEid(worker_ctx_ptr->transaction_worker_id),
                                             current_global_eid);

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

      // Currently when there is a long running txn, the logger can only know the worker's eid when the worker is committing the txn.
      // We should switch to localized epoch manager to solve this problem.
      // XXX: work around. We should switch to localized epoch manager to solve this problem
      if (min_workers_persist_eid < persist_epoch_id_) {
        min_workers_persist_eid = persist_epoch_id_;
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

void PhysicalLogger::PersistEpochBegin(FileHandle &file_handle, const size_t epoch_id) {
  // Write down the epoch begin record
  // LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_BEGIN, epoch_id);

  logger_output_buffer_.Reset();

  size_t start = logger_output_buffer_.Position();
  logger_output_buffer_.WriteInt(0);

  logger_output_buffer_.WriteEnumInSingleByte(LOGRECORD_TYPE_EPOCH_BEGIN);
  logger_output_buffer_.WriteLong((uint64_t) epoch_id);

  logger_output_buffer_.WriteIntAt(start, (int32_t) (logger_output_buffer_.Position() - start - sizeof(int32_t)));

  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle.file);
}

void PhysicalLogger::PersistEpochEnd(FileHandle &file_handle, const size_t epoch_id) {
  // Write down the epoch end record
  // LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_END, epoch_id);

  logger_output_buffer_.Reset();

  size_t start = logger_output_buffer_.Position();
  logger_output_buffer_.WriteInt(0);

  logger_output_buffer_.WriteEnumInSingleByte(LOGRECORD_TYPE_EPOCH_END);
  logger_output_buffer_.WriteLong((uint64_t) epoch_id);

  logger_output_buffer_.WriteIntAt(start, (int32_t) (logger_output_buffer_.Position() - start - sizeof(int32_t)));

  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle.file);

}

void PhysicalLogger::PersistLogBuffer(FileHandle &file_handle, std::unique_ptr<LogBuffer> log_buffer) {

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