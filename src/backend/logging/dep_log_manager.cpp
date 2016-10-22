//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dep_log_manager.cpp
//
// Identification: src/backend/logging/dep_log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "backend/concurrency/epoch_manager_factory.h"
#include "backend/logging/dep_log_manager.h"
#include "backend/logging/durability_factory.h"
#include "backend/catalog/manager.h"
#include "backend/expression/container_tuple.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/concurrency/epoch_manager.h"

namespace peloton {
  namespace logging {

// register worker threads to the log manager before execution.
// note that we always construct logger prior to worker.
// this function is called by each worker thread.
    void DepLogManager::RegisterWorker() {
      PL_ASSERT(tl_worker_ctx == nullptr);
      // shuffle worker to logger
      std::shared_ptr<WorkerContext> shared_ctx_ptr(new WorkerContext(worker_count_++));
      size_t logger_id = HashToLogger(shared_ctx_ptr->worker_id);

      log_workers_lock_.Lock();
      log_workers_.emplace(shared_ctx_ptr->worker_id, shared_ctx_ptr);
      log_workers_lock_.Unlock();

      // Assign to the thread local reference
      tl_worker_ctx = shared_ctx_ptr.get();

      // Share the ownership with the logger
      loggers_[logger_id]->RegisterWorker(shared_ctx_ptr);
    }

// deregister worker threads.
    void DepLogManager::DeregisterWorker() {
      PL_ASSERT(tl_worker_ctx != nullptr);

      size_t logger_id = HashToLogger(tl_worker_ctx->worker_id);

      loggers_[logger_id]->DeregisterWorker(tl_worker_ctx);

      log_workers_lock_.Lock();
      log_workers_.erase(tl_worker_ctx->worker_id);
      log_workers_lock_.Unlock();
    }

    void DepLogManager::WriteRecordToBuffer(LogRecord &record) {
      WorkerContext *ctx = tl_worker_ctx;
      LOG_TRACE("Worker %d write a record", ctx->worker_id);

      PL_ASSERT(ctx);

      // First serialize the epoch to current output buffer
      // TODO: Eliminate this extra copy
      auto &output = ctx->output_buffer;

      // Reset the output buffer
      output.Reset();

      // Reserve for the frame length
      size_t start = output.Position();
      output.WriteInt(0);

      LogRecordType type = record.GetType();
      output.WriteEnumInSingleByte(type);

      switch (type) {
        case LOGRECORD_TYPE_TUPLE_INSERT:
        case LOGRECORD_TYPE_TUPLE_DELETE:
        case LOGRECORD_TYPE_TUPLE_UPDATE: {
          auto &manager = catalog::Manager::GetInstance();
          auto tuple_pos = record.GetItemPointer();
          auto tg = manager.GetTileGroup(tuple_pos.block).get();

          // Write down the database id and the table id
          output.WriteLong(tg->GetDatabaseId());
          output.WriteLong(tg->GetTableId());

          // Write the full tuple into the buffer
          expression::ContainerTuple<storage::TileGroup> container_tuple(
            tg, tuple_pos.offset
          );
          container_tuple.SerializeTo(output);
          break;
        }
        case LOGRECORD_TYPE_TRANSACTION_BEGIN:
        case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
          output.WriteLong(ctx->current_cid);
          break;
        }
        case LOGRECORD_TYPE_EPOCH_BEGIN:
        case LOGRECORD_TYPE_EPOCH_END: {
          output.WriteLong((uint64_t) ctx->current_commit_eid);
          break;
        }
        default: {
          LOG_ERROR("Unsupported log record type");
          PL_ASSERT(false);
        }
      }

      size_t epoch_idx = ctx->current_commit_eid % concurrency::EpochManager::GetEpochQueueCapacity();

      PL_ASSERT(ctx->per_epoch_buffer_ptrs[epoch_idx].empty() == false);
      LogBuffer* buffer_ptr = ctx->per_epoch_buffer_ptrs[epoch_idx].top().get();
      PL_ASSERT(buffer_ptr);

      // Add the frame length
      // XXX: We rely on the fact that the serializer treat a int32_t as 4 bytes
      output.WriteIntAt(start, (int32_t) (output.Position() - start - sizeof(int32_t)));

      // Copy the output buffer into current buffer
      bool is_success = buffer_ptr->WriteData(output.Data(), output.Size());
      if (is_success == false) {
        // A buffer is full, pass it to the front end logger
        // Get a new buffer and register it to current epoch
        buffer_ptr = RegisterNewBufferToEpoch(std::move((ctx->buffer_pool.GetBuffer(ctx->current_commit_eid))));
        // Write it again
        is_success = buffer_ptr->WriteData(output.Data(), output.Size());
        PL_ASSERT(is_success);
      }
    }

    void DepLogManager::StartTxn(concurrency::Transaction *txn) {
      PL_ASSERT(tl_worker_ctx);
      size_t txn_eid = txn->GetEpochId();

      // Record the txn timer
      DurabilityFactory::StartTxnTimer(txn_eid, tl_worker_ctx);

      PL_ASSERT(tl_worker_ctx->current_commit_eid == INVALID_EPOCH_ID || tl_worker_ctx->current_commit_eid <= txn_eid);

      // Handle the epoch id
      if (tl_worker_ctx->current_commit_eid == INVALID_EPOCH_ID
          || tl_worker_ctx->current_commit_eid != txn_eid) {
        // if this is a new epoch, then write to a new buffer
        tl_worker_ctx->current_commit_eid = txn_eid;

        // Reset the dependency
        // TODO: Pay attention to epoch overflow...
        size_t epoch_idx = txn_eid % concurrency::EpochManager::GetEpochQueueCapacity();
        tl_worker_ctx->per_epoch_dependencies[epoch_idx].clear();

        RegisterNewBufferToEpoch(std::move(tl_worker_ctx->buffer_pool.GetBuffer(txn_eid)));
      }

      // Handle the commit id
      cid_t txn_cid = txn->GetEndCommitId();
      tl_worker_ctx->current_cid = txn_cid;
    }

    void DepLogManager::StartPersistTxn() {
      // Log down the begin of a transaction
      LogRecord record = LogRecordFactory::CreateTxnRecord(LOGRECORD_TYPE_TRANSACTION_BEGIN, tl_worker_ctx->current_commit_eid);
      WriteRecordToBuffer(record);
    }

    void DepLogManager::EndPersistTxn() {
      PL_ASSERT(tl_worker_ctx);
      LogRecord record = LogRecordFactory::CreateTxnRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT, tl_worker_ctx->current_cid);
      WriteRecordToBuffer(record);
    }

    void DepLogManager::FinishPendingTxn() {
      PL_ASSERT(tl_worker_ctx);
      // Regular commit
      size_t glob_peid = pepoch_id_.load();
      DurabilityFactory::StopTimersByPepoch(glob_peid, tl_worker_ctx);

      // Dependent commit
      uint64_t current_time_in_usec = 0;
      TimerType timer_type = DurabilityFactory::GetTimerType();

      auto itr = tl_worker_ctx->pending_txn_timers.upper_bound(glob_peid);

      while (itr != tl_worker_ctx->pending_txn_timers.end()) {
        size_t eid = itr->first;
        size_t idx = eid % concurrency::EpochManager::GetEpochQueueCapacity();
        if (per_epoch_status_[idx] == EPOCH_STAT_COMMITABLE) {
          if (current_time_in_usec == 0) {
            current_time_in_usec = DurabilityFactory::GetCurrentTimeInUsec();
          }

          for (uint64_t txn_start_us : itr->second) {
            PL_ASSERT(current_time_in_usec > txn_start_us);
            tl_worker_ctx->txn_summary.AddTxnLatReport(current_time_in_usec - txn_start_us, (timer_type == TIMER_DISTRIBUTION));
          }

          itr = tl_worker_ctx->pending_txn_timers.erase(itr);
        } else {
          itr++;
        }
      }
    }

    void DepLogManager::RecordReadDependency(const storage::TileGroupHeader *tg_header, const oid_t tuple_slot) {
      auto ctx = tl_worker_ctx;
      size_t dep_eid = concurrency::EpochManager::GetEidFromCid(tg_header->GetBeginCommitId(tuple_slot));

      // Filter out self dependency
      if (dep_eid == ctx->current_commit_eid) {
        return;
      }

      size_t epoch_idx = ctx->current_commit_eid % concurrency::EpochManager::GetEpochQueueCapacity();
      ctx->per_epoch_dependencies[epoch_idx].insert(dep_eid);
    }

    void DepLogManager::LogInsert(const ItemPointer &tuple_pos) {
      LogRecord record = LogRecordFactory::CreateTupleRecord(LOGRECORD_TYPE_TUPLE_INSERT, tuple_pos);
      WriteRecordToBuffer(record);
    }

    void DepLogManager::LogUpdate(const ItemPointer &tuple_pos) {
      LogRecord record = LogRecordFactory::CreateTupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE, tuple_pos);
      WriteRecordToBuffer(record);
    }

    void DepLogManager::LogDelete(const ItemPointer &tuple_pos_deleted) {
      // Need the tuple value for the deleted tuple
      LogRecord record = LogRecordFactory::CreateTupleRecord(LOGRECORD_TYPE_TUPLE_DELETE, tuple_pos_deleted);
      WriteRecordToBuffer(record);
    }


    void DepLogManager::StartLoggers() {
      for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
        LOG_TRACE("Start logger %d", (int) logger_id);
        loggers_[logger_id]->StartLogging();
      }
      is_running_ = true;
      pepoch_thread_.reset(new std::thread(&DepLogManager::RunPepochLogger, this));
    }

    void DepLogManager::StopLoggers() {
      for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
        loggers_[logger_id]->StopLogging();
      }
      is_running_ = false;
      pepoch_thread_->join();
    }

    void DepLogManager::RunPepochLogger() {

      FileHandle file_handle;
      std::string filename = pepoch_dir_ + "/" + pepoch_filename_;
      // Create a new file
      if (LoggingUtil::OpenFile(filename.c_str(), "wb", file_handle) == false) {
        LOG_ERROR("Unable to create pepoch file %s\n", filename.c_str());
        exit(EXIT_FAILURE);
      }

      auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
      size_t logger_count = loggers_.size();
      size_t epoch_queue_capacity = epoch_manager.GetEpochQueueCapacity();

      // Local states
      std::vector<size_t> loggers_last_eid(loggers_.size(), 0);

      // per run tmp var
      std::set<size_t> alive_worker_epochs;
      std::unordered_set<size_t> new_commitable_epochs;
      size_t logger_eid;
      std::vector<size_t> logger_cur_worker_eids;

      while (true) {
        if (is_running_ == false) {
          break;
        }

        std::this_thread::sleep_for(std::chrono::microseconds
                                      (concurrency::EpochManagerFactory::GetInstance().GetEpochLengthInMicroSecQuarter())
        );

        // Init local state
        size_t pepoch_thread_cur_eid = epoch_manager.GetCurrentEpochId(); // it's value will come from an std::min
        new_commitable_epochs.clear();
        alive_worker_epochs.clear();
        alive_worker_epochs.insert(MAX_EPOCH_ID); // Ensure there is something in the set

        for (size_t i = 0; i < logger_count; ++i) {
          auto logger_ptr = loggers_[i].get();
          // Init tmp vars
          logger_eid = INVALID_EPOCH_ID;
          logger_cur_worker_eids.clear();

          // Get logger status
          logger_ptr->GetLoggerStatus(logger_eid, logger_cur_worker_eids); // Acquire lock inside the function call
          PL_ASSERT(loggers_last_eid[i] <= logger_eid);
          loggers_last_eid[i] = logger_eid;
          for (size_t weid : logger_cur_worker_eids) {
            // Collect live worker eids
            alive_worker_epochs.insert(weid);
          }
        }
        // Get new current eid
        for (size_t eid : loggers_last_eid) {
          pepoch_thread_cur_eid = std::min(pepoch_thread_cur_eid, eid);
        }

        // Get new pepoch id
        size_t last_pepoch_id = pepoch_id_.load();
        size_t new_pepoch_id = std::min(pepoch_thread_cur_eid, *alive_worker_epochs.begin()) - 1;

        // Reset dependencies that are not needed
        for (size_t eid = last_pepoch_id + 1; eid <= new_pepoch_id; ++eid) {
          // TODO: How to detect and handle epoch capacity overflow???
          size_t idx = eid % epoch_queue_capacity;
          per_epoch_complete_dependencies_[idx].clear();
        }

        // Collect dependency for epochs that are:
        //      1. Larger than the new_pepoch_id
        //      2. Not in the alive epoch set
        //      3. Smaller than the pepoch_thread_cur_eid
        // Epoch boundary guarantee that when pepoch thread is reading those per epoch structure, no worker is updating
        // them concurrently. However, when we exceed the epoch queue capacity... there may be something really bad..
        log_workers_lock_.Lock();
        {
          for (size_t ck_eid = new_pepoch_id + 1; ck_eid < pepoch_thread_cur_eid; ++ck_eid) {

            if (alive_worker_epochs.count(ck_eid) != 0) {
              // Alive epoch
              continue;
            }
            size_t idx = ck_eid % epoch_queue_capacity;
            // Every epoch's dependency should only be updated once
            // Collect dependency
            for (auto itr : log_workers_) {
              auto &dep_epochs = itr.second->per_epoch_dependencies[idx];
              for (size_t dep_eid : dep_epochs) {
                if (dep_eid > new_pepoch_id) {
                  // Only add valid dependency
                  per_epoch_complete_dependencies_[idx].insert(dep_eid);
                }
              }
            }


            // TODO: Could have shrunk the critical region... but it's not often executed concurrently...
            // Check dependency
            bool commitable = true;
            auto dep_itr = per_epoch_complete_dependencies_[idx].begin();
            while (dep_itr != per_epoch_complete_dependencies_[idx].end()) {
              size_t dep_eid = *dep_itr;
              size_t dep_epoch_idx = dep_eid % epoch_queue_capacity;
              if (dep_eid <= new_pepoch_id
                  || per_epoch_status_[dep_epoch_idx] == EPOCH_STAT_COMMITABLE
                  || new_commitable_epochs.count(dep_eid) != 0) {
                // 1. Dependent epoch is dead
                // 2. Dependent epoch is commitablep
                dep_itr = per_epoch_complete_dependencies_[idx].erase(dep_itr);
              } else {
                commitable = false;
                break;
              }
            }

            // Mark in new commitable epochs
            if (commitable == true) {
              new_commitable_epochs.insert(ck_eid);
            }
          }
        }
        log_workers_lock_.Unlock();

        // Persist the epochs
        // TODO: Do we need to wait until the dependent epoch id is flushed out to the pepoch file?

        // Flush the new pepoch id
        bool written = false;
        if (new_pepoch_id > pepoch_id_) {
          written = true;
          fwrite((const void *)(&new_pepoch_id), sizeof(new_pepoch_id), 1, file_handle.file);
        }

        // Flush other epochs
        for (size_t new_commitable_eid : new_commitable_epochs) {
          written = true;
          // XXX: MSB is 1 indicate it's a stand alone commitable epoch
          size_t altered_eid = new_commitable_eid | (1ul << 63);
          fwrite((const void *)(&altered_eid), sizeof(altered_eid), 1, file_handle.file);
        }

        // Fsync
        if (written == true) {
          LoggingUtil::FFlushFsync(file_handle);
        }

        // Publish all changes
        pepoch_id_ = new_pepoch_id;
        for (size_t new_commitable_eid : new_commitable_epochs) {
          size_t idx = new_commitable_eid % epoch_queue_capacity;
          per_epoch_status_[idx] = EPOCH_STAT_COMMITABLE;
        }
        COMPILER_MEMORY_FENCE;
        for (size_t dead_eid = last_pepoch_id + 1; dead_eid < new_pepoch_id; ++dead_eid) {
          size_t idx = dead_eid % epoch_queue_capacity;
          // Reset for future usage
          per_epoch_status_[idx] = EPOCH_STAT_NOT_COMMITABLE;
        }
      }

      // Safely close the file
      bool res = LoggingUtil::CloseFile(file_handle);
      if (res == false) {
        LOG_ERROR("Cannot close pepoch file");
        exit(EXIT_FAILURE);
      }

    }

  }
}