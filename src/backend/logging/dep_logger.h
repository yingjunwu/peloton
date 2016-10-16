//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dep_logger.h
//
// Identification: src/backend/logging/dep_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <thread>
#include <list>
#include <stack>
#include <unordered_map>

#include "backend/concurrency/transaction.h"
#include "backend/concurrency/epoch_manager.h"
#include "backend/logging/log_buffer.h"
#include "backend/logging/log_record.h"
#include "backend/logging/log_buffer_pool.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/dep_worker_context.h"
#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "backend/common/pool.h"


namespace peloton {

  namespace storage {
    class TileGroupHeader;
  }

  namespace logging {

    class DepLogger {
      struct EpochStatus {
        std::unordered_set<size_t> depend_epochs_;
        bool persisted;
      };

    public:
      DepLogger(const size_t &logger_id, const std::string &log_dir) :
        logger_id_(logger_id),
        log_dir_(log_dir),
        logger_thread_(nullptr),
        is_running_(false),
        logger_output_buffer_(),
        file_handle_(),
        next_file_id_(0),
        recovery_pool_(new VarlenPool(BACKEND_TYPE_MM)),
        worker_map_lock_(),
        worker_map_(),
        workers_cur_eids_(),
        logger_cur_eid_(START_EPOCH_ID)
      {}

      ~DepLogger() {}

      void StartRecover(UNUSED_ATTRIBUTE size_t checkpoint_eid, UNUSED_ATTRIBUTE  size_t persist_eid) {
        // TODO: implement it
      }

      void WaitForRecovery() {
        // TODO: implement it
      }

      void StartLogging() {
        is_running_ = true;
        logger_thread_.reset(new std::thread(&DepLogger::Run, this));
      }

      void StopLogging() {
        is_running_ = false;
        logger_thread_->join();
      }

      void RegisterWorker(std::shared_ptr<DepWorkerContext> worker_ctx);
      void DeregisterWorker(DepWorkerContext *worker_ctx);

      // Called by the pepoch thread
      void GetLoggerStatus(size_t &current_eid, std::vector<size_t> &worker_eids) {
        status_lock_.Lock();
        current_eid = logger_cur_eid_;
        worker_eids = workers_cur_eids_;
        status_lock_.Unlock();
      }

    private:
      void RunRecovery(size_t checkpoint_eid, size_t persist_eid);
      void Run();

      void PersistEpochBegin(const size_t epoch_id);
      void PersistEpochEnd(const size_t epoch_id);
      void PersistLogBuffer(std::unique_ptr<LogBuffer> log_buffer);

      std::string GetLogFileFullPath(size_t epoch_id) {
        return log_dir_ + "/" + logging_filename_prefix_ + "_" + std::to_string(logger_id_) + "_" + std::to_string(epoch_id);
      }

      std::vector<int> GetSortedLogFileIdList();
      bool ReplayLogFile(FileHandle &file_handle, size_t checkpoint_eid, size_t pepoch_eid);
      bool InstallTupleRecord(LogRecordType type, storage::Tuple *tuple, storage::DataTable *table, cid_t cur_cid);

      // Return value is the swapped txn id, either INVALID_TXNID or INITIAL_TXNID
      txn_id_t LockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset);
      void UnlockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset, txn_id_t new_txn_id);

    private:
      size_t logger_id_;
      std::string log_dir_;

      // logger thread
      std::unique_ptr<std::thread> logger_thread_;
      volatile bool is_running_;

      /* File system related */
      CopySerializeOutput logger_output_buffer_;
      FileHandle file_handle_;
      size_t next_file_id_;

      /* Recovery */
      // TODO: Check if we can discard the recovery pool after the recovery is done. Since every thing is copied to the
      // tile gorup and tile group related pool
      std::unique_ptr<VarlenPool> recovery_pool_;
      bool recovery_done_ = false;

      // The spin lock to protect the worker map. We only update this map when creating/terminating a new worker
      Spinlock worker_map_lock_;
      // map from worker id to the worker's context.
      std::unordered_map<oid_t, std::shared_ptr<DepWorkerContext>> worker_map_;

      // logger status, shared with pepoch thread
      Spinlock status_lock_;
      std::vector<size_t> workers_cur_eids_;
      size_t logger_cur_eid_;

      const std::string logging_filename_prefix_ = "log";
    };


  }
}