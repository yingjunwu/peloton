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
#include "backend/logging/worker_context.h"
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
    public:
      DepLogger(const size_t &logger_id, const std::string &log_dir) :
        logger_id_(logger_id),
        log_dir_(log_dir),
        logger_thread_(nullptr),
        is_running_(false),
        logger_output_buffer_(),
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

      void RegisterWorker(std::shared_ptr<WorkerContext> worker_ctx);
      void DeregisterWorker(WorkerContext *worker_ctx);

      // Called by the pepoch thread
      void GetLoggerStatus(size_t &current_eid, std::vector<size_t> &worker_eids) {
        status_lock_.Lock();
        current_eid = logger_cur_eid_;
        worker_eids = workers_cur_eids_;
        status_lock_.Unlock();
      }

    private:
      void Run();
      void RunRecoveryThread(
          const size_t thread_id UNUSED_ATTRIBUTE,
          const size_t checkpoint_eid UNUSED_ATTRIBUTE,
          const size_t persist_eid UNUSED_ATTRIBUTE)
      {
        // TODO: Implement it
        return;
      }

      void PersistEpochBegin(FileHandle &file_handle, const size_t epoch_id);
      void PersistEpochEnd(FileHandle &file_handle, const size_t epoch_id);
      void PersistLogBuffer(FileHandle &file_handle, std::unique_ptr<LogBuffer> log_buffer);

      std::string GetLogFileFullPath(size_t epoch_id) {
        return log_dir_ + "/" + logging_filename_prefix_ + "_" + std::to_string(logger_id_) + "_" + std::to_string(epoch_id);
      }

      bool ReplayLogFile(
        const size_t thread_id UNUSED_ATTRIBUTE,
        FileHandle &file_handle UNUSED_ATTRIBUTE,
        size_t checkpoint_eid UNUSED_ATTRIBUTE,
        size_t pepoch_eid UNUSED_ATTRIBUTE) {
        // TODO: Implement it
        return true;
      }
      bool InstallTupleRecord(
          LogRecordType type UNUSED_ATTRIBUTE,
          storage::Tuple *tuple UNUSED_ATTRIBUTE,
          storage::DataTable *table UNUSED_ATTRIBUTE,
          cid_t cur_cid UNUSED_ATTRIBUTE) {
        // TODO: Implement it
        return true;
      }

      // Return value is the swapped txn id, either INVALID_TXNID or INITIAL_TXNID
      txn_id_t LockTuple(
        storage::TileGroupHeader *tg_header UNUSED_ATTRIBUTE,
        oid_t tuple_offset UNUSED_ATTRIBUTE) {
        // TODO: Implement it
        return INVALID_TXN_ID;
      }

      void UnlockTuple(
        storage::TileGroupHeader *tg_header UNUSED_ATTRIBUTE,
        oid_t tuple_offset UNUSED_ATTRIBUTE,
        txn_id_t new_txn_id UNUSED_ATTRIBUTE) {
        // TODO: Implement it
        return;
      }

    private:
      size_t logger_id_;
      std::string log_dir_;

      // logger thread
      std::unique_ptr<std::thread> logger_thread_;
      volatile bool is_running_;

      /* File system related */
      CopySerializeOutput logger_output_buffer_;
      size_t next_file_id_;

      /* Recovery */
      // TODO: Check if we can discard the recovery pool after the recovery is done. Since every thing is copied to the
      // tile gorup and tile group related pool
      std::unique_ptr<VarlenPool> recovery_pool_;
      bool recovery_done_ = false;

      // The spin lock to protect the worker map. We only update this map when creating/terminating a new worker
      Spinlock worker_map_lock_;
      // map from worker id to the worker's context.
      std::unordered_map<oid_t, std::shared_ptr<WorkerContext>> worker_map_;

      // logger status, shared with pepoch thread
      Spinlock status_lock_;
      std::vector<size_t> workers_cur_eids_;
      size_t logger_cur_eid_;

      const std::string logging_filename_prefix_ = "log";

      const int new_file_interval_ = 500; // 500 milliseconds.

    };


  }
}