//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dep_log_manager.h
//
// Identification: src/backend/logging/dep_log_manager.h
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

#include "libcuckoo/cuckoohash_map.hh"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/epoch_manager_factory.h"
#include "backend/logging/log_buffer.h"
#include "backend/logging/log_record.h"
#include "backend/logging/log_buffer_pool.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/logging_util.h"
#include "backend/logging/dep_worker_context.h"
#include "backend/logging/dep_logger.h"
#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"


namespace peloton {

namespace storage {
  class TileGroupHeader;
}


namespace logging {

/**
 * logging file name layout :
 *
 * dir_name + "/" + prefix + "_" + epoch_id
 *
 *
 * logging file layout :
 *
 *  -----------------------------------------------------------------------------
 *  | txn_cid | database_id | table_id | operation_type | data | ... | txn_end_flag
 *  -----------------------------------------------------------------------------
 *
 * NOTE: this layout is designed for physiological logging.
 *
 * NOTE: tuple length can be obtained from the table schema.
 *
 */

/* Per worker thread local context */
extern thread_local DepWorkerContext* tl_dep_worker_ctx;

class DepLogManager : public LogManager {
  DepLogManager(const DepLogManager &) = delete;
  DepLogManager &operator=(const DepLogManager &) = delete;
  DepLogManager(DepLogManager &&) = delete;
  DepLogManager &operator=(DepLogManager &&) = delete;

protected:
  enum EpochStatus {
    EPOCH_STAT_INVALID = 0,
    EPOCH_STAT_NOT_COMMITABLE = 1,
    EPOCH_STAT_COMMITABLE = 2, // Persisted + all dependent epoch persisted
  };

  DepLogManager()
    : worker_count_(0),
      is_running_(false),
      per_epoch_complete_dependencies_(concurrency::EpochManager::GetEpochQueueCapacity()),
      pepoch_id_(START_EPOCH_ID),
      per_epoch_status_(concurrency::EpochManager::GetEpochQueueCapacity()) {
        for (size_t i = 0; i < per_epoch_status_.size(); ++i) {
          per_epoch_status_[i] = EPOCH_STAT_NOT_COMMITABLE;
        }
        PL_ASSERT(concurrency::EpochManagerFactory::IsLocalizedEpochManager() == true);
      }

public:
  static DepLogManager &GetInstance() {
    // TODO: Enforce that we use localized epoch managers
    static DepLogManager log_manager;
    return log_manager;
  }
  virtual ~DepLogManager() {}

  virtual void SetDirectories(const std::vector<std::string> &logging_dirs) override {
    if (logging_dirs.size() > 0) {
      pepoch_dir_ = logging_dirs.at(0);
    }
    // check the existence of logging directories.
    // if not exists, then create the directory.
    for (auto logging_dir : logging_dirs) {
      if (LoggingUtil::CheckDirectoryExistence(logging_dir.c_str()) == false) {
        LOG_INFO("Logging directory %s is not accessible or does not exist", logging_dir.c_str());
        bool res = LoggingUtil::CreateDirectory(logging_dir.c_str(), 0700);
        if (res == false) {
          LOG_ERROR("Cannot create directory: %s", logging_dir.c_str());
        }
      }
    }

    logger_count_ = logging_dirs.size();
    for (size_t i = 0; i < logger_count_; ++i) {
      loggers_.emplace_back(new DepLogger(i, logging_dirs.at(i)));
    }
  }

  // Worker side logic
  virtual void RegisterWorker() override;
  virtual void DeregisterWorker() override;

  void RecordReadDependency(const storage::TileGroupHeader *tg_header, const oid_t tuple_slot);
  void LogInsert(const ItemPointer &tuple_pos);
  void LogUpdate(const ItemPointer &tuple_pos);
  void LogDelete(const ItemPointer &tuple_pos_deleted);
  void StartTxn(concurrency::Transaction *txn);
  void CommitCurrentTxn();
  void FinishPendingTxn();

  // Logger side logic
  virtual void DoRecovery() override;
  virtual void StartLoggers() override;
  virtual void StopLoggers() override;


  void RunPepochLogger();

private:

  // Don't delete the returned pointer
  inline LogBuffer * RegisterNewBufferToEpoch(std::unique_ptr<LogBuffer> log_buffer_ptr) {
    LOG_TRACE("Worker %d Register buffer to epoch %d", (int) tl_dep_worker_ctx->worker_id, (int) tl_dep_worker_ctx->current_eid);
    PL_ASSERT(log_buffer_ptr && log_buffer_ptr->Empty());
    PL_ASSERT(tl_dep_worker_ctx);
    size_t eid_idx = tl_dep_worker_ctx->current_eid % concurrency::EpochManager::GetEpochQueueCapacity();
    tl_dep_worker_ctx->per_epoch_buffer_ptrs[eid_idx].push(std::move(log_buffer_ptr));
    return tl_dep_worker_ctx->per_epoch_buffer_ptrs[eid_idx].top().get();
  }


  inline size_t HashToLogger(oid_t worker_id) {
    return ((size_t) worker_id) % logger_count_;
  }

  void WriteRecordToBuffer(LogRecord &record);

private:
  std::atomic<oid_t> worker_count_;

  std::vector<std::shared_ptr<DepLogger>> loggers_;

  std::unique_ptr<std::thread> pepoch_thread_;
  volatile bool is_running_;

  std::string pepoch_dir_;

  const std::string pepoch_filename_ = "pepoch";

  // Pepoch thread should also know about all workers inorder to collect the dependency infomation
  Spinlock log_workers_lock_;
  std::unordered_map<size_t, std::shared_ptr<DepWorkerContext>> log_workers_;

  // Epoch status map
  // Epochs that still have running txns and are younger than the max dead epoch.
  std::vector<std::unordered_set<size_t>> per_epoch_complete_dependencies_;

  // Concurrent accessible members
  std::atomic<size_t> pepoch_id_;
  std::vector<std::atomic<int>> per_epoch_status_;
};

}
}