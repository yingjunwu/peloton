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
#include <set>

#include "libcuckoo/cuckoohash_map.hh"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/epoch_manager_factory.h"
#include "backend/logging/log_buffer.h"
#include "backend/logging/log_record.h"
#include "backend/logging/log_buffer_pool.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/logging_util.h"
#include "backend/logging/worker_context.h"
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


/**
 * WARNING: Dependency tracking is incorrect when there is DELETE tuple operations -- Jiexi
 *
 * The reason is that we track dependency based on the read set of a transaction. However, the fact that
 * a tranction encounter a deleted tuple does not reflect in its read set. As a result, we lose the RAW dependency
 * from this txn to the txn that deleted the tuple.
 */
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
    logger_dirs_ = logging_dirs;

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

  virtual const std::vector<std::string> &GetDirectories() override {
    return logger_dirs_;
  }

  // Worker side logic
  virtual void RegisterWorker() override;
  virtual void DeregisterWorker() override;

  void RecordReadDependency(const storage::TileGroupHeader *tg_header, const oid_t tuple_slot);
  void LogInsert(const ItemPointer &tuple_pos);
  void LogUpdate(const ItemPointer &tuple_pos);
  void LogDelete(const ItemPointer &tuple_pos_deleted);

  void StartTxn(concurrency::Transaction *txn) override ;
  void StartPersistTxn() ;
  void EndPersistTxn();
  void FinishPendingTxn() override ;

  // Logger side logic
  virtual void DoRecovery(const size_t &begin_eid UNUSED_ATTRIBUTE) {
    // TODO: Implement it
  };
  virtual void StartLoggers() override;
  virtual void StopLoggers() override;


  void RunPepochLogger();

private:
  void WriteRecordToBuffer(LogRecord &record);

private:
  std::atomic<oid_t> worker_count_;

  std::vector<std::shared_ptr<DepLogger>> loggers_;

  std::vector<std::string> logger_dirs_;

  std::unique_ptr<std::thread> pepoch_thread_;
  volatile bool is_running_;

  std::string pepoch_dir_;

  const std::string pepoch_filename_ = "pepoch";

  // Pepoch thread should also know about all workers inorder to collect the dependency infomation
  Spinlock log_workers_lock_;
  std::unordered_map<size_t, std::shared_ptr<WorkerContext>> log_workers_;

  // Epoch status map
  // Epochs that still have running txns and are younger than the max dead epoch.
  std::vector<std::unordered_set<size_t>> per_epoch_complete_dependencies_;

  // Concurrent accessible members
  std::atomic<size_t> pepoch_id_;
  std::vector<std::atomic<int>> per_epoch_status_;
};

}
}