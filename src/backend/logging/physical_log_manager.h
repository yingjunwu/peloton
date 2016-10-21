//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// physical_log_manager.h
//
// Identification: src/backend/logging/physical_log_manager.h
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
#include "backend/concurrency/epoch_manager.h"
#include "backend/logging/log_buffer.h"
#include "backend/logging/log_record.h"
#include "backend/logging/log_buffer_pool.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/logging_util.h"
#include "backend/logging/physical_worker_context.h"
#include "backend/logging/physical_logger.h"
#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"


namespace peloton {
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
 *  | txn_cid | database_id | table_id | operation_type | column_count | columns | data | ... | txn_end_flag
 *  -----------------------------------------------------------------------------
 *
 * NOTE: this layout is designed for physiological delta logging.
 *
 * NOTE: tuple length can be obtained from the table schema.
 *
 */

class PhysicalLogManager : public LogManager {
  PhysicalLogManager(const PhysicalLogManager &) = delete;
  PhysicalLogManager &operator=(const PhysicalLogManager &) = delete;
  PhysicalLogManager(PhysicalLogManager &&) = delete;
  PhysicalLogManager &operator=(PhysicalLogManager &&) = delete;

protected:

  PhysicalLogManager()
    : worker_count_(0),
      is_running_(false) {}

public:
  static PhysicalLogManager &GetInstance() {
    static PhysicalLogManager log_manager;
    return log_manager;
  }
  virtual ~PhysicalLogManager() {}

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
      loggers_.emplace_back(new PhysicalLogger(i, logging_dirs.at(i)));
    }
  }

  // Worker side logic
  virtual void RegisterWorker() override;
  virtual void DeregisterWorker() override;

  void LogInsert(const ItemPointer &tuple_pos);
  void LogUpdate(const ItemPointer &tuple_pos);
  void LogDelete(const ItemPointer &tuple_pos_deleted);
  void StartTxn(concurrency::Transaction *txn);
  void CommitCurrentTxn();
  void FinishPendingTxn();

  // Logger side logic
  virtual void DoRecovery(const size_t &begin_eid) override;
  virtual void StartLoggers() override;
  virtual void StopLoggers() override;


  void RunPepochLogger();

private:

  void WriteRecordToBuffer(LogRecord &record);

private:
  std::atomic<oid_t> worker_count_;

  std::vector<std::shared_ptr<PhysicalLogger>> loggers_;

  std::unique_ptr<std::thread> pepoch_thread_;
  volatile bool is_running_;

  std::string pepoch_dir_;

  const std::string pepoch_filename_ = "pepoch";
};

}
}