//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// reordered_physical_log_manager.h
//
// Identification: src/backend/logging/reordered_physical_log_manager.h
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
#include "backend/logging/worker_context.h"
#include "backend/logging/reordered_physical_logger.h"
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

class ReorderedPhysicalLogManager : public LogManager {
  ReorderedPhysicalLogManager(const ReorderedPhysicalLogManager &) = delete;
  ReorderedPhysicalLogManager &operator=(const ReorderedPhysicalLogManager &) = delete;
  ReorderedPhysicalLogManager(ReorderedPhysicalLogManager &&) = delete;
  ReorderedPhysicalLogManager &operator=(ReorderedPhysicalLogManager &&) = delete;

protected:

  ReorderedPhysicalLogManager()
    : worker_count_(0),
      is_running_(false) {}

public:
  static ReorderedPhysicalLogManager &GetInstance() {
    static ReorderedPhysicalLogManager log_manager;
    return log_manager;
  }
  virtual ~ReorderedPhysicalLogManager() {}

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
      loggers_.emplace_back(new ReorderedPhysicalLogger(i, logging_dirs.at(i)));
    }
  }

  virtual const std::vector<std::string> &GetDirectories() override {
    return logger_dirs_;
  }

  // Worker side logic
  virtual void RegisterWorker() override;
  virtual void DeregisterWorker() override;

  virtual void StartTxn(concurrency::Transaction *txn) override ;

  void LogInsert(const ItemPointer &tuple_pos);
  void LogUpdate(const ItemPointer &tuple_pos, const ItemPointer &old_pos);
  void LogDelete(const ItemPointer &tuple_pos, const ItemPointer &old_pos);

  void StartPersistTxn();
  void EndPersistTxn();


  // Logger side logic
  virtual void DoRecovery(const size_t &begin_eid) override;
  virtual void StartLoggers() override;
  virtual void StopLoggers() override;


  void RunPepochLogger();

private:

  void WriteRecordToBuffer(LogRecord &record);
  size_t RecoverPepoch();

private:
  std::atomic<oid_t> worker_count_;

  std::vector<std::string> logger_dirs_;

  std::vector<std::shared_ptr<ReorderedPhysicalLogger>> loggers_;

  std::unique_ptr<std::thread> pepoch_thread_;
  volatile bool is_running_;

  std::string pepoch_dir_;

  const std::string pepoch_filename_ = "pepoch";
};

}
}