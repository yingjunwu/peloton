//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// phylog_log_manager.h
//
// Identification: src/backend/logging/loggers/phylog_log_manager.h
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
#include "backend/logging/worker_log_context.h"
#include "backend/logging/epoch_logger.h"
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
 *  | epoch_id | database_id | table_id | operation_type | column_count | columns | data | ... | epoch_end_flag
 *  -----------------------------------------------------------------------------
 *
 * NOTE: this layout is designed for epoch logging.
 *
 * NOTE: tuple length can be obtained from the table schema.
 *
 */

class EpochLogManager : public LogManager {
  EpochLogManager(const EpochLogManager &) = delete;
  EpochLogManager &operator=(const EpochLogManager &) = delete;
  EpochLogManager(EpochLogManager &&) = delete;
  EpochLogManager &operator=(EpochLogManager &&) = delete;

protected:

  EpochLogManager(){}

public:
  static EpochLogManager &GetInstance() {
    static EpochLogManager log_manager;
    return log_manager;
  }
  virtual ~EpochLogManager() {}

  virtual void SetDirectories(const std::vector<std::string> &logging_dirs) override {
    
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
      loggers_.emplace_back(new EpochLogger(i, logging_dirs.at(i)));
    }
  }

  // Worker side logic

  virtual void LogInsert(const size_t &epoch_id, const ItemPointer &master_entry, const ItemPointer &tuple_pos) override ;
  virtual void LogUpdate(const size_t &epoch_id, const ItemPointer &master_entry, const ItemPointer &tuple_pos) override ;
  virtual void LogDelete(const size_t &epoch_id, const ItemPointer & master_entry, const ItemPointer &tuple_pos_deleted) override ;

  virtual void DoRecovery() override {}

  // Logger side logic
  virtual void StartLoggers() override ;
  virtual void StopLoggers() override ;

private:

  std::vector<std::shared_ptr<EpochLogger>> loggers_;

  std::unordered_map<oid_t, std::unordered_map<oid_t, ItemPointer>> delta_snapshots_[16];

};

}
}