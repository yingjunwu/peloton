//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "backend/concurrency/transaction.h"

namespace peloton {
namespace logging {

// loggers are created before workers.
class LogManager {
  LogManager(const LogManager &) = delete;
  LogManager &operator=(const LogManager &) = delete;
  LogManager(LogManager &&) = delete;
  LogManager &operator=(LogManager &&) = delete;

public:
  LogManager(int thread_count) 
    : is_running_(false), 
      logging_thread_count_(thread_count), 
      logging_dirs_(thread_count, TMP_DIR) {}
  virtual ~LogManager() {}

  void SetDirectories(const std::vector<std::string> &logging_dirs) {
    logging_dirs_ = logging_dirs;
  }


  virtual void RegisterWorkerToLogger() = 0;
  virtual void DeregisterWorkerFromLogger() = 0;

  virtual void LogInsert(const ItemPointer &tuple_pos) = 0;
  virtual void LogUpdate(const ItemPointer &tuple_pos) = 0;
  virtual void LogDelete(const ItemPointer &tuple_pos_deleted) = 0;
  virtual void StartTxn(concurrency::Transaction *txn) = 0;
  virtual void CommitCurrentTxn() = 0;

  virtual void StartLoggers() = 0;
  virtual void StopLoggers() = 0;

protected:
  std::string GetLogFileFullPath(size_t logger_id, size_t epoch_id) {
    return logging_dirs_.at(logger_id) + "/" + logging_filename_prefix_ + "_" + std::to_string(logger_id) + "_" + std::to_string(epoch_id);
  }

protected:
  bool is_running_;

  size_t logging_thread_count_;
  std::vector<std::string> logging_dirs_;

  const std::string logging_filename_prefix_ = "log";

};

}
}