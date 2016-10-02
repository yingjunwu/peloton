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
  LogManager() {}

  virtual ~LogManager() {}

  virtual void SetDirectories(const std::vector<std::string> &logging_dirs) = 0;

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
  size_t logger_count_;

};

}
}