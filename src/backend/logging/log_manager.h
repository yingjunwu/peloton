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

class LogManager {
  LogManager(const LogManager &) = delete;
  LogManager &operator=(const LogManager &) = delete;
  LogManager(LogManager &&) = delete;
  LogManager &operator=(LogManager &&) = delete;

public:
  LogManager(const std::string &log_dir) : peloton_log_directory(log_dir) {
    // TODO: check the existence of the directory
  }

  virtual ~LogManager() {}

  virtual void CreateLogWorker() = 0;
  virtual void TerminateLogWorker() = 0;

  virtual void LogInsert(const ItemPointer &tuple_pos) = 0;
  virtual void LogUpdate(const ItemPointer &tuple_pos) = 0;
  virtual void LogDelete(const ItemPointer &tuple_pos_deleted) = 0;
  virtual void StartTxn(concurrency::Transaction *txn) = 0;
  virtual void CommitCurrentTxn() = 0;

  virtual void StartLogger() = 0;
  virtual void StopLogger() = 0;

  inline std::string GetLogDirectoryName() { return peloton_log_directory; }

private:
  std::string peloton_log_directory;
};

}
}