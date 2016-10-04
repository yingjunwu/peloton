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

#include "backend/common/macros.h"
#include "backend/concurrency/transaction.h"
#include "backend/logging/phylog_worker_context.h"

namespace peloton {
namespace logging {

/* Per worker thread local context */
extern thread_local PhylogWorkerContext* tl_phylog_worker_ctx;

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

  virtual void DoRecovery() = 0;

  virtual void StartLoggers() = 0;
  virtual void StopLoggers() = 0;

protected:
  size_t logger_count_;

};

}
}