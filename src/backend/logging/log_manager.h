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
#include <cmath>

#include "backend/common/macros.h"
#include "backend/concurrency/transaction.h"
#include "backend/logging/worker_context.h"

namespace peloton {
namespace logging {


/* Per worker thread local context */
extern thread_local WorkerContext* tl_worker_ctx;

// loggers are created before workers.
class LogManager {
  LogManager(const LogManager &) = delete;
  LogManager &operator=(const LogManager &) = delete;
  LogManager(LogManager &&) = delete;
  LogManager &operator=(LogManager &&) = delete;

public:
  LogManager() : global_persist_epoch_id_(INVALID_EPOCH_ID) {}

  virtual ~LogManager() {}

  virtual void SetDirectories(const std::vector<std::string> &logging_dirs) = 0;

  void SetRecoveryThreadCount(const size_t &recovery_thread_count) {
    recovery_thread_count_ = recovery_thread_count;
  }

  virtual void RegisterWorker() = 0;
  virtual void DeregisterWorker() = 0;

  virtual void DoRecovery(const size_t &begin_eid) = 0;

  virtual void StartLoggers() = 0;
  virtual void StopLoggers() = 0;

  size_t GetPersistEpochId() {
    return global_persist_epoch_id_.load();
  }

protected:
  size_t logger_count_;

  size_t recovery_thread_count_ = 1;

  std::atomic<size_t> global_persist_epoch_id_;

};

}
}