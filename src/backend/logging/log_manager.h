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
    // if (recovery_thread_count_ > max_checkpointer_count_) {
    //   LOG_ERROR("# recovery thread cannot be larger than # max checkpointer");
    //   exit(EXIT_FAILURE);
    // }
  }

  virtual void RegisterWorker() = 0;
  virtual void DeregisterWorker() = 0;

  virtual void DoRecovery() = 0;

  virtual void StartLoggers() = 0;
  virtual void StopLoggers() = 0;

  size_t GetPersistEpochId() {
    return global_persist_epoch_id_.load();
  }

protected:
  size_t logger_count_;

  size_t recovery_thread_count_;

  std::atomic<size_t> global_persist_epoch_id_;

};

}
}