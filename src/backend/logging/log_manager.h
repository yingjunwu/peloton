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
#include "backend/logging/worker_log_context.h"

namespace peloton {
namespace logging {

/* Per worker thread local context */
extern thread_local WorkerLogContext* tl_worker_log_ctx;

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

  virtual void RegisterWorkerToLogger() {
    PL_ASSERT(false);
  }
  virtual void DeregisterWorkerFromLogger() {
    PL_ASSERT(false);
  }

  virtual void LogInsert(UNUSED_ATTRIBUTE const size_t &,
                         UNUSED_ATTRIBUTE const ItemPointer &, 
                         UNUSED_ATTRIBUTE const ItemPointer &) {
    PL_ASSERT(false);
  }
  virtual void LogUpdate(UNUSED_ATTRIBUTE const size_t &, 
                         UNUSED_ATTRIBUTE const ItemPointer &,
                         UNUSED_ATTRIBUTE const ItemPointer &) {
    PL_ASSERT(false);
  }
  virtual void LogDelete(UNUSED_ATTRIBUTE const size_t &, 
                         UNUSED_ATTRIBUTE const ItemPointer &,
                         UNUSED_ATTRIBUTE const ItemPointer &) {
    PL_ASSERT(false);
  }

  virtual void LogInsert(UNUSED_ATTRIBUTE const ItemPointer &) {
    PL_ASSERT(false);
  }
  virtual void LogUpdate(UNUSED_ATTRIBUTE const ItemPointer &) {
    PL_ASSERT(false);
  }
  virtual void LogDelete(UNUSED_ATTRIBUTE const ItemPointer &) {
    PL_ASSERT(false);
  }
  virtual void StartTxn(UNUSED_ATTRIBUTE concurrency::Transaction *) {
    PL_ASSERT(false);
  }
  virtual void CommitCurrentTxn() {
    PL_ASSERT(false);
  }

  virtual void FinishPendingTxn() {
    PL_ASSERT(false);
  }

  virtual void DoRecovery() = 0;

  virtual void StartLoggers() = 0;
  virtual void StopLoggers() = 0;

protected:
  size_t logger_count_;

};

}
}