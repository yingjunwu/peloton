//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/backend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction.h"
#include "backend/logging/backend_buffer_pool.h"

namespace peloton {
namespace logging {

class BackendLogger {

public:
  BackendLogger() : buffer_pool_() {}
  ~BackendLogger() {}

  virtual void StartTxn(concurrency::Transaction *) = 0;

  virtual void LogInsert(const ItemPointer &) = 0;

  virtual void LogDelete(const ItemPointer &) = 0;

  virtual void LogUpdate(const ItemPointer &) = 0;

  virtual void AbortCurrentTxn() = 0;

  virtual void CommitCurrentTxn() = 0;

protected:
  void WriteRecord();

  BackendBufferPool buffer_pool_;
};

}
}
