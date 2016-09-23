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

#include "backend/common/serializer.h"
#include "backend/concurrency/transaction.h"
#include "backend/logging/backend_buffer_pool.h"
#include "backend/logging/log_record.h"


namespace peloton {
namespace logging {

class BackendLogger {
  BackendLogger(const BackendLogger &) = delete;
  BackendLogger &operator=(const BackendLogger &) = delete;
  BackendLogger(BackendLogger &&) = delete;
  BackendLogger &operator=(BackendLogger &&) = delete;

public:

  BackendLogger()
    : backend_logger_id_(backend_logger_id_generator++), buffer_pool_(), output_buffer_() {}

  ~BackendLogger() {}

  virtual void StartTxn(concurrency::Transaction *) = 0;

  virtual void LogInsert(const ItemPointer &) = 0;

  virtual void LogDelete(const ItemPointer &) = 0;

  virtual void LogUpdate(const ItemPointer &) = 0;

  virtual void AbortCurrentTxn() = 0;

  virtual void CommitCurrentTxn() = 0;



  // Send the current log buffer to some place
  size_t GetBackendLoggerId() { return backend_logger_id_; }

  static size_t GetBackendLoggerCount() { return backend_logger_id_generator.load(); }

  void GrantLogBuffer();

protected:
  size_t backend_logger_id_;

  BackendBufferPool buffer_pool_;

  // TODO: We have an extra copy in this buffer
  // Change the CopySerializeOutput API to reduce this extra copy.
  CopySerializeOutput output_buffer_;

private:
  static std::atomic<size_t> backend_logger_id_generator;
};

}
}
