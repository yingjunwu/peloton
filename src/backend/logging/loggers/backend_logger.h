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
    : buffer_pool_(), current_buffer_ptr_(), output_buffer_() {
    // Get the inital buffer
    current_buffer_ptr_ = buffer_pool_.GetBuffer();
  }

  ~BackendLogger() {}

  virtual void StartTxn(concurrency::Transaction *) = 0;

  virtual void LogInsert(const ItemPointer &) = 0;

  virtual void LogDelete(const ItemPointer &) = 0;

  virtual void LogUpdate(const ItemPointer &) = 0;

  virtual void AbortCurrentTxn() = 0;

  virtual void CommitCurrentTxn() = 0;

  // Send the current log buffer to some place
  virtual void PublishCurrentLogBuffer() = 0;

protected:
  BackendBufferPool buffer_pool_;

  std::unique_ptr<LogBuffer> current_buffer_ptr_;

  // TODO: We have an extra copy in this buffer
  // Change the CopySerializeOutput API to reduce this extra copy.
  CopySerializeOutput output_buffer_;
};

}
}
