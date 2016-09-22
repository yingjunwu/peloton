//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/silor_backend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "backend/logging/loggers/backend_logger.h"

namespace peloton {
namespace logging {

class SiloRBackendLogger : public BackendLogger {
  // Deleted functions
  SiloRBackendLogger(const SiloRBackendLogger &) = delete;
  SiloRBackendLogger &operator=(const SiloRBackendLogger &) = delete;
  SiloRBackendLogger(SiloRBackendLogger &&) = delete;
  SiloRBackendLogger &operator=(const SiloRBackendLogger &&) = delete;

  virtual void StartTxn(concurrency::Transaction *txn);
  virtual void LogInsert(const ItemPointer &tuple_pos);
  virtual void LogDelete(const ItemPointer &tuple_pos);
  virtual void LogUpdate(const ItemPointer &tuple_pos);
  virtual void AbortCurrentTxn();
  virtual void CommitCurrentTxn();

private:
  size_t current_eid_;

};

}
}
