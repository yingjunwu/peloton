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

public:
  SiloRBackendLogger() : current_eid_(INVALID_EPOCH_ID), current_cid_(INVALID_CID) {}
  ~SiloRBackendLogger() {}

  virtual void StartTxn(concurrency::Transaction *txn) override ;
  virtual void LogInsert(const ItemPointer &tuple_pos) override ;
  virtual void LogDelete(const ItemPointer &tuple_pos) override ;
  virtual void LogUpdate(const ItemPointer &tuple_pos) override ;
  virtual void AbortCurrentTxn() override ;
  virtual void CommitCurrentTxn() override ;

  virtual void PublishCurrentLogBuffer() override ;

private:
  void WriteRecord(LogRecord& record);

  size_t current_eid_;
  cid_t current_cid_;
};

}
}
