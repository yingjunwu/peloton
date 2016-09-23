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

// A Dummy backend logger
class InvalidBackendLogger : public BackendLogger {
public :
  InvalidBackendLogger() {}
  ~InvalidBackendLogger() {}

  void StartTxn(UNUSED_ATTRIBUTE concurrency::Transaction *txn) final {}
  void LogInsert(UNUSED_ATTRIBUTE const ItemPointer &pos) final {};
  void LogDelete(UNUSED_ATTRIBUTE const ItemPointer &pos) final {}

  void LogUpdate(UNUSED_ATTRIBUTE const ItemPointer &pos) final {}

  // void AbortCurrentTxn() final {}

  void CommitCurrentTxn() final {}

  void PublishCurrentLogBuffer() final {};
}

}
}