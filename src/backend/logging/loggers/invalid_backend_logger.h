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

  void StartTxn(UNUSED_ATTRIBUTE concurrency::Transaction *txn) {}
  void LogInsert(UNUSED_ATTRIBUTE const ItemPointer &pos) {}
  void LogDelete(UNUSED_ATTRIBUTE const ItemPointer &pos) {}

  void LogUpdate(UNUSED_ATTRIBUTE const ItemPointer &pos) {}

  void AbortCurrentTxn() {}

  void CommitCurrentTxn() {}

  void PublishCurrentLogBuffer() {};
}

}
}