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

#include "backend/logging/records/tuple_record.h"
#include "backend/common/lockfree_queue.h"
#include "backend/logging/log_manager.h"

namespace peloton {
namespace logging {

class SiloRBackendLogger : public BackendLogger {
  // Deleted functions
  SiloRBackendLogger(const SiloRBackendLogger &) = delete;
  SiloRBackendLogger &operator=(const SiloRBackendLogger &) = delete;
  SiloRBackendLogger(SiloRBackendLogger &&) = delete;
  SiloRBackendLogger &operator=(const SiloRBackendLogger &&) = delete;

public:
  SiloRBackendLogger();

  ~SiloRBackendLogger();

  virtual void Log();

  virtual LogRecord *GetTupleRecord(LogRecordType log_record_type,
                                    txn_id_t txn_id, oid_t table_oid,
                                    oid_t db_oid, ItemPointer insert_location,
                                    ItemPointer delete_location,
                                    const void *data = nullptr);

private:

};

}
}
