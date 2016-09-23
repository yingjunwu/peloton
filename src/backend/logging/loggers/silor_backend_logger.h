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

#include <vector>
#include <list>

#include "backend/logging/loggers/backend_logger.h"

namespace peloton {
namespace logging {

class SiloRBackendLogger : public BackendLogger {
public:
  struct LogBufferMap {
    std::vector<std::list<std::unique_ptr<LogBuffer>>> buffer_map_;

    LogBufferMap() : buffer_map_(maximum_backend_logger_count) {};
    ~LogBufferMap() {}
  };

  SiloRBackendLogger() : BackendLogger(),
    current_buffer_ptr_(nullptr), current_eid_(INVALID_EPOCH_ID), current_cid_(INVALID_CID) {
    PL_ASSERT(GetBackendLoggerId() < maximum_backend_logger_count);
  }
  ~SiloRBackendLogger() {}

  virtual void StartTxn(concurrency::Transaction *txn) override ;
  virtual void LogInsert(const ItemPointer &tuple_pos) override ;
  virtual void LogDelete(const ItemPointer &tuple_pos) override ;
  virtual void LogUpdate(const ItemPointer &tuple_pos) override ;
  // virtual void AbortCurrentTxn() override ;
  virtual void CommitCurrentTxn() override ;

  virtual void PublishCurrentLogBuffer() override ;

private:
  void WriteRecord(LogRecord& record);
  LogBuffer *GetCurrentLogBufferPtr() {
    PL_ASSERT(current_eid_ != INVALID_EPOCH_ID);
    return per_epoch_buffermaps_vector[current_eid_].buffer_map_[GetBackendLoggerId()].back().get();
  }

  void RegisterBufferToEpoch(std::unique_ptr<LogBuffer> log_buffer_ptr);

private:
  static const size_t maximum_backend_logger_count = 64;

  static std::vector<LogBufferMap> per_epoch_buffermaps_vector;

  // Backend logger don't have the onwership of this pointer
  LogBuffer * current_buffer_ptr_;

  size_t current_eid_;
  cid_t current_cid_;
};

}
}
