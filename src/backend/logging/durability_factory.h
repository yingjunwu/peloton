//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// durability_factory.h
//
// Identification: src/backend/logging/durability_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/logging/checkpoint_manager.h"
#include "backend/logging/phylog_log_manager.h"
#include "backend/logging/dummy_log_manager.h"

namespace peloton {
namespace logging {

class DurabilityFactory {
 public:

  static LogManager& GetLoggerInstance() {
    switch (logging_type_) {
      case LOGGING_TYPE_PHYLOG:
        return PhyLogLogManager::GetInstance();
      default:
        return DummyLogManager::GetInstance();
    }
  }

  static CheckpointManager &GetCheckpointerInstance() {
    return CheckpointManager::GetInstance();
  }

  static void Configure(LoggingType logging_type, CheckpointType checkpoint_type, bool timer_on = false) {
    logging_type_ = logging_type;
    checkpoint_type_ = checkpoint_type;
    timer_flag = timer_on;
  }

  inline static LoggingType GetLoggingType() { return logging_type_; }

  inline static CheckpointType GetCheckpointType() { return checkpoint_type_; }

  inline static bool GetTimerFlag() { return timer_flag; }


  /* Statistics */
  static void StartTxnTimer(size_t eid, WorkerLogContext *worker_ctx);
  static void StopTimersByPepoch(size_t persist_eid, WorkerLogContext *worker_ctx);

 private:
  static uint64_t GetCurrentTimeInUsec() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return ((uint64_t)tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  static LoggingType logging_type_;
  static CheckpointType checkpoint_type_;
  static bool timer_flag;

};
} // namespace gc
} // namespace peloton
