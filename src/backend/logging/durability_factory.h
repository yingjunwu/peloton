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
#include "backend/logging/epoch_log_manager.h"
#include "backend/logging/dummy_log_manager.h"

namespace peloton {
namespace logging {

class DurabilityFactory {
 public:

  static LogManager& GetLoggerInstance() {
    switch (logging_type_) {
      case LOGGING_TYPE_PHYLOG:
        return PhyLogLogManager::GetInstance();
      case LOGGING_TYPE_EPOCH:
        return EpochLogManager::GetInstance();
      default:
        return DummyLogManager::GetInstance();
    }
  }

  static CheckpointManager &GetCheckpointerInstance() {
    return CheckpointManager::GetInstance();
  }

  static void Configure(LoggingType logging_type, CheckpointType checkpoint_type, TimerType timer_type) {
    logging_type_ = logging_type;
    checkpoint_type_ = checkpoint_type;
    timer_type_ = timer_type;
  }

  inline static LoggingType GetLoggingType() { return logging_type_; }

  inline static CheckpointType GetCheckpointType() { return checkpoint_type_; }

  inline static TimerType GetTimerType() { return timer_type_; }


  /* Statistics */
  static void StartTxnTimer(size_t eid, PhylogWorkerContext *worker_ctx);
  static void StopTimersByPepoch(size_t persist_eid, PhylogWorkerContext *worker_ctx);
  
  static void StartTxnTimer(size_t eid, EpochWorkerContext *worker_ctx);
  static void StopTimersByPepoch(size_t persist_eid, EpochWorkerContext *worker_ctx);

 private:
  static uint64_t GetCurrentTimeInUsec() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return ((uint64_t)tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  static LoggingType logging_type_;
  static CheckpointType checkpoint_type_;
  static TimerType  timer_type_;

};
} // namespace gc
} // namespace peloton
