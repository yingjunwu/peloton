//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// durability_factory.h
//
// Identification: src/backend/concurrency/durability_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/logging/checkpoint/checkpointer.h"
#include "backend/logging/loggers/silor_backend_logger.h"

namespace peloton {
namespace logging {

class DurabilityFactory {
 public:
  static BackendLogger &GetBackendLoggerInstance() {
    switch (gc_type_) {
      case GC_TYPE_N2O_TXN:
        return N2OTxn_GCManager::GetInstance(gc_thread_count_);
      case GC_TYPE_N2O_EPOCH:
        return N2OEpochGCManager::GetInstance(gc_thread_count_);
      case GC_TYPE_OFF:
        return Off_GCManager::GetInstance();
      default:
        return Off_GCManager::GetInstance();
    }
  }

  static Checkpointer &GetCheckpointerInstance() {

  }

  static void Configure(GCType gc_type, int thread_count = default_gc_thread_count_) {
    if (gc_type != GC_TYPE_OFF && gc_type != GC_TYPE_N2O_TXN && gc_type != GC_TYPE_N2O_EPOCH) {
      // Enforce the default
      gc_type = GC_TYPE_OFF;
    }
    gc_type_ = gc_type;
    gc_thread_count_ = thread_count;
  }

  static GCType GetGCType() { return gc_type_; }

 private:

  static std::string logging_dir_prefix_;

  static std::string checkpoint_dir_prefix_;

  const static std::string logging_filename_prefix_;

  const static std::string checkpoint_filename_prefix_;

  // GC type
  static LoggingType logging_type_;

  static CheckpointType checkpoint_type_;

  // thread count
  static int frontend_logger_count_;
  static int checkpointer_count_;
  const static int default_frontend_logger_count_ = 1;
  const static int default_checkpointer_count_ = 1;
};
} // namespace gc
} // namespace peloton
