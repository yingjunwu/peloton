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

  static void Configure(LoggingType logging_type, CheckpointType checkpoint_type) {
    logging_type_ = logging_type;
    checkpoint_type_ = checkpoint_type;
  }

  static LoggingType GetLoggingType() { return logging_type_; }

  static CheckpointType GetCheckpointType() { return checkpoint_type_; }

 private:
  // GC type
  static LoggingType logging_type_;

  static CheckpointType checkpoint_type_;
};
} // namespace gc
} // namespace peloton
