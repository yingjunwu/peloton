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

#include "backend/logging/checkpointer.h"
#include "backend/logging/phylog_log_manager.h"
#include "backend/logging/dummy_log_manager.h"

namespace peloton {
namespace logging {

class DurabilityFactory {
 public:

  static LogManager& GetLoggerInstance() {
    switch (logging_type_) {
      case LOGGING_TYPE_PHYLOG:
        return PhyLogLogManager::GetInstance("/tmp", 1);
      default:
        return DummyLogManager::GetInstance();
    }
  }

// TODO: Checkpointer or Checkpoint manager?
//  static Checkpointer &GetCheckpointerInstance() {
//    return Checkpointer::GetInstance(checkpointer_count_);
//  }

  static void Configure(LoggingType logging_type, CheckpointType checkpoint_type, int frontend_logger_count = default_frontend_logger_count_, int checkpointer_count = default_checkpointer_count_) {

    logging_type_ = logging_type;
    checkpoint_type_ = checkpoint_type;

    frontend_logger_count_ = frontend_logger_count;
    checkpointer_count_ = checkpointer_count;

  }

  static LoggingType GetLoggingType() { return logging_type_; }

  static CheckpointType GetCheckpointType() { return checkpoint_type_; }

 private:
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
