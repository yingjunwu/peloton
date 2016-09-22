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

#include "backend/logging/checkpoint/checkpointer.h"
#include "backend/logging/loggers/silor_backend_logger.h"

#define TMP_DIR = "/tmp/"

namespace peloton {
namespace logging {

class DurabilityFactory {
 public:
  static FrontendLogger &GetFrontendLoggerInstance() {
    return FrontendLogger::GetInstance();
  }

  static BackendLogger &GetBackendLoggerInstance() {
    return BackendLogger::GetInstance();
  }

  static Checkpointer &GetCheckpointerInstance() {
    return Checkpointer::GetInstance();
  }

  static void Configure(LoggingType logging_type, CheckpointType checkpoint_type, int frontend_logger_count = default_frontend_logger_count_, int checkpointer_count = default_checkpointer_count_) {

    logging_type_ = logging_type;
    checkpoint_type_ = checkpoint_type;

    frontend_logger_count_ = frontend_logger_count;
    checkpointer_count_ = checkpointer_count;

  }

  static LoggingType GetLoggingType() { return logging_type_; }

  static CheckpointType GetCheckpointType() { return checkpoint_type_; }

 private:

  static std::string logging_dir_prefix_;

  static std::string checkpoint_dir_prefix_;

  const static std::string logging_filename_prefix_ = "logging_";

  const static std::string checkpoint_filename_prefix_ = "checkpoint_";

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
