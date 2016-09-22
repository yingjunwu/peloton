//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// durability_factory.cpp
//
// Identification: src/backend/concurrency/durability_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "durability_factory.h"

namespace peloton {
namespace logging { 
  LoggingType DurabilityFactory::logging_type_ = LOGGING_TYPE_INVALID;
  CheckpointType DurabilityFactory::checkpoint_type_ = CHECKPOINT_TYPE_INVALID;

  int DurabilityFactory::frontend_logger_count_ = default_frontend_logger_count_;

  int DurabilityFactory::checkpointer_count_ = default_checkpointer_count_;
}
}
