//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/backend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/logging/loggers/backend_logger.h"
#include "backend/catalog/manager.h"

namespace peloton {
namespace logging {

  std::atomic<size_t> BackendLogger::backend_logger_id_generator = 0;
}
}