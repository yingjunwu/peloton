//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "backend/logging/log_manager.h"

namespace peloton {
namespace logging {

thread_local WorkerContext* tl_worker_ctx = nullptr;

}
}