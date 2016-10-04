//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/phylog_frontend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/logging/log_manager.h"
#include "backend/logging/durability_factory.h"

namespace peloton {
namespace logging {

thread_local PhylogWorkerContext* tl_phylog_worker_ctx = nullptr;


}
}