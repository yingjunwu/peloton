//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_log_manager.cpp
//
// Identification: src/backend/logging/epoch_log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <backend/concurrency/epoch_manager_factory.h>

#include "backend/logging/epoch_log_manager.h"
#include "backend/catalog/manager.h"
#include "backend/expression/container_tuple.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace logging {

thread_local EpochWorkerContext* tl_epoch_worker_ctx = nullptr;

void EpochLogManager::StartTxn(concurrency::Transaction *txn) {
  PL_ASSERT(tl_epoch_worker_ctx);
  size_t txn_eid = txn->GetEpochId();
  tl_epoch_worker_ctx->current_eid = txn_eid;
}

void EpochLogManager::LogInsert(UNUSED_ATTRIBUTE const ItemPointer &master_entry, UNUSED_ATTRIBUTE const ItemPointer &tuple_pos) {
  // tl_epoch_worker_ctx->delta_snapshot_[master_entry.block][master_entry.offset] = tuple_pos;
}

void EpochLogManager::LogUpdate(UNUSED_ATTRIBUTE const ItemPointer &master_entry, UNUSED_ATTRIBUTE const ItemPointer &tuple_pos) {
  // tl_epoch_worker_ctx->delta_snapshot_[master_entry.block][master_entry.offset] = tuple_pos;
}

void EpochLogManager::LogDelete(UNUSED_ATTRIBUTE const ItemPointer &master_entry, UNUSED_ATTRIBUTE const ItemPointer &tuple_pos_deleted) {
  // tl_epoch_worker_ctx->delta_snapshot_[master_entry.block][master_entry.offset] = tuple_pos_deleted;
}

void EpochLogManager::StartLoggers() {
  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    LOG_TRACE("Start logger %d", (int) logger_id);
    loggers_[logger_id]->StartLogging();
  }
}

void EpochLogManager::StopLoggers() {
  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    loggers_[logger_id]->StopLogging();
  }
}

}
}