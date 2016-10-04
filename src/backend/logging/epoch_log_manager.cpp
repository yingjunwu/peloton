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

void EpochLogManager::LogInsert(const size_t &epoch_id, const ItemPointer &master_entry, const ItemPointer &tuple_pos) {
  delta_snapshots_[epoch_id % 16][master_entry.block][master_entry.offset] = tuple_pos;
}

void EpochLogManager::LogUpdate(const size_t &epoch_id, const ItemPointer &master_entry, const ItemPointer &tuple_pos) {
  delta_snapshots_[epoch_id % 16][master_entry.block][master_entry.offset] = tuple_pos;
}

void EpochLogManager::LogDelete(const size_t &epoch_id, const ItemPointer &master_entry, const ItemPointer &tuple_pos_deleted) {
  delta_snapshots_[epoch_id % 16][master_entry.block][master_entry.offset] = tuple_pos_deleted;
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