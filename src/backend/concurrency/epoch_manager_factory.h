//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_manager.h
//
// Identification: src/backend/concurrency/epoch_manager_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <thread>
#include <vector>

#include "backend/concurrency/epoch_manager.h"
#include "backend/concurrency/single_queue_epoch_manager.h"
#include "backend/concurrency/snapshot_epoch_manager.h"
#include "backend/concurrency/localized_epoch_manager.h"
#include "backend/concurrency/localized_snapshot_epoch_manager.h"

namespace peloton {
namespace concurrency {

class EpochManagerFactory {
 public:
  static EpochManager &GetInstance() {
    switch (epoch_type_) {
      case EPOCH_SINGLE_QUEUE:
        return SingleQueueEpochManager::GetInstance(epoch_length_);
      case EPOCH_SNAPSHOT:
        return SnapshotEpochManager::GetInstance(epoch_length_);
      case EPOCH_LOCALIZED:
        return LocalizedEpochManager::GetInstance(epoch_length_, max_worker_count_);
      case EPOCH_LOCALIZED_SNAPSHOT:
        return LocalizedSnapshotEpochManager::GetInstance(epoch_length_, max_worker_count_);
      default:
        return SingleQueueEpochManager::GetInstance(epoch_length_);

    }
  }

  static bool IsLocalizedEpochManager() {
    return epoch_type_ == EPOCH_LOCALIZED || epoch_type_ == EPOCH_LOCALIZED_SNAPSHOT;
  }

  static EpochType GetType() {
    return epoch_type_;
  }

  static void Configure(EpochType type, double epoch_length) {
    epoch_length_ = epoch_length;
    epoch_type_ = type;
  }

 private:
  static const size_t max_worker_count_ = 256;
  static double epoch_length_;
  static EpochType epoch_type_;
};

}
}

