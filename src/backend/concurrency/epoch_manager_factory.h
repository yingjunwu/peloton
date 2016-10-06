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

namespace peloton {
namespace concurrency {

class EpochManagerFactory {
 public:
  static EpochManager &GetInstance() {
    switch (epoch_type_) {
      case EPOCH_SINGLE_QUEUE:
        return SingleQueueEpochManager::GetInstance(epoch_length_);
      case EPOCH_SNAPSHOT:
      default:
        return SingleQueueEpochManager::GetInstance(epoch_length_);

    }
  }

  static void Configure(EpochType type, int epoch_length) {
    epoch_length_ = epoch_length;
    epoch_type_ = type;
  }

 private:
  static int epoch_length_;
  static EpochType epoch_type_;
};

}
}

