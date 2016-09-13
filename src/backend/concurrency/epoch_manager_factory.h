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

namespace peloton {
namespace concurrency {

class EpochManagerFactory {
 public:
  static EpochManager &GetInstance() {
    static EpochManager epoch_manager(epoch_length_);
    return epoch_manager;
  }

  static void Configure(int epoch_length) {
    epoch_length_ = epoch_length;
  }

 private:
  static int epoch_length_;
};

}
}

