//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_manager_factory.cpp
//
// Identification: src/backend/concurrency/epoch_manager_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "epoch_manager_factory.h"

namespace peloton {
namespace concurrency {
  EpochType EpochManagerFactory::epoch_type_ = EPOCH_SINGLE_QUEUE;
  double EpochManagerFactory::epoch_length_ = 10;
}
}
