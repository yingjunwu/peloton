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
  int EpochManagerFactory::epoch_length_ = 40;
}
}