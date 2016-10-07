//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delta_snapshot.h
//
// Identification: src/backend/logging/delta_snapshot.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>

#include "backend/common/types.h"

namespace peloton {
namespace logging {

struct DeltaSnapshot {
	
  DeltaSnapshot(const size_t &worker_id) {
  	worker_id_ = worker_id;
  }

  std::unordered_map<ItemPointer*, std::pair<ItemPointer, cid_t>> data_;

  size_t worker_id_;
};

}
}
