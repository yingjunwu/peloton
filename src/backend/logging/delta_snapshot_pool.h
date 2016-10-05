//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delta_snapshot_pool.h
//
// Identification: src/backend/logging/delta_snapshot_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "backend/common/macros.h"
#include "backend/common/types.h"
#include "backend/common/platform.h"

#include "backend/logging/delta_snapshot.h"

namespace peloton {
namespace logging {
  class DeltaSnapshotPool {
    DeltaSnapshotPool(const DeltaSnapshotPool &) = delete;
    DeltaSnapshotPool &operator=(const DeltaSnapshotPool &) = delete;
    DeltaSnapshotPool(const DeltaSnapshotPool &&) = delete;
    DeltaSnapshotPool &operator=(const DeltaSnapshotPool &&) = delete;

  public:
    DeltaSnapshotPool(size_t worker_id) : 
      head_(0), 
      tail_(snapshot_queue_size_),
      worker_id_(worker_id),
      snapshot_queue_(snapshot_queue_size_) {}

    std::unique_ptr<DeltaSnapshot> GetSnapshot();

    void PutSnapshot(std::unique_ptr<DeltaSnapshot> snapshot);

    inline size_t GetWorkerId() { return worker_id_; }

  private:
    static const size_t snapshot_queue_size_ = 16;
    std::atomic<size_t> head_;
    std::atomic<size_t> tail_;
    
    size_t worker_id_;

    std::vector<std::unique_ptr<DeltaSnapshot>> snapshot_queue_;
};

}
}
