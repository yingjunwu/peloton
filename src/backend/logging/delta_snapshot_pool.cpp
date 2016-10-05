//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delta_snapshot_pool.cpp
//
// Identification: src/backend/logging/delta_snapshot_pool.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/logging/delta_snapshot_pool.h"

namespace peloton {
namespace logging {

  // Acquire a snapshot from the snapshot pool.
  // This function will be blocked until there is an available snapshot.
  // Note that only the corresponding worker thread can call this function.
  std::unique_ptr<DeltaSnapshot> DeltaSnapshotPool::GetSnapshot() {
    size_t head_idx = head_ % snapshot_queue_size_;
    while (true) {
      if (head_.load() < tail_.load() - 1) {
        if (snapshot_queue_[head_idx] == false) {
          // Not any buffer allocated now
          snapshot_queue_[head_idx].reset(new DeltaSnapshot(worker_id_));
        }
        break;
      }

      // sleep a while, and try to get a new buffer
      _mm_pause();
      LOG_TRACE("Worker %d uses up its buffer", (int) worker_id_);
    }

    head_.fetch_add(1, std::memory_order_relaxed);

    return std::move(snapshot_queue_[head_idx]);
  }

  // This function is called only by the corresponding logger.
  void DeltaSnapshotPool::PutSnapshot(std::unique_ptr<DeltaSnapshot> snapshot) {
    PL_ASSERT(snapshot.get() != nullptr);
    PL_ASSERT(snapshot->worker_id_ == worker_id_);

    size_t tail_idx = tail_ % snapshot_queue_size_;
    
    // The buffer pool must not be full
    PL_ASSERT(tail_idx != head_ % snapshot_queue_size_);
    // The tail pos must be null
    PL_ASSERT(snapshot_queue_[tail_idx] == false);
    // The returned buffer must be empty
    PL_ASSERT(snapshot->data_.size() == 0);
    snapshot_queue_[tail_idx].reset(snapshot.release());
    
    tail_.fetch_add(1, std::memory_order_relaxed);
  }

}
}
