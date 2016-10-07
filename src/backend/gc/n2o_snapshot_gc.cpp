//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// n2o_snapshot_gc.cpp
//
// Identification: src/backend/gc/n2o_snapshot_gc.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/gc/n2o_snapshot_gc.h"
#include "backend/concurrency/epoch_manager_factory.h"
#include "backend/concurrency/snapshot_epoch_manager.h"

namespace peloton {
namespace gc {

// An extra context for snapshot versions
thread_local EpochGarbageContext *current_snapshot_epoch_garbage_context = nullptr;


void N2OSnapshotGCManager::Running(int thread_id) {
  // Check if we can move anything from the possibly free list to the free list.

  std::this_thread::sleep_for(
    std::chrono::milliseconds(GC_PERIOD_MILLISECONDS));
  while (true) {
    PL_ASSERT(concurrency::EpochManagerFactory::GetType() == EPOCH_SNAPSHOT);
    auto epoch_manager_ptr = reinterpret_cast<concurrency::SnapshotEpochManager*>(&concurrency::EpochManagerFactory::GetInstance());

    size_t max_rw_eid = epoch_manager_ptr->GetMaxDeadEidForRwGC();
    size_t max_ro_eid = epoch_manager_ptr->GetMaxDeadEidForSnapshotGC();

    PL_ASSERT(max_rw_eid != MAX_EPOCH_ID);
    PL_ASSERT(max_ro_eid != MAX_EPOCH_ID);

    snapshot_gc_manager.Reclaim(thread_id, max_ro_eid);
    rw_gc_manager.Reclaim(thread_id, max_rw_eid);

    snapshot_gc_manager.Unlink(thread_id, max_ro_eid);
    rw_gc_manager.Unlink(thread_id, max_rw_eid);

    if (is_running_ == false) {
      return;
    }
  }
}

}  // namespace gc
}  // namespace peloton
