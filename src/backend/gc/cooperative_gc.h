//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cooperative_gc.h
//
// Identification: src/backend/gc/cooperative_gc.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <unordered_map>
#include <map>
#include <vector>

#include "backend/common/types.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "backend/gc/gc_manager.h"
#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace gc {

//===--------------------------------------------------------------------===//
// GC Manager
//===--------------------------------------------------------------------===//

class Cooperative_GCManager : public GCManager {
public:
  Cooperative_GCManager(int thread_count)
    : is_running_(true),
      gc_thread_count_(thread_count),
      gc_threads_(thread_count) {
    for(int i = 0; i < thread_count; i++) {
      std::shared_ptr<LockfreeQueue<TupleMetadata>> queue(new LockfreeQueue<TupleMetadata>(MAX_QUEUE_LENGTH));
      reclaim_queues_.emplace_back(queue);
    }
    StartGC();
  }

  virtual ~Cooperative_GCManager() { StopGC(); }

  static Cooperative_GCManager &GetInstance(int thread_count = 1) {
    static Cooperative_GCManager gcManager(thread_count);
    return gcManager;
  }

  // Get status of whether GC thread is running or not
  virtual bool GetStatus() { return this->is_running_; }

  virtual void StartGC();

  virtual void StopGC();


  virtual void RecycleOldTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                        const oid_t &tuple_id, const cid_t &tuple_end_cid);


  virtual void RecycleInvalidTupleSlot(const oid_t &table_id, const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual ItemPointer ReturnFreeSlot(const oid_t &table_id);

  virtual void RegisterTable(oid_t table_id) {
    // Insert a new entry for the table
    if (recycle_queue_map_.find(table_id) == recycle_queue_map_.end()) {
      LOG_TRACE("register table %d to garbage collector", (int)table_id);
      std::shared_ptr<LockfreeQueue<TupleMetadata>> recycle_queue(new LockfreeQueue<TupleMetadata>(MAX_QUEUE_LENGTH));
      recycle_queue_map_[table_id] = recycle_queue;
    }
  }

private:
  void ClearGarbage();
  
  void Running(int thread_id);

  void AddToRecycleMap(const TupleMetadata &tuple_metadata);

  bool ResetTuple(const TupleMetadata &);

  inline int HashToThread(const oid_t &tuple_id) {
    return tuple_id % gc_thread_count_;
  }

private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//
  volatile bool is_running_;

  const int gc_thread_count_;
  std::vector<std::unique_ptr<std::thread>> gc_threads_;

  std::vector<std::shared_ptr<LockfreeQueue<TupleMetadata>>> reclaim_queues_;

  // TODO: use shared pointer to reduce memory copy
  // cuckoohash_map<oid_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> recycle_queue_map_;

  std::unordered_map<oid_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> recycle_queue_map_;
  //std::unordered_map<oid_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> recycle_queue_map_;
};

}  // namespace gc
}  // namespace peloton
