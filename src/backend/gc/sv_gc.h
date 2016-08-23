//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sv_gc.h
//
// Identification: src/backend/gc/sv_gc.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <thread>
#include <unordered_map>
#include <map>
#include <vector>
#include <queue>

#include "backend/common/types.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "backend/gc/gc_manager.h"


namespace peloton {
namespace gc {

struct SV_GCContext {

  ItemPointer PopFront(const oid_t table_id) {
    if (item_pointer_queues_[table_id].size() == 0) {
      return INVALID_ITEMPOINTER;
    } else {
      ItemPointer ret_item_pointer = item_pointer_queues_[table_id].front();
      item_pointer_queues_[table_id].pop();
      return ret_item_pointer;
    }
  }

  void PushBack(const oid_t table_id, const ItemPointer &item_pointer) {
    item_pointer_queues_[table_id].push(item_pointer);
  }

  std::unordered_map<oid_t, std::queue<ItemPointer>> item_pointer_queues_;
};

extern thread_local SV_GCContext current_gc_context;


class SV_GCManager : public GCManager {
 public:
  SV_GCManager(const SV_GCManager &) = delete;
  SV_GCManager &operator=(const SV_GCManager &) = delete;
  SV_GCManager(SV_GCManager &&) = delete;
  SV_GCManager &operator=(SV_GCManager &&) = delete;

  SV_GCManager() {}

  virtual ~SV_GCManager() {}


  static SV_GCManager& GetInstance(UNUSED_ATTRIBUTE int thread_count = 1) {
    static SV_GCManager gc_manager;
    return gc_manager;
  }

  // Get status of whether GC thread is running or not
  virtual bool GetStatus() { return true; }

  virtual void StartGC() {}

  virtual void StopGC() {}

  // recycle old version
  virtual void RecycleOldTupleSlot(const oid_t &table_id UNUSED_ATTRIBUTE, 
                                   const oid_t &tile_group_id UNUSED_ATTRIBUTE,
                                   const oid_t &tuple_id UNUSED_ATTRIBUTE, 
                                   const cid_t &tuple_end_cid UNUSED_ATTRIBUTE){}

  // recycle invalid version
  virtual void RecycleInvalidTupleSlot(const oid_t &table_id, const oid_t &tile_group_id, const oid_t &tuple_id) {
    current_gc_context.PushBack(table_id, ItemPointer(tile_group_id, tuple_id));
  }

  virtual ItemPointer ReturnFreeSlot(const oid_t &table_id) {
    return current_gc_context.PopFront(table_id);
  }

  virtual void RegisterTable(oid_t table_id UNUSED_ATTRIBUTE) {}

};
}
}
