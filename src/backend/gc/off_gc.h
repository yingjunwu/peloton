//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// off_gc.h
//
// Identification: src/backend/gc/off_gc.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/gc/gc_manager.h"

namespace peloton {
namespace gc {

//===--------------------------------------------------------------------===//
// GC Manager
//===--------------------------------------------------------------------===//

class Off_GCManager : public GCManager {
public:
  Off_GCManager() {}

  virtual ~Off_GCManager() {};

  static Off_GCManager &GetInstance() {
    static Off_GCManager gcManager;
    return gcManager;
  }

  // Get status of whether GC thread is running or not
  virtual bool GetStatus() {return false;};

  virtual void StartGC() {};

  virtual void StopGC() {};

  // recycle old version
  virtual void RecycleOldTupleSlot(const oid_t __attribute__((unused))&table_id,
                                   const oid_t __attribute__((unused))&tile_group_id,
                                   const oid_t __attribute__((unused))&tuple_id,
                                   const size_t __attribute__((unused)) epoch_id) {}

  // recycle invalid version
  virtual void RecycleInvalidTupleSlot(const std::vector<ItemPointer> &invalid_tuples  __attribute__((unused))) {}

  virtual ItemPointer ReturnFreeSlot(const oid_t __attribute__((unused))&table_id) {
    return INVALID_ITEMPOINTER;
  }

  virtual void RegisterTable(oid_t table_id __attribute__((unused))){}

  virtual void DirectRecycleTuple(oid_t table_id __attribute__((unused)),
                                  ItemPointer garbage_tuple __attribute__((unused))) {};
};

}  // namespace gc
}  // namespace peloton
