//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager_factory.h
//
// Identification: src/backend/concurrency/gc_manager_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/gc/off_gc.h"
#include "backend/gc/n2o_txn_gc.h"
#include "backend/gc/n2o_epoch_gc.h"
#include "backend/gc/n2o_snapshot_gc.h"

#include "backend/concurrency/epoch_manager.h"

namespace peloton {
namespace gc {

class GCManagerFactory {
 public:
  static GCManager &GetInstance() {
    switch (gc_type_) {
      case GC_TYPE_N2O_TXN:
        return N2OTxn_GCManager::GetInstance(gc_thread_count_);
      case GC_TYPE_N2O_EPOCH:
        return N2OEpochGCManager::GetInstance(gc_thread_count_);
      case GC_TYPE_N2O_SNAPSHOT:
        return N2OSnapshotGCManager::GetInstance(gc_thread_count_);
      case GC_TYPE_OFF:
        return Off_GCManager::GetInstance();
      default:
        return Off_GCManager::GetInstance();
    }
  }

  static void Configure( GCType gc_type, int thread_count = default_gc_thread_count_) {
    if (gc_type != GC_TYPE_OFF && gc_type != GC_TYPE_N2O_TXN && gc_type != GC_TYPE_N2O_EPOCH) {
      // Enforce the default
      gc_type = GC_TYPE_OFF;
    }
    gc_type_ = gc_type;
    gc_thread_count_ = thread_count;
  }

  static GCType GetGCType() { return gc_type_; }

 private:

  // GC type
  static GCType gc_type_;

  // GC thread count
  static int gc_thread_count_;
  const static int default_gc_thread_count_ = 1;
};
} // namespace gc
} // namespace peloton
