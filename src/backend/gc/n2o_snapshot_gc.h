//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// n2o_snapshot_gc.h
//
// Identification: src/backend/gc/n2o_snapshot_gc.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <unordered_map>
#include <map>
#include <vector>
#include <backend/concurrency/snapshot_epoch_manager.h>

#include "backend/common/types.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "backend/gc/gc_manager.h"
#include "backend/storage/tile_group_header.h"
#include "libcuckoo/cuckoohash_map.hh"
#include "backend/gc/n2o_epoch_gc.h"

namespace peloton {
namespace gc {
  // An extra context for snapshot versions
  extern thread_local EpochGarbageContext *current_snapshot_epoch_garbage_context;

  class N2OSnapshotGCManager : public GCManager {
  public:
    N2OSnapshotGCManager(int thread_count)
      : is_running_(true),
        gc_thread_count_(thread_count),
        gc_threads_(thread_count),
        snapshot_gc_manager(thread_count, true),
        rw_gc_manager(thread_count, true) {
      StartGC();
    }

    ~N2OSnapshotGCManager() { StopGC(); }

    static N2OSnapshotGCManager& GetInstance(int thread_count = 1) {
      static N2OSnapshotGCManager gcManager(thread_count);
      return gcManager;
    }

    // Get status of whether GC thread is running or not
    virtual bool GetStatus() override { return this->is_running_; }

    virtual void StartGC() override {
      for (int i = 0; i < gc_thread_count_; ++i) {
        StartGC(i);
      }
    };

    virtual void StopGC() override {
      for (int i = 0; i < gc_thread_count_; ++i) {
        StopGC(i);
      }
    }

    virtual void RecycleOldTupleSlot(const oid_t &table_id UNUSED_ATTRIBUTE,
                                     const oid_t &tile_group_id UNUSED_ATTRIBUTE,
                                     const oid_t &tuple_id UNUSED_ATTRIBUTE,
                                     const size_t epoch_id UNUSED_ATTRIBUTE) override
                                     { PL_ASSERT(false); }

    virtual void RecycleInvalidTupleSlot(const std::vector<ItemPointer> &invalid_tuples) override {
      rw_gc_manager.RecycleInvalidTupleSlot(invalid_tuples);
    }

    virtual void DirectRecycleTuple(oid_t table_id, ItemPointer garbage_tuple) override {
      rw_gc_manager.DirectRecycleTuple(table_id, garbage_tuple);
    }

    void RecycleOldTupleSlot(storage::TileGroup *tg, const oid_t &tuple_id, const size_t &eid) {
      size_t snapshot_eid = concurrency::SnapshotEpochManager::GetNearestSnapshotEpochId(eid);
      cid_t snapshot_cid = concurrency::EpochManager::GetReadonlyCidFromEid(snapshot_eid);
      auto tg_header = tg->GetHeader();
      cid_t begin_cid = tg_header->GetBeginCommitId(tuple_id);
      cid_t end_cid = tg_header->GetEndCommitId(tuple_id);
      if (snapshot_cid >= begin_cid && snapshot_cid < end_cid) {
        RecycleSnapshotTupleSlot(tg->GetTableId(), tg->GetTileGroupId(), tuple_id);
      } else {
        RecycleRWOldTupleSlot(tg->GetTableId(), tg->GetTileGroupId(), tuple_id);
      }
    }

    virtual ItemPointer ReturnFreeSlot(const oid_t &table_id) override {
      ItemPointer free_itemptr = rw_gc_manager.ReturnFreeSlot(table_id);
      if (free_itemptr == INVALID_ITEMPOINTER) {
        free_itemptr = snapshot_gc_manager.ReturnFreeSlot(table_id);
      }
      return free_itemptr;
    }

    virtual void RegisterTable(oid_t table_id) override {
      snapshot_gc_manager.RegisterTable(table_id);
      rw_gc_manager.RegisterTable(table_id);
    }

    virtual void CreateGCContext(size_t eid) override {
      snapshot_gc_manager.CreateGCContext(eid, current_snapshot_epoch_garbage_context);
      // Use the nearest ro epoch id
      rw_gc_manager.CreateGCContext(concurrency::SnapshotEpochManager::GetNearestSnapshotEpochId(eid), current_epoch_garbage_context);
    }

    virtual void EndGCContext() override {}

  private:
    void StartGC(int thread_id) {
      this->is_running_ = true;
      gc_threads_[thread_id].reset(new std::thread(&N2OSnapshotGCManager::Running, this, thread_id));
    }

    void StopGC(int thread_id) {
      if (this->gc_threads_[thread_id] == nullptr) {
        return;;
      }
      this->is_running_ = false;
      this->gc_threads_[thread_id]->join();
      ClearGarbage(thread_id);
    }

    void ClearGarbage(int thread_id) {
      snapshot_gc_manager.ClearGarbage(thread_id);
      rw_gc_manager.ClearGarbage(thread_id);
    }

    void Running(int thread_id);

    void RecycleRWOldTupleSlot(const oid_t &table_id, const oid_t &tg_id, const oid_t &tuple_id) {
      rw_gc_manager.RecycleOldTupleSlot(table_id, tg_id, tuple_id, current_epoch_garbage_context);
    }

    void RecycleSnapshotTupleSlot(const oid_t &table_id, const oid_t &tg_id, const oid_t &tuple_id) {
      snapshot_gc_manager.RecycleOldTupleSlot(table_id, tg_id, tuple_id, current_snapshot_epoch_garbage_context);
    }


  private:
    //===--------------------------------------------------------------------===//
    // Data members
    //===--------------------------------------------------------------------===//
    volatile bool is_running_;
    const int gc_thread_count_;

    std::vector<std::unique_ptr<std::thread>> gc_threads_;
    N2OEpochGCManager snapshot_gc_manager;
    N2OEpochGCManager rw_gc_manager;
  };
}
}

