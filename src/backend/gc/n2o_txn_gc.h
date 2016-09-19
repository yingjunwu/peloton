//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// n2o_gc.h
//
// Identification: src/backend/gc/n2o_gc.h
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
    struct GarbageContext {
      std::vector<TupleMetadata> garbages;
      size_t epoch_id;

      GarbageContext() : garbages(), epoch_id(0) {}
    };

    extern thread_local GarbageContext *current_garbage_context;

    class N2OTxn_GCManager : public GCManager {
    public:
      N2OTxn_GCManager(int thread_count)
        : is_running_(true),
          gc_thread_count_(thread_count),
          gc_threads_(thread_count),
          unlink_queues_(),
          local_unlink_queues_(),
          reclaim_maps_(thread_count) {

        unlink_queues_.reserve(thread_count);
        for (int i = 0; i < gc_thread_count_; ++i) {
          std::shared_ptr<LockfreeQueue<std::shared_ptr<GarbageContext>>> unlink_queue(
            new LockfreeQueue<std::shared_ptr<GarbageContext>>(MAX_QUEUE_LENGTH)
          );
          unlink_queues_.push_back(unlink_queue);
          local_unlink_queues_.emplace_back();
        }
        StartGC();
      }

      ~N2OTxn_GCManager() { StopGC(); }

      static N2OTxn_GCManager& GetInstance(int thread_count = 1) {
        static N2OTxn_GCManager gcManager(thread_count);
        return gcManager;
      }

      // Get status of whether GC thread is running or not
      virtual bool GetStatus() { return this->is_running_; }

      virtual void StartGC() {
        for (int i = 0; i < gc_thread_count_; ++i) {
          StartGC(i);
        }
      };

      virtual void StopGC() {
        for (int i = 0; i < gc_thread_count_; ++i) {
          StopGC(i);
        }
      }

      virtual void RecycleOldTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                                       const oid_t &tuple_id, const size_t epoch_id);

      virtual void RecycleInvalidTupleSlot(const oid_t &table_id __attribute__((unused)),
                                           const oid_t &tile_group_id __attribute__((unused)),
                                           const oid_t &tuple_id __attribute__((unused))) {
        assert(false);
      }

      virtual ItemPointer ReturnFreeSlot(const oid_t &table_id);

      virtual void RegisterTable(oid_t table_id) {
        // Insert a new entry for the table
        if (recycle_queue_map_.find(table_id) == recycle_queue_map_.end()) {
          LOG_TRACE("register table %d to garbage collector", (int)table_id);
          std::shared_ptr<LockfreeQueue<TupleMetadata>> recycle_queue(new LockfreeQueue<TupleMetadata>(MAX_QUEUE_LENGTH));
          recycle_queue_map_[table_id] = recycle_queue;
        }
      }

      virtual void CreateGCContext();

      virtual void EndGCContext(size_t eid);

    private:
      void StartGC(int thread_id);

      void StopGC(int thread_id);

      inline unsigned int HashToThread(const size_t eid) {
        return (unsigned int)eid % gc_thread_count_;
      }

      void ClearGarbage(int thread_id);

      void Running(int thread_id);

      void Reclaim(int thread_id, const size_t max_eid);

      void Unlink(int thread_id, const size_t max_eid);

      void AddToRecycleMap(std::shared_ptr<GarbageContext> gc_ctx);

      bool ResetTuple(const TupleMetadata &);

      void DeleteTupleFromIndexes(const TupleMetadata &tuple_metadata);

    private:
      //===--------------------------------------------------------------------===//
      // Data members
      //===--------------------------------------------------------------------===//
      volatile bool is_running_;

      const int gc_thread_count_;

      std::vector<std::unique_ptr<std::thread>> gc_threads_;

      std::vector<std::shared_ptr<peloton::LockfreeQueue<std::shared_ptr<GarbageContext>>>> unlink_queues_;
      std::vector<std::list<std::shared_ptr<GarbageContext>>> local_unlink_queues_;

      // Map of actual grabage.
      // The key is the timestamp when the garbage is identified, value is the
      // metadata of the garbage.
      // TODO: use shared pointer to reduce memory copy
      std::vector<std::multimap<size_t, std::shared_ptr<GarbageContext>>> reclaim_maps_;

      // TODO: use shared pointer to reduce memory copy
      // table_id -> queue
      //cuckoohash_map<oid_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> recycle_queue_map_;
      std::unordered_map<oid_t, std::shared_ptr<peloton::LockfreeQueue<TupleMetadata>>> recycle_queue_map_;
    };
  }
}

