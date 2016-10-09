//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// vacuum_gc.cpp
//
// Identification: src/backend/gc/vacuum_gc.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/gc/n2o_txn_gc.h"
#include "backend/concurrency/transaction_manager_factory.h"
namespace peloton {
  namespace gc {
    thread_local GarbageContext *current_garbage_context = nullptr;

    void N2OTxn_GCManager::CreateGCContext(size_t eid) {
      current_garbage_context = new GarbageContext();
      current_garbage_context->epoch_id = eid;
    }

    // WARNING: This function must be called before the current_txn is distructed
    void N2OTxn_GCManager::EndGCContext() {
      // Add the garbage context to the lockfree queue
      if (current_garbage_context->garbages.empty() == true) {
        delete current_garbage_context;
      } else {
        std::shared_ptr<GarbageContext> gc_context(current_garbage_context);
        unlink_queues_[HashToThread(gc_context->epoch_id)]->Enqueue(gc_context);
      }
      // Reset the GC context (not necessary...)
      current_garbage_context = nullptr;
    }

    void N2OTxn_GCManager::StartGC(int thread_id) {
      LOG_TRACE("Starting GC");
      this->is_running_ = true;
      gc_threads_[thread_id].reset(new std::thread(&N2OTxn_GCManager::Running, this, thread_id));
    }

    void N2OTxn_GCManager::StopGC(int thread_id) {
      LOG_TRACE("Stopping GC");
      this->is_running_ = false;
      this->gc_threads_[thread_id]->join();
      ClearGarbage(thread_id);
    }

    bool N2OTxn_GCManager::ResetTuple(const TupleMetadata &tuple_metadata) {
      auto &manager = catalog::Manager::GetInstance();
      auto tile_group =
        manager.GetTileGroup(tuple_metadata.tile_group_id);

      // During the resetting, a table may deconstruct because of the DROP TABLE request
      if (tile_group == nullptr) {
        LOG_TRACE("Garbage tuple(%u, %u) in table %u no longer exists",
                  tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id,
                  tuple_metadata.table_id);
        return false;
      }

      auto tile_group_header = tile_group->GetHeader();

      // Reset the header
      tile_group_header->SetTransactionId(tuple_metadata.tuple_slot_id,
                                          INVALID_TXN_ID);
      tile_group_header->SetBeginCommitId(tuple_metadata.tuple_slot_id, MAX_CID);
      tile_group_header->SetEndCommitId(tuple_metadata.tuple_slot_id, MAX_CID);
      tile_group_header->SetPrevItemPointer(tuple_metadata.tuple_slot_id,
                                            INVALID_ITEMPOINTER);
      tile_group_header->SetNextItemPointer(tuple_metadata.tuple_slot_id,
                                            INVALID_ITEMPOINTER);
      tile_group_header->SetNextSnapshotItemPointer(tuple_metadata.tuple_slot_id, INVALID_ITEMPOINTER);

      std::memset(
        tile_group_header->GetReservedFieldRef(tuple_metadata.tuple_slot_id), 0,
        storage::TileGroupHeader::GetReservedSize());
      LOG_TRACE("Garbage tuple(%u, %u) in table %u is reset",
                tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id,
                tuple_metadata.table_id);
      return true;
    }

// Multiple GC thread share the same recycle map
    void N2OTxn_GCManager::AddGarbageCtxToRecycleMap(std::shared_ptr<GarbageContext> gc_ctx) {
      // If the tuple being reset no longer exists, just skip it
      for (auto &tuple_metadata : gc_ctx->garbages) {
        AddTupleToRecycleMap(tuple_metadata);
      }
    }

    void N2OTxn_GCManager::AddTupleToRecycleMap(TupleMetadata &tuple_metadata) {
        if (ResetTuple(tuple_metadata) == false) {
          return;
        }
        // if the entry for table_id exists.
        assert(recycle_queue_map_.find(tuple_metadata.table_id) != recycle_queue_map_.end());
        recycle_queue_map_[tuple_metadata.table_id]->Enqueue(tuple_metadata);
    }

    void N2OTxn_GCManager::Running(int thread_id) {
      // Check if we can move anything from the possibly free list to the free list.

      std::this_thread::sleep_for(
        std::chrono::milliseconds(GC_PERIOD_MILLISECONDS));
      while (true) {
        size_t max_eid = concurrency::EpochManagerFactory::GetInstance().GetMaxDeadEid();

        assert(max_eid != MAX_EPOCH_ID);

        Reclaim(thread_id, max_eid);

        Unlink(thread_id, max_eid);

        if (is_running_ == false) {
          return;
        }
      }
    }

// executed by a single thread. so no synchronization is required.
    void N2OTxn_GCManager::Reclaim(int thread_id, const size_t max_eid) {
      int gc_counter = 0;

      // we delete garbage in the free list
      auto garbage_ctx = reclaim_maps_[thread_id].begin();
      while (garbage_ctx != reclaim_maps_[thread_id].end()) {
        const size_t garbage_eid = garbage_ctx->first;
        auto garbage_context = garbage_ctx->second;

        // if the timestamp of the garbage is older than the current max_cid,
        // recycle it
        if (garbage_eid < max_eid) {
          AddGarbageCtxToRecycleMap(garbage_context);

          // Remove from the original map
          garbage_ctx = reclaim_maps_[thread_id].erase(garbage_ctx);
          gc_counter++;
        } else {
          // Early break since we use an ordered map
          break;
        }
      }
      LOG_TRACE("Marked %d txn contexts as recycled", gc_counter);
    }

    void N2OTxn_GCManager::Unlink(int thread_id, const size_t max_eid) {
      int tuple_counter = 0;

      // we check if any possible garbage is actually garbage
      // every time we garbage collect at most MAX_ATTEMPT_COUNT tuples.

      std::vector<std::shared_ptr<GarbageContext>> garbages;

      // First iterate the local unlink queue
      local_unlink_queues_[thread_id].remove_if(
        [this, &garbages, &tuple_counter, max_eid](const std::shared_ptr<GarbageContext>& g) -> bool {
          bool res = g->epoch_id  < max_eid;
          if (res) {
            for (auto t : g->garbages) {
              DeleteTupleFromIndexes(t);
            }
            garbages.push_back(g);
            tuple_counter++;
          }
          return res;
        }
      );

      for (size_t i = 0; i < MAX_ATTEMPT_COUNT; ++i) {

        std::shared_ptr<GarbageContext> garbage_ctx;
        // if there's no more tuples in the queue, then break.
        if (unlink_queues_[thread_id]->Dequeue(garbage_ctx) == false) {
          break;
        }

        if (garbage_ctx->epoch_id < max_eid) {
          // Now that we know we need to recycle tuple, we need to delete all
          // tuples from the indexes to which it belongs as well.
          for (auto t : garbage_ctx->garbages) {
            DeleteTupleFromIndexes(t);
          }
          // Add to the garbage map
          // reclaim_map_.insert(std::make_pair(getnextcid(), tuple_metadata));
          garbages.push_back(garbage_ctx);
          tuple_counter++;

        } else {
          // if a tuple cannot be reclaimed, then add it back to the list.
          local_unlink_queues_[thread_id].push_back(garbage_ctx);
        }
      }  // end for

      size_t current_eid = concurrency::EpochManagerFactory::GetInstance().GetCurrentEpoch();
      for(auto& item : garbages){
          reclaim_maps_[thread_id].insert(std::make_pair(current_eid, item));
      }
      LOG_TRACE("Marked %d tuples as garbage", tuple_counter);
    }

// called by transaction manager.
    void N2OTxn_GCManager::RecycleOldTupleSlot(const oid_t &table_id,
                                            const oid_t &tile_group_id,
                                            const oid_t &tuple_id,
                                            const size_t epoch_id UNUSED_ATTRIBUTE) {

      TupleMetadata tuple_metadata;
      tuple_metadata.table_id = table_id;
      tuple_metadata.tile_group_id = tile_group_id;
      tuple_metadata.tuple_slot_id = tuple_id;

      current_garbage_context->garbages.push_back(tuple_metadata);
      LOG_TRACE("Marked tuple(%u, %u) in table %u as possible garbage",
                tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id,
                tuple_metadata.table_id);
    }

    void N2OTxn_GCManager::RecycleInvalidTupleSlot(const std::vector<ItemPointer> &invalid_tuples) {
      if (invalid_tuples.empty() == true) {
        return;
      }

      size_t cur_eid = concurrency::EpochManagerFactory::GetInstance().GetCurrentEpoch();
      for (ItemPointer itemptr : invalid_tuples) {
        oid_t tg_id = itemptr.block;
        oid_t tuple_id = itemptr.offset;
        auto tg = catalog::Manager::GetInstance().GetTileGroup(tg_id);
        current_garbage_context->garbages.emplace_back(tg->GetTableId(), tg_id, tuple_id);
      }
      current_garbage_context->epoch_id = cur_eid;
    }

    void N2OTxn_GCManager::DirectRecycleTuple(oid_t table_id, ItemPointer garbage_tuple) {
      TupleMetadata tuple_meta = TupleMetadata(table_id, garbage_tuple.block, garbage_tuple.offset);
      AddTupleToRecycleMap(tuple_meta);
    }

// this function returns a free tuple slot, if one exists
// called by data_table.
    ItemPointer N2OTxn_GCManager::ReturnFreeSlot(const oid_t &table_id) {
      // return INVALID_ITEMPOINTER;
      assert(recycle_queue_map_.count(table_id) != 0);
      TupleMetadata tuple_metadata;
      auto recycle_queue = recycle_queue_map_[table_id];


      //std::shared_ptr<LockfreeQueue<TupleMetadata>> recycle_queue;
      // if there exists recycle_queue
      //if (recycle_queue_map_.find(table_id, recycle_queue) == true) {
      //TupleMetadata tuple_metadata;
      if (recycle_queue->Dequeue(tuple_metadata) == true) {
        LOG_TRACE("Reuse tuple(%u, %u) in table %u", tuple_metadata.tile_group_id,
                  tuple_metadata.tuple_slot_id, table_id);
        return ItemPointer(tuple_metadata.tile_group_id,
                           tuple_metadata.tuple_slot_id);
      }
      //}
      return INVALID_ITEMPOINTER;
    }


    void N2OTxn_GCManager::ClearGarbage(int thread_id) {
      while(!unlink_queues_[thread_id]->IsEmpty() || !local_unlink_queues_[thread_id].empty()) {
        Unlink(thread_id, MAX_EPOCH_ID);
      }

      while(reclaim_maps_[thread_id].size() != 0) {
        Reclaim(thread_id, MAX_EPOCH_ID);
      }

      return;
    }

// delete a tuple from all its indexes it belongs to.
void N2OTxn_GCManager::DeleteTupleFromIndexes(const TupleMetadata &tuple_metadata) {
  LOG_TRACE("Deleting index for tuple(%u, %u)", tuple_metadata.tile_group_id,
            tuple_metadata.tuple_slot_id);
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tuple_metadata.tile_group_id);

  assert(tile_group != nullptr);
  storage::DataTable *table =
    dynamic_cast<storage::DataTable *>(tile_group->GetAbstractTable());
  assert(table != nullptr);
  if (table->GetIndexCount() == 1) {
    // in this case, the table only has primary index. do nothing.
    return;
  }

  // construct the expired version.
  std::unique_ptr<storage::Tuple> expired_tuple(
    new storage::Tuple(table->GetSchema(), true));
  tile_group->CopyTuple(tuple_metadata.tuple_slot_id, expired_tuple.get());

  // unlink the version from all the indexes.
  for (size_t idx = 0; idx < table->GetIndexCount(); ++idx) {
    auto index = table->GetIndex(idx);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    // build key.
    std::unique_ptr<storage::Tuple> key(
      new storage::Tuple(index_schema, true));
    key->SetFromTuple(expired_tuple.get(), indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
      case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY: {
        LOG_TRACE("Deleting primary index");

        // Do nothing for new to old version chain here
        if (concurrency::TransactionManagerFactory::IsN2O() == true) {
          continue;
        }

        // find next version the index bucket should point to.
        auto tile_group_header = tile_group->GetHeader();
        ItemPointer next_version =
          tile_group_header->GetNextItemPointer(tuple_metadata.tuple_slot_id);

        auto next_tile_group_header = manager.GetTileGroup(next_version.block)->GetHeader();
        auto next_begin_cid = next_tile_group_header->GetBeginCommitId(next_version.offset);
        assert(next_version.IsNull() == false);

        assert(next_begin_cid != MAX_CID);

        std::vector<ItemPointer *> item_pointer_containers;
        // find the bucket.
        index->ScanKey(key.get(), item_pointer_containers);
        // as this is primary key, there should be exactly one entry.
        assert(item_pointer_containers.size() == 1);


        auto index_version = *item_pointer_containers[0];
        auto index_tile_group_header = manager.GetTileGroup(index_version.block)->GetHeader();
        auto index_begin_cid = index_tile_group_header->GetBeginCommitId(index_version.offset);

        // if next_version is newer than index's version
        // update index
        if(index_begin_cid < next_begin_cid) {
          AtomicUpdateItemPointer(item_pointer_containers[0], next_version);
        }

      } break;
      default: {
        LOG_TRACE("Deleting other index");
        index->DeleteEntry(key.get(),
                           ItemPointer(tuple_metadata.tile_group_id,
                                       tuple_metadata.tuple_slot_id));
      }
    }
  }
}


  }  // namespace gc
}  // namespace peloton
