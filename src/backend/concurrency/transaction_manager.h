//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <unordered_map>
#include <list>

#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/epoch_manager_factory.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/catalog/manager.h"
#include "backend/expression/container_tuple.h"
#include "backend/storage/tuple.h"
#include "backend/gc/gc_manager_factory.h"
#include "backend/planner/project_info.h"

#include "backend/logging/log_manager.h"

#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {

namespace concurrency {

extern thread_local Transaction *current_txn;
extern thread_local cid_t latest_read_ts;

class TransactionManager {
 public:
  TransactionManager() {
    next_txn_id_ = ATOMIC_VAR_INIT(START_TXN_ID);
    next_cid_ = ATOMIC_VAR_INIT(START_CID);
    read_abort_ = 0;
    acquire_owner_abort_ = 0;
    no_space_abort_ = 0;
    cannot_own_abort_ = 0;

    rescue_not_visible_ = 0;
    rescue_not_in_range_ = 0;
    rescue_read_abort_ = 0;
    rescue_not_possible_ = 0;
    rescue_successful_ = 0;
  }

  virtual ~TransactionManager() {
     printf("read_abort_ = %d, acquire_owner_abort_ = %d, no_space_abort_ = %d, cannot_own_abort_ = %d\n", read_abort_.load(), acquire_owner_abort_.load(), no_space_abort_.load(), cannot_own_abort_.load());

     printf("rescue_not_visible_ = %d, rescue_not_in_range_ = %d, rescue_read_abort_ = %d, rescue_not_possible_ = %d, rescue_successful_ = %d\n", rescue_not_visible_.load(), rescue_not_in_range_.load(), rescue_read_abort_.load(), rescue_not_possible_.load(), rescue_successful_.load());
  }

  txn_id_t GetNextTransactionId() { return next_txn_id_++; }

  // cid_t GetNextCommitId() { return next_cid_++; }

  bool IsOccupied(const void *position_ptr);

  virtual VisibilityType IsVisible(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  virtual bool IsOwner(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id) = 0;


  virtual bool IsOwnable(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tile_group_id, const oid_t &tuple_id) = 0;

  // This method is used by executor to yield ownership after the acquired ownership.
  virtual void YieldOwnership(const oid_t &tile_group_id, const oid_t &tuple_id) = 0;

  virtual bool PerformInsert(const ItemPointer &location) = 0;

  virtual bool PerformInsert(const ItemPointer &location UNUSED_ATTRIBUTE, ItemPointer *itemptr_ptr UNUSED_ATTRIBUTE) {
    LOG_ERROR("never call this function!");
    return false;
  }


  // Init the tile group header reserved field for the recovery usage
  virtual void
  InitInsertedTupleForRecovery(storage::TileGroupHeader *tg_header, oid_t tuple_slot, ItemPointer *itemptr_ptr) = 0;

  virtual bool PerformRead(const ItemPointer &location) = 0;

  virtual void PerformUpdate(const ItemPointer &old_location,
                             const ItemPointer &new_location, const bool is_blind_write = false) = 0;

  virtual void PerformDelete(const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformUpdate(const ItemPointer &location) = 0;

  virtual void PerformDelete(const ItemPointer &location) = 0;

  /*
   * Write a virtual function to push deleted and verified (acc to optimistic
   * concurrency control) tuples into possibly free from all underlying
   * concurrency implementations of transactions.
   */

  void RecycleInvalidTupleSlot(const std::vector<ItemPointer> &invalid_tuples) {
    gc::GCManagerFactory::GetInstance().RecycleInvalidTupleSlot(invalid_tuples);
  }

  void RecycleOldTupleSlot(const oid_t &tile_group_id, const oid_t &tuple_id,
                           const size_t tuple_eid) {
    auto tile_group =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id);

    if(gc::GCManagerFactory::GetGCType() == GC_TYPE_VACUUM
    || gc::GCManagerFactory::GetGCType() == GC_TYPE_N2O
    || gc::GCManagerFactory::GetGCType() == GC_TYPE_N2O_TXN
    || gc::GCManagerFactory::GetGCType() == GC_TYPE_N2O_EPOCH) {
  
      LOG_TRACE("recycle old tuple slot: %u, %u", tile_group_id, tuple_id);

      auto& gc_instance = gc::GCManagerFactory::GetInstance();

      gc_instance.RecycleOldTupleSlot(
        tile_group->GetTableId(), tile_group_id, tuple_id, tuple_eid);
    } else if (gc::GCManagerFactory::GetGCType() == GC_TYPE_N2O_SNAPSHOT) {
      PL_ASSERT(EpochManagerFactory::GetType() == EPOCH_SNAPSHOT
                || EpochManagerFactory::GetType() == EPOCH_LOCALIZED_SNAPSHOT);
      reinterpret_cast<gc::N2OSnapshotGCManager*>(&gc::GCManagerFactory::GetInstance())
                         ->RecycleOldTupleSlot(tile_group.get(), tuple_id, tuple_eid);
    }
  }


  // Txn manager may store related information in TileGroupHeader, so when
  // TileGroup is dropped, txn manager might need to be notified
  virtual void DroppingTileGroup(const oid_t &tile_group_id
                                 __attribute__((unused))) {
    return;
  }

  void SetTransactionResult(const Result result) {
    current_txn->SetResult(result);
  }

  //for use by recovery
  void SetNextCid(cid_t cid) { next_cid_ = cid; }

  Transaction *BeginReadonlyTransaction() {
    txn_id_t txn_id = READONLY_TXN_ID;
    auto &epoch_manager = EpochManagerFactory::GetInstance();
    cid_t begin_cid = epoch_manager.EnterReadOnlyEpoch();
    size_t eid = EpochManager::GetEidFromCid(begin_cid);

    Transaction *txn = new Transaction(txn_id, true, begin_cid);

    latest_read_ts = begin_cid;
    txn->SetEpochId(eid);

    current_txn = txn;
    return txn;
  }

  virtual Transaction *BeginTransaction() = 0;

  Result EndReadonlyTransaction() {
    EpochManagerFactory::GetInstance().ExitReadOnlyEpoch(current_txn->GetEpochId());

    delete current_txn;
    current_txn = nullptr;

    return RESULT_SUCCESS;
  }

  virtual void EndTransaction() = 0;

  virtual Result CommitTransaction() = 0;

  virtual Result AbortTransaction() = 0;

  void ResetStates() {
    next_txn_id_ = START_TXN_ID;
    next_cid_ = START_CID;
  }

  virtual ItemPointer *GetHeadPtr(
    const storage::TileGroupHeader *const tile_group_header UNUSED_ATTRIBUTE,
    const oid_t tuple_id UNUSED_ATTRIBUTE) {
    LOG_ERROR("never call this function!");
    return nullptr;
  }


  inline void AddOneReadAbort() {
     ++read_abort_;
  }

  inline void AddOneAcquireOwnerAbort() {
     ++acquire_owner_abort_;
  }

  inline void AddOneNoSpaceAbort() {
     ++no_space_abort_;
  }

  inline void AddOneCannotOwnAbort() {
     ++cannot_own_abort_;
  }

  inline void AddRescueNotVisible() {
     ++rescue_not_visible_;
  }

  inline void AddRescueNotInRange() {
     ++rescue_not_in_range_;
  }

  inline void AddRescueReadAbort() {
     ++rescue_read_abort_;
  }

  inline void AddRescueNotPossible() {
     ++rescue_not_possible_;
  }

  inline void AddRescueSuccessful() {
    ++rescue_successful_;
  }

 private:
  std::atomic<txn_id_t> next_txn_id_;
  std::atomic<cid_t> next_cid_;

   std::atomic<int> read_abort_;
   std::atomic<int> acquire_owner_abort_;
   std::atomic<int> no_space_abort_;
   std::atomic<int> cannot_own_abort_;

   std::atomic<int> rescue_not_visible_;
   std::atomic<int> rescue_not_in_range_;
   std::atomic<int> rescue_read_abort_;
   std::atomic<int> rescue_not_possible_;
   std::atomic<int> rescue_successful_;
};
}  // End storage namespace
}  // End peloton namespace
