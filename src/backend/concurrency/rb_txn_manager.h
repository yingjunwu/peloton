//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// optimistic_rb_txn_manager.h
//
// Identification: src/backend/concurrency/optimistic_rb_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/rollback_segment.h"
#include "backend/storage/data_table.h"
#include "backend/index/rb_btree_index.h"

namespace peloton {

namespace concurrency {

extern thread_local storage::RollbackSegmentPool *current_segment_pool;
extern thread_local std::unordered_map<ItemPointer, void *> updated_index_entries;

//===--------------------------------------------------------------------===//
// optimistic concurrency control with rollback segment
//===--------------------------------------------------------------------===//

class RBTxnManager : public TransactionManager {
public:
  typedef char* RBSegType;

  RBTxnManager() {}

  virtual ~RBTxnManager() {}

  virtual VisibilityType IsVisible(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  virtual bool IsOwner(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id);

  virtual bool IsOwnable(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) = 0;

  virtual inline bool IsInserted(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) {
      PL_ASSERT(IsOwner(tile_group_header, tuple_id));
      return tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID;
  }

  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tile_group_id, const oid_t &tuple_id) = 0;

  virtual void YieldOwnership(const oid_t &tile_group_id,
    const oid_t &tuple_id);

  virtual bool PerformInsert(const ItemPointer &location UNUSED_ATTRIBUTE) { PL_ASSERT(false); return false; };

  virtual bool PerformInsert(const ItemPointer &location, void *item_ptr);

  // Get the read timestamp of the latest transaction on this thread, it is 
  // either the begin commit time of current transaction of the just committed
  // transaction.
  virtual cid_t GetLatestReadTimestamp() {
    return latest_read_ts;
  }

  /**
   * Deprecated interfaces
   */
  virtual bool PerformRead(const ItemPointer &location);

  virtual void PerformUpdate(const ItemPointer &old_location __attribute__((unused)),
                             const ItemPointer &new_location __attribute__((unused)),
                             UNUSED_ATTRIBUTE const bool is_blind_write = false) {
    PL_ASSERT(false);
  }

  virtual void PerformDelete(const ItemPointer &old_location  __attribute__((unused)),
                             const ItemPointer &new_location __attribute__((unused))) {
    PL_ASSERT(false);
  }

  virtual void PerformDelete(const ItemPointer &location);

  virtual void PerformUpdate(const ItemPointer &location  __attribute__((unused))) {
    PL_ASSERT(false);
  }

  // Add a new rollback segment to the tuple
  virtual void PerformUpdateWithRb(const ItemPointer &location, char *new_rb_seg);

  // Insert a version, basically maintain secondary index
  virtual bool RBInsertVersion(storage::DataTable *target_table,
    const ItemPointer &location, const storage::Tuple *tuple);

  // Rollback the master copy of a tuple to the status at the begin of the 
  // current transaction
  void RollbackTuple(std::shared_ptr<storage::TileGroup> tile_group,
                            const oid_t tuple_id);

  // Whe a txn commits, it needs to set an end timestamp to all RBSeg it has
  // created in order to make them invisible to future transactions
  virtual void InstallRollbackSegments(storage::TileGroupHeader *tile_group_header,
                                const oid_t tuple_id, const cid_t end_cid);

  /**
   * @brief Test if a reader with read timestamp @read_ts should follow on the
   * rb chain started from rb_seg
   */
  virtual inline bool IsRBVisible(char *rb_seg, cid_t read_ts) {
    if (rb_seg == nullptr) {
      return false;
    }

    cid_t rb_ts = storage::RollbackSegmentPool::GetTimeStamp(rb_seg);

    return read_ts < rb_ts;
  }

  // Return nullptr if the tuple is not activated to current txn.
  // Otherwise return the version that current tuple is activated
  virtual inline char* GetActivatedRB(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_slot_id) {
    cid_t txn_begin_cid = current_txn->GetBeginCommitId();
    cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_slot_id);

    PL_ASSERT(tuple_begin_cid != MAX_CID);
    // Owner can not call this function
    PL_ASSERT(IsOwner(tile_group_header, tuple_slot_id) == false);

    RBSegType rb_seg = GetRbSeg(tile_group_header, tuple_slot_id);
    char *prev_visible;
    bool master_activated = (txn_begin_cid >= tuple_begin_cid);

    if (master_activated)
      prev_visible = tile_group_header->GetReservedFieldRef(tuple_slot_id);
    else
      prev_visible = nullptr;

    while (IsRBVisible(rb_seg, txn_begin_cid)) {
      prev_visible = rb_seg;
      rb_seg = storage::RollbackSegmentPool::GetNextPtr(rb_seg);
    }

    return prev_visible;
  }

  virtual Result CommitTransaction() = 0;

  virtual Result AbortTransaction() = 0;

  virtual Transaction *BeginTransaction() = 0;

  virtual void EndTransaction() = 0;

  // Init reserved area of a tuple
  // delete_flag is used to mark that the transaction that owns the tuple
  // has deleted the tuple
  // next_seg_pointer (8 bytes) | sindex entry ptr |
  // | delete_flag (1 bytes)
  virtual void InitTupleReserved(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);
    SetRbSeg(tile_group_header, tuple_id, nullptr);
    SetSIndexPtr(tile_group_header, tuple_id, nullptr);
    ClearDeleteFlag(tile_group_header, tuple_id);
    *(reinterpret_cast<bool*>(reserved_area + DELETE_FLAG_OFFSET)) = false;
    new (reserved_area + LOCK_OFFSET) Spinlock();
  }

  // Get current segment pool of the transaction manager
  virtual inline storage::RollbackSegmentPool *GetSegmentPool() {return current_segment_pool;}

  virtual inline RBSegType GetRbSeg(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    LockTuple(tile_group_header, tuple_id);
    char **rb_seg_ptr = (char **)(tile_group_header->GetReservedFieldRef(tuple_id) + RB_SEG_OFFSET);
    if (*rb_seg_ptr != nullptr) {
      if (*(int *)(*rb_seg_ptr+16) == 0) {
        LOG_INFO("Reading an empty rollback segment");
      }
    }
    RBSegType result = *rb_seg_ptr;
    UnlockTuple(tile_group_header, tuple_id);
    return result;
  }

 protected:
  static const size_t RB_SEG_OFFSET = 0;
  static const size_t SINDEX_PTR_OFFSET = RB_SEG_OFFSET + sizeof(char *);
  static const size_t DELETE_FLAG_OFFSET = SINDEX_PTR_OFFSET + sizeof(char *);
  static const size_t LOCK_OFFSET = DELETE_FLAG_OFFSET + sizeof(char *);
  // TODO: add cooperative GC
  // The RB segment pool that is actively being used
  cuckoohash_map<cid_t, std::shared_ptr<storage::RollbackSegmentPool>> living_pools_;
  // The RB segment pool that has been marked as garbage
  cuckoohash_map<cid_t, std::shared_ptr<storage::RollbackSegmentPool>> garbage_pools_;

  inline void LockTuple(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                             LOCK_OFFSET);
    lock->Lock();
  }

  inline void UnlockTuple(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                             LOCK_OFFSET);
    lock->Unlock();
  }

  inline void SetRbSeg(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id, const RBSegType seg_ptr) {
    // Sanity check
    LockTuple(tile_group_header, tuple_id);
    if (seg_ptr != nullptr)
      PL_ASSERT(*(int *)(seg_ptr + 16) != 0);
    const char **rb_seg_ptr = (const char **)(tile_group_header->GetReservedFieldRef(tuple_id) + RB_SEG_OFFSET);
    *rb_seg_ptr = seg_ptr;
    UnlockTuple(tile_group_header, tuple_id);
  }

  inline void SetSIndexPtr(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id,
                          void *ptr) {
    void **index_ptr = (void **)(tile_group_header->GetReservedFieldRef(tuple_id) + SINDEX_PTR_OFFSET);
    *index_ptr = ptr;
  }

  inline void *GetSIndexPtr(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    void **index_ptr = (void **)(tile_group_header->GetReservedFieldRef(tuple_id) + SINDEX_PTR_OFFSET);
    return *index_ptr;
  }

  inline bool GetDeleteFlag(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    return *(reinterpret_cast<bool*>(tile_group_header->GetReservedFieldRef(tuple_id) + DELETE_FLAG_OFFSET));
  }

  inline void SetDeleteFlag(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    *(reinterpret_cast<bool*>(tile_group_header->GetReservedFieldRef(tuple_id) + DELETE_FLAG_OFFSET)) = true;
  }

  inline void ClearDeleteFlag(const storage::TileGroupHeader *tile_group_header, const oid_t tuple_id) {
    *(reinterpret_cast<bool*>(tile_group_header->GetReservedFieldRef(tuple_id) + DELETE_FLAG_OFFSET)) = false;
  }

  virtual void InstallSecondaryIndex(cid_t end_commit_id);
};
}
}