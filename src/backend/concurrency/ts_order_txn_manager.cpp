//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ts_order_txn_manager.cpp
//
// Identification: src/backend/concurrency/ts_order_txn_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "ts_order_txn_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_header.h"

namespace peloton {
namespace concurrency {

Spinlock *TsOrderTxnManager::GetSpinlockField(
    const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id) {
  return (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) + LOCK_OFFSET);
}

cid_t TsOrderTxnManager::GetLastReaderCid(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  return *(cid_t*)(tile_group_header->GetReservedFieldRef(tuple_id) + LAST_READER_OFFSET);
}

// Set the last reader of a tuple, won't change if the tuple has a bigger last
// reader id, return false if the tuple is owned by others.
bool TsOrderTxnManager::SetLastReaderCid(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {

  assert(IsOwner(tile_group_header, tuple_id) == false);

  cid_t *ts_ptr = (cid_t*)(tile_group_header->GetReservedFieldRef(tuple_id) + LAST_READER_OFFSET);
  
  cid_t current_cid = current_txn->GetBeginCommitId();

  GetSpinlockField(tile_group_header, tuple_id)->Lock();
  
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  
  if(tuple_txn_id != INITIAL_TXN_ID) {
    GetSpinlockField(tile_group_header, tuple_id)->Unlock();
    return false;
  } else {
    if (*ts_ptr < current_cid) {
      *ts_ptr = current_cid;
    }
    GetSpinlockField(tile_group_header, tuple_id)->Unlock();
    return true;
  }
}

TsOrderTxnManager &TsOrderTxnManager::GetInstance() {
  static TsOrderTxnManager txn_manager;
  return txn_manager;
}

// Visibility check
VisibilityType TsOrderTxnManager::IsVisible(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

  bool own = (current_txn->GetTransactionId() == tuple_txn_id);
  bool activated = (current_txn->GetBeginCommitId() >= tuple_begin_cid);
  bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);

  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    if (activated && !invalidated) {
      // deleted tuple
      return VISIBILITY_DELETED;
    } else {
      // aborted tuple
      return VISIBILITY_INVISIBLE;
    }
  }

  // there are exactly two versions that can be owned by a transaction.
  // unless it is an insertion.
  if (own == true) {
    if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
      assert(tuple_end_cid == MAX_CID);
      // the only version that is visible is the newly inserted/updated one.
      return VISIBILITY_OK;
    } else if (tuple_end_cid == INVALID_CID) {
      // tuple being deleted by current txn
      return VISIBILITY_DELETED;
    } else {
      // old version of the tuple that is being updated by current txn
      return VISIBILITY_INVISIBLE;
    }
  } else {
    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // in this protocol, we do not allow cascading abort. so never read an
        // uncommitted version.
        return VISIBILITY_INVISIBLE;
      } else {
        // the older version may be visible.
        if (activated && !invalidated) {
          return VISIBILITY_OK;
        } else {
          return VISIBILITY_INVISIBLE;
        }
      }
    } else {
      // if the tuple is not owned by any transaction.
      if (activated && !invalidated) {
        return VISIBILITY_OK;
      } else {
        return VISIBILITY_INVISIBLE;
      }
    }
  }
}

// check whether the current transaction owns the tuple.
// this function is called by update/delete executors.
bool TsOrderTxnManager::IsOwner(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  return tuple_txn_id == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// this function is called by update/delete executors.
bool TsOrderTxnManager::IsOwnable(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  // As long as the tuple is valid, and it is visible to the current txn
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid > current_txn->GetBeginCommitId();
}

bool TsOrderTxnManager::AcquireOwnership(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tile_group_id __attribute__((unused)), const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();

  GetSpinlockField(tile_group_header, tuple_id)->Lock();
  // change timestamp
  cid_t last_reader_cid = GetLastReaderCid(tile_group_header, tuple_id);

  // If read by others after the current txn begins, can't acquire ownership
  if (last_reader_cid > current_txn->GetBeginCommitId()) {
    GetSpinlockField(tile_group_header, tuple_id)->Unlock();
    SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  } else {
    if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {    
      GetSpinlockField(tile_group_header, tuple_id)->Unlock();
      SetTransactionResult(Result::RESULT_FAILURE);
      return false;
    } else {
      GetSpinlockField(tile_group_header, tuple_id)->Unlock();
      return true;
    }
  }
}

// release write lock on a tuple.
// one example usage of this method is when a tuple is acquired, but operation
// (insert,update,delete) can't proceed, the executor needs to yield the 
// ownership before return false to upper layer.
// It should not be called if the tuple is in the write set as commit and abort
// will release the write lock anyway.
void TsOrderTxnManager::YieldOwnership(const oid_t &tile_group_id,
  const oid_t &tuple_id) {

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  assert(IsOwner(tile_group_header, tuple_id));
  tile_group_header->SetTransactionId(tuple_id, INITIAL_TXN_ID);
}

bool TsOrderTxnManager::PerformRead(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();

  //auto last_reader_cid = GetLastReaderCid(tile_group_header, tuple_id);
  if (IsOwner(tile_group_header, tuple_id) == true) {
    // it is possible to be a blind write.
    assert(GetLastReaderCid(tile_group_header, tuple_id) <= current_txn->GetBeginCommitId());
    return true;
  }

  if (SetLastReaderCid(tile_group_header, tuple_id) == true) {
    current_txn->RecordRead(location);
    return true;
  } else {
    return false;
  }
}

bool TsOrderTxnManager::PerformInsert(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // Set MVCC info
  assert(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetTransactionId(tuple_id, transaction_id);

  // no need to set next item pointer.

  // Add the new tuple into the insert set
  current_txn->RecordInsert(location);
  
  InitTupleReserved(tile_group_header, tuple_id);

  //SetLastReaderCid(tile_group_header, location.offset, current_txn->GetBeginCommitId());
  return true;
}

void TsOrderTxnManager::PerformUpdate(const ItemPointer &old_location,
                                      const ItemPointer &new_location, UNUSED_ATTRIBUTE const bool is_blind_write) {
  LOG_TRACE("Performing Write %u %u", old_location.block, old_location.offset);


  auto tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(old_location.block)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  assert(GetLastReaderCid(tile_group_header, old_location.offset) == current_txn->GetBeginCommitId());

  auto transaction_id = current_txn->GetTransactionId();
  // if we can perform update, then we must have already locked the older
  // version.
  assert(tile_group_header->GetTransactionId(old_location.offset) ==
         transaction_id);
  assert(new_tile_group_header->GetTransactionId(new_location.offset) ==
         INVALID_TXN_ID);
  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
         MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

  // Set double linked list
  auto old_next = tile_group_header->GetNextItemPointer(old_location.offset);

  if (old_next.IsNull() == false){
    auto old_next_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(old_next.block)->GetHeader();

    // its fine to set prev item pointer.
    // anyone that wants to use the prev item pointer must already hold the lock on the older version.
    old_next_tile_group_header->SetPrevItemPointer(old_next.offset, new_location);
  }

  tile_group_header->SetNextItemPointer(old_location.offset, new_location);

  // I think there's no need to set this fence,
  // as we have already acquired the lock on the older version.
  // every transaction that wants to update this version must acquire the lock on the older version first.
  // COMPILER_MEMORY_FENCE;

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_next);
  
  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  InitTupleReserved(new_tile_group_header, new_location.offset);
  
  // Add the old tuple into the update set
  current_txn->RecordUpdate(old_location);
}

void TsOrderTxnManager::PerformUpdate(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) ==
         current_txn->GetTransactionId());
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // Add the old tuple into the update set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // update an inserted version
    current_txn->RecordUpdate(old_location);
  }
}

void TsOrderTxnManager::PerformDelete(const ItemPointer &old_location,
                                      const ItemPointer &new_location) {
  LOG_TRACE("Performing Delete");

  auto tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(old_location.block)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();

  assert(GetLastReaderCid(tile_group_header, old_location.offset) <= current_txn->GetBeginCommitId());

  assert(tile_group_header->GetTransactionId(old_location.offset) ==
         transaction_id);
  assert(new_tile_group_header->GetTransactionId(new_location.offset) ==
         INVALID_TXN_ID);
  assert(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
         MAX_CID);
  assert(new_tile_group_header->GetEndCommitId(new_location.offset) == MAX_CID);

  // Set up double linked list
  auto old_next = tile_group_header->GetNextItemPointer(old_location.offset);
  new_tile_group_header->SetNextItemPointer(new_location.offset, old_next);

  COMPILER_MEMORY_FENCE;
  if (!old_next.IsNull()){
    auto old_next_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(old_next.block)->GetHeader();
    old_next_tile_group_header->SetPrevItemPointer(old_next.offset, new_location);
  }

  tile_group_header->SetNextItemPointer(old_location.offset, new_location);

  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  
  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

  InitTupleReserved(new_tile_group_header, new_location.offset);
  
  current_txn->RecordDelete(old_location);
}

void TsOrderTxnManager::PerformDelete(const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  assert(tile_group_header->GetTransactionId(tuple_id) ==
         current_txn->GetTransactionId());
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

  // Add the old tuple into the delete set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // if this version is not newly inserted.
    current_txn->RecordDelete(old_location);
  } else {
    // if this version is newly inserted.
    current_txn->RecordDelete(location);
  }
}

Result TsOrderTxnManager::CommitTransaction() {
  LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  if (current_txn->IsReadOnly() == true) {
    Result ret = current_txn->GetResult();

    EndTransaction();

    return ret;
  }

  auto &manager = catalog::Manager::GetInstance();

  // generate transaction id.
  cid_t end_commit_id = current_txn->GetBeginCommitId();

  auto &rw_set = current_txn->GetRWSet();

  // TODO: Add optimization for read only
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RW_TYPE_READ) {
        continue;
      } else if (tuple_entry.second == RW_TYPE_UPDATE) {
        // we must guarantee that, at any time point, only one version is
        // visible.
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        assert(cid > end_commit_id);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, cid);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INITIAL_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // GC recycle.
        RecycleOldTupleSlot(tile_group_id, tuple_slot, end_commit_id);

      } else if (tuple_entry.second == RW_TYPE_DELETE) {
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        assert(cid > end_commit_id);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, cid);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // GC recycle.
        RecycleOldTupleSlot(tile_group_id, tuple_slot, end_commit_id);

      } else if (tuple_entry.second == RW_TYPE_INSERT) {
        assert(tile_group_header->GetTransactionId(tuple_slot) ==
               current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        assert(tile_group_header->GetTransactionId(tuple_slot) ==
               current_txn->GetTransactionId());

        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        // set the begin commit id to persist insert
        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);
      }
    }
  }

  Result ret = current_txn->GetResult();

  EndTransaction();

  return ret;
}

Result TsOrderTxnManager::AbortTransaction() {
  LOG_TRACE("Aborting peloton txn : %lu ", current_txn->GetTransactionId());
  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetRWSet();

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RW_TYPE_UPDATE) {
        
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        // TODO: I think there is no need to set this fence.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        // reset the item pointers.
        auto old_next = new_tile_group_header->GetNextItemPointer(new_version.offset);
        if (old_next.IsNull() == false){
          auto old_next_tile_group_header = catalog::Manager::GetInstance()
            .GetTileGroup(old_next.block)->GetHeader();
          old_next_tile_group_header->SetPrevItemPointer(old_next.offset, ItemPointer(tile_group_id, tuple_slot));
        }


        tile_group_header->SetNextItemPointer(tuple_slot, old_next);
        COMPILER_MEMORY_FENCE;
        new_tile_group_header->SetPrevItemPointer(new_version.offset, INVALID_ITEMPOINTER);
        new_tile_group_header->SetNextItemPointer(new_version.offset, INVALID_ITEMPOINTER);
        // tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // GC recycle
        RecycleInvalidTupleSlot(new_version.block, new_version.offset);

      } else if (tuple_entry.second == RW_TYPE_DELETE) {
        // tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        // TODO: I think there is no need to set this fence.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        // reset the item pointers.
        auto old_next = new_tile_group_header->GetNextItemPointer(new_version.offset);
        if (!old_next.IsNull()){
          auto old_next_tile_group_header = catalog::Manager::GetInstance()
            .GetTileGroup(old_next.block)->GetHeader();
          old_next_tile_group_header->SetPrevItemPointer(old_next.offset, ItemPointer(tile_group_id, tuple_slot));
        }


        tile_group_header->SetNextItemPointer(tuple_slot, old_next);
        COMPILER_MEMORY_FENCE;
        new_tile_group_header->SetPrevItemPointer(new_version.offset, INVALID_ITEMPOINTER);
        new_tile_group_header->SetNextItemPointer(new_version.offset, INVALID_ITEMPOINTER);
        // tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // GC recycle
        RecycleInvalidTupleSlot(new_version.block, new_version.offset);

      } else if (tuple_entry.second == RW_TYPE_INSERT) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // GC recycle
        //RecycleInvalidTupleSlot(tile_group_id, tuple_slot);

      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // GC recycle
        RecycleInvalidTupleSlot(tile_group_id, tuple_slot);

      }
    }
  }

  EndTransaction();
  return Result::RESULT_ABORTED;
}
}
}
