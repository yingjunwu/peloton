//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// optimistic_rb_txn_manager.cpp
//
// Identification: src/backend/concurrency/optimistic_rb_txn_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "rb_txn_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/index/index_factory.h"

namespace peloton {

namespace concurrency {

thread_local storage::RollbackSegmentPool *current_segment_pool;
thread_local std::unordered_map<ItemPointer, void *> updated_index_entries;

/**
 * @brief Insert a tuple into secondary index. Notice that we only support at
 *  most one secondary index now for rollback segment.
 *
 * @param target_table Table of the version
 * @param location Location of the updated tuple
 * @param tuple New tuple
 */
bool RBTxnManager::RBInsertVersion(storage::DataTable *target_table,
  const ItemPointer &location, const storage::Tuple *tuple) {

  bool is_sindex_version = (index::IndexFactory::GetSecondaryIndexType() == 
    SECONDARY_INDEX_TYPE_VERSION);

  // Index checks and updates
  int index_count = target_table->GetIndexCount();

  std::function<bool(const void *)> fn =
      std::bind(&concurrency::TransactionManager::IsOccupied,
                this, std::placeholders::_1);

  // (A) Check existence for primary/unique indexes
  // FIXME Since this is NOT protected by a lock, concurrent insert may happen.
  // What does the above mean? - RX
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = target_table->GetIndex(index_itr);

    // Skip PK index
    if (index->GetIndexType() == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY) {
      continue;
    }

    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    index::RBItemPointer *rb_itempointer_ptr = nullptr;
    ItemPointer *itempointer_ptr = nullptr;

    if (is_sindex_version) {
      switch (index->GetIndexType()) {
      case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY:
        break;
      case INDEX_CONSTRAINT_TYPE_UNIQUE: {
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
        if (index->CondInsertEntry(key.get(), location, fn, &rb_itempointer_ptr) == false) {
          return false;
        }
        // Record into the updated index entry set, used when commit
        auto itr = updated_index_entries.find(location);
        if (itr != updated_index_entries.end()) {
          index->DeleteEntry(key.get(), *rb_itempointer_ptr);
          itr->second = rb_itempointer_ptr;
        } else {
          updated_index_entries.emplace(location, rb_itempointer_ptr);
        }
        
        break;
      }

      case INDEX_CONSTRAINT_TYPE_DEFAULT:
      default:
        index->InsertEntry(key.get(), location, &rb_itempointer_ptr);
        auto itr = updated_index_entries.find(location);
        if (itr != updated_index_entries.end()) {
          index->DeleteEntry(key.get(), *rb_itempointer_ptr);
          itr->second = rb_itempointer_ptr;
        } else {
          updated_index_entries.emplace(location, rb_itempointer_ptr);
        }
        // Record into the updated index entry set, used when commit
        break;
      }
    } else {
      switch (index->GetIndexType()) {
      case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY:
        break;
      case INDEX_CONSTRAINT_TYPE_UNIQUE: {
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
        if (index->CondInsertEntry(key.get(), location, fn, &itempointer_ptr) == false) {
          return false;
        }
        // Record into the updated index entry set, used when commit
        auto itr = updated_index_entries.find(location);
        if (itr != updated_index_entries.end()) {
          index->DeleteEntry(key.get(), *itempointer_ptr);
          itr->second = itempointer_ptr;
        } else {
          updated_index_entries.emplace(location, itempointer_ptr);
        }
        
        break;
      }

      case INDEX_CONSTRAINT_TYPE_DEFAULT:
      default:
        index->InsertEntry(key.get(), location, &itempointer_ptr);
        auto itr = updated_index_entries.find(location);
        if (itr != updated_index_entries.end()) {
          index->DeleteEntry(key.get(), *itempointer_ptr);
          itr->second = itempointer_ptr;
        } else {
          updated_index_entries.emplace(location, itempointer_ptr);
        }
        // Record into the updated index entry set, used when commit
        break;
      }
    }
    
    LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }
  return true;
}

void RBTxnManager::RollbackTuple(std::shared_ptr<storage::TileGroup> tile_group, const oid_t tuple_id) {
  auto tile_group_header = tile_group->GetHeader();
  auto txn_begin_cid = current_txn->GetBeginCommitId();

  auto rb_seg = GetRbSeg(tile_group_header, tuple_id);
  // Follow the RB chain, rollback if needed, stop when first unreadable RB
  while (IsRBVisible(rb_seg, txn_begin_cid) == true) {

    // Copy the content of the rollback segment onto the tuple
    tile_group->ApplyRollbackSegment(rb_seg, tuple_id);

    // Move to next rollback segment
    rb_seg = storage::RollbackSegmentPool::GetNextPtr(rb_seg);
  }

  COMPILER_MEMORY_FENCE;

  // Set the tile group header's rollback segment header to next rollback segment
  SetRbSeg(tile_group_header, tuple_id, rb_seg);
}

void RBTxnManager::InstallRollbackSegments(storage::TileGroupHeader *tile_group_header,
                                                      const oid_t tuple_id, const cid_t end_cid) {
  auto txn_begin_cid = current_txn->GetBeginCommitId();
  auto rb_seg = GetRbSeg(tile_group_header, tuple_id);

  while (IsRBVisible(rb_seg, txn_begin_cid)) {
    storage::RollbackSegmentPool::SetTimeStamp(rb_seg, end_cid);
    rb_seg = storage::RollbackSegmentPool::GetNextPtr(rb_seg);
  }
}

void RBTxnManager::PerformUpdateWithRb(const ItemPointer &location, 
  char *new_rb_seg) {

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;
  auto tile_group_header =
    catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();

  PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) == current_txn->GetTransactionId());
  PL_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // new_rb_seg is a new segment
  PL_ASSERT(storage::RollbackSegmentPool::GetNextPtr(new_rb_seg) == nullptr);
  PL_ASSERT(storage::RollbackSegmentPool::GetTimeStamp(new_rb_seg) == MAX_CID);

  // First link it to the old rollback segment
  auto old_rb_seg = GetRbSeg(tile_group_header, tuple_id);
  storage::RollbackSegmentPool::SetNextPtr(new_rb_seg, old_rb_seg);

  COMPILER_MEMORY_FENCE;

  // Modify the head of the segment list
  // Note that since we are holding the write lock, we don't need atomic update here
  SetRbSeg(tile_group_header, tuple_id, new_rb_seg);

  // Add the location to the update set
  current_txn->RecordUpdate(location);
}

// release write lock on a tuple.
// one example usage of this method is when a tuple is acquired, but operation
// (insert,update,delete) can't proceed, the executor needs to yield the 
// ownership before return false to upper layer.
// It should not be called if the tuple is in the write set as commit and abort
// will release the write lock anyway.
void RBTxnManager::YieldOwnership(const oid_t &tile_group_id,
  const oid_t &tuple_id) {

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  PL_ASSERT(IsOwner(tile_group_header, tuple_id));
  tile_group_header->SetTransactionId(tuple_id, INITIAL_TXN_ID);
}

// Visibility check
// check whether a tuple is visible to current transaction.
// in this protocol, we require that a transaction cannot see other
// transaction's local copy.
VisibilityType RBTxnManager::IsVisible(
  const storage::TileGroupHeader *const tile_group_header,
  const oid_t &tuple_id) {
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    // This is caused by an committed deletion
    // visibility_invisible is not possible, as aborted version will not show up in the centralized storage.
    return VISIBILITY_DELETED;
  }
  bool own = (current_txn->GetTransactionId() == tuple_txn_id);

  // there are exactly two versions that can be owned by a transaction.
  // unless it is an insertion.
  if (own == true) {
    if (GetDeleteFlag(tile_group_header, tuple_id) == true) {
      // the tuple is deleted by current transaction
      return VISIBILITY_DELETED;
    } else {
      PL_ASSERT(tuple_end_cid == MAX_CID);
      // the tuple is updated/inserted by current transaction
      return VISIBILITY_OK;
    }
  } else {
    bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);
    if (invalidated) {
      // a commited deleted tuple
      return VISIBILITY_DELETED;
    }
    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // The tuple is inserted
        return VISIBILITY_DELETED;
      }
    }

    if (GetActivatedRB(tile_group_header, tuple_id) != nullptr) {
      return VISIBILITY_OK;
    } else {
      // GetActivatedRB return nullptr if the master version is invisible,
      // which indicates a delete
      return VISIBILITY_DELETED;
    }
  }
}

// check whether the current transaction owns the tuple.
// this function is called by update/delete executors.
bool RBTxnManager::IsOwner(
  const storage::TileGroupHeader *const tile_group_header,
  const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  return tuple_txn_id == current_txn->GetTransactionId();
}

bool RBTxnManager::PerformRead(const ItemPointer &location) {
  current_txn->RecordRead(location);
  return true;
}

bool RBTxnManager::PerformInsert(const ItemPointer &location, void *item_ptr) {
  LOG_TRACE("Perform insert in RB with rb_itemptr %p", rb_item_ptr);

  // PL_ASSERT(rb_item_ptr != nullptr);
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // Set MVCC info
  PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  PL_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetTransactionId(tuple_id, transaction_id);

  // no need to set next item pointer.

  // init the reserved field
  InitTupleReserved(tile_group_header, tuple_id);

  SetSIndexPtr(tile_group_header, tuple_id, item_ptr);

  // Add the new tuple into the insert set
  current_txn->RecordInsert(location);
  return true;
}

void RBTxnManager::PerformDelete(const ItemPointer &location) {

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
         current_txn->GetTransactionId());

  // tuple deleted should be globally visible
  PL_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // Set the delete flag
  SetDeleteFlag(tile_group_header, tuple_id);

  // Add the old tuple into the delete set
  bool ins_del = current_txn->RecordDelete(location);

  // Check if we have INS_DEL, if so, just delete and reclaim the tuple
  if (ins_del) {
    // Mark as invalid
    tile_group_header->SetTransactionId(tuple_id, INVALID_TXN_ID);
    // recycle the inserted tuple
    // FIXME: need to delete them in index and free the tuple --jiexi
    // RecycleTupleSlot(tile_group_id, tuple_slot, START_OID);
  }
}

void RBTxnManager::InstallSecondaryIndex(cid_t end_commit_id) {
  auto &manager = catalog::Manager::GetInstance();
  // Install secondary index
  for (auto itr = updated_index_entries.begin(); itr != updated_index_entries.end(); itr++) {
    auto location = itr->first;
    oid_t tile_group_id = location.block;
    oid_t tuple_id = location.offset;
    auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

    if (index::IndexFactory::GetSecondaryIndexType() == SECONDARY_INDEX_TYPE_VERSION) {
      index::RBItemPointer *old_index_ptr = (index::RBItemPointer *)GetSIndexPtr(tile_group_header, tuple_id);
      old_index_ptr->timestamp = end_commit_id;
    }

    SetSIndexPtr(tile_group_header, tuple_id, itr->second);
  }
}

}  // End concurrency namespace
}  // End peloton namespace
