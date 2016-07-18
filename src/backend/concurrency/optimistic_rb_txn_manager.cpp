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

#include "optimistic_rb_txn_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"

namespace peloton {

namespace concurrency {

OptimisticRbTxnManager &OptimisticRbTxnManager::GetInstance() {
  static OptimisticRbTxnManager txn_manager;
  return txn_manager;
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// this function is called by update/delete executors.
bool OptimisticRbTxnManager::IsOwnable(
  const storage::TileGroupHeader *const tile_group_header,
  const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  char *evidence = GetActivatedRB(tile_group_header, tuple_id);

  return tuple_txn_id == INITIAL_TXN_ID && 
        evidence == tile_group_header->GetReservedFieldRef(tuple_id);
}

// get write lock on a tuple.
// this is invoked by update/delete executors.
bool OptimisticRbTxnManager::AcquireOwnership(
  const storage::TileGroupHeader *const tile_group_header,
  const oid_t &tile_group_id __attribute__((unused)), const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();

  if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
    LOG_TRACE("Fail to acquire tuple. Set txn failure.");
    SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  }
  return true;
}

/**
 * @brief Check if begin commit id and end commit id still falls in the same version
 *        as when then transaction starts
 */
bool OptimisticRbTxnManager::ValidateRead(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id, const cid_t &end_cid) {
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

  if (IsOwner(tile_group_header, tuple_id) == true) {
    // Read tuple is owned by current txn
    return true;
  }

  // The following is essentially to test that begin_cid and end_cid will look at the same
  // version

  if (end_cid >= tuple_end_cid) {
    // Read tuple is invalidated by others
    return false;
  }

  auto evidence = GetActivatedRB(tile_group_header, tuple_id);

  if (evidence == tile_group_header->GetReservedFieldRef(tuple_id)) {
    // begin cid is activated on the master version
    // we already know that the end cid is less than the master version's end cid
    // since master end cid > end cid > begin cid >= master version's begin cid, we are ok
    return true;
  }

  // Now the evidence is a rollback segment
  // If the read is valid, the end cid should also be activated by that rollback segment
  return end_cid >= storage::RollbackSegmentPool::GetTimeStamp(evidence);
}

Result OptimisticRbTxnManager::CommitTransaction() {
  LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetRWSet();

  //*****************************************************
  // we can optimize read-only transaction.
  if (current_txn->IsReadOnly() == true) {
    // validate read set.
    for (auto &tile_group_entry : rw_set) {
      oid_t tile_group_id = tile_group_entry.first;
      auto tile_group = manager.GetTileGroup(tile_group_id);
      auto tile_group_header = tile_group->GetHeader();
      for (auto &tuple_entry : tile_group_entry.second) {
        auto tuple_slot = tuple_entry.first;
        // if this tuple is not newly inserted.
        if (tuple_entry.second == RW_TYPE_READ) {
          // No one should be writting, I can still read it and the begin commit
          // id still fall before the end commit id of the tuple
          //
          // To give an example why tile_group_header->GetEndCommitId(tuple_slot) >=
          //    current_txn->GetBeginCommitId() is needed
          //
          // T0 begin at 1, delete a tuple, then get end commit 2, but not commit yet
          // T1 begin at 3, read the same tuple, it should read the master version
          // T0 now commit, master version has been changed to be visible for (0, 2)
          // Now the master version is no longer visible for T0.
          if (tile_group_header->GetTransactionId(tuple_slot) == INITIAL_TXN_ID &&
            GetActivatedRB(tile_group_header, tuple_slot) != nullptr &&
              tile_group_header->GetEndCommitId(tuple_slot) >=
              current_txn->GetBeginCommitId()) {
            // the version is not owned by other txns and is still visible.
            continue;
          }
          LOG_TRACE("Abort in read only txn");
          // otherwise, validation fails. abort transaction.
          return AbortTransaction();
        } else {
          // It must be a deleted
          PL_ASSERT(tuple_entry.second == RW_TYPE_INS_DEL);
          PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot) == INVALID_TXN_ID);
        }
      }
    }
    
    EndTransaction();
    return Result::RESULT_SUCCESS;
  }
  //*****************************************************

  // generate transaction id.
  cid_t end_commit_id = GetNextCommitId();

  // validate read set.
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      // if this tuple is not newly inserted. Meaning this is either read,
      // update or deleted.
      if (tuple_entry.second != RW_TYPE_INSERT &&
          tuple_entry.second != RW_TYPE_INS_DEL) {

        if (ValidateRead(tile_group_header, tuple_slot, end_commit_id)) {
          continue;
        }
        LOG_TRACE("transaction id=%lu",
                  tile_group_header->GetTransactionId(tuple_slot));
        LOG_TRACE("begin commit id=%lu",
                  tile_group_header->GetBeginCommitId(tuple_slot));
        LOG_TRACE("end commit id=%lu",
                  tile_group_header->GetEndCommitId(tuple_slot));
        // otherwise, validation fails. abort transaction.
        return AbortTransaction();
      }
    }
  }
  //////////////////////////////////////////////////////////

  // Secondary index
  InstallSecondaryIndex(end_commit_id);

  // auto &log_manager = logging::LogManager::GetInstance();
  // log_manager.LogBeginTransaction(end_commit_id);
  // install everything.
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RW_TYPE_UPDATE) {
        // // logging.
        // log_manager.LogUpdate(current_txn, end_commit_id, old_version,
        //                       new_version);

        // First set the timestamp of the updated master copy
        // Since we have the rollback segment, it's safe to do so
        PL_ASSERT(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);

        // Then we mark all rollback segment's timestamp as our end timestamp
        InstallRollbackSegments(tile_group_header, tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        // Finally we release the write lock on the original tuple
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_DELETE) {
        // // logging.
        // log_manager.LogDelete(end_commit_id, delete_location);

        // we do not change begin cid for master copy
        // First set the timestamp of the master copy
        PL_ASSERT(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        // COMPILER_MEMORY_FENCE;

        // we may have updated this tuple before we delete it, roll it back
        RollbackTuple(tile_group, tuple_slot);

        // Reset the deleted bit for safety
        ClearDeleteFlag(tile_group_header, tuple_slot);

        // Set the timestamp of the entry corresponding to the latest version
        if (index::IndexFactory::GetSecondaryIndexType() == SECONDARY_INDEX_TYPE_VERSION) {
          index::RBItemPointer *index_ptr = (index::RBItemPointer *)GetSIndexPtr(tile_group_header, tuple_slot);
          if (index_ptr != nullptr)
            index_ptr->timestamp = end_commit_id;
        }

        COMPILER_MEMORY_FENCE;

        // Finally we release the write lock
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // recycle the newer version.
        // FIXME: need to delete them in index and free the tuple --jiexi
        // RecycleTupleSlot(tile_group_id, tuple_slot, START_OID);

      } else if (tuple_entry.second == RW_TYPE_INSERT) {
        PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot) ==
               current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        // ItemPointer insert_location(tile_group_id, tuple_slot);
        // log_manager.LogInsert(current_txn, end_commit_id, insert_location);

        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot) == INVALID_TXN_ID);
        // Do nothing for INS_DEL
      }
    }
  }
  // log_manager.LogCommitTransaction(end_commit_id);
  current_txn->SetEndCommitId(end_commit_id);
  EndTransaction();

  return Result::RESULT_SUCCESS;
}

Result OptimisticRbTxnManager::AbortTransaction() {
  LOG_TRACE("Aborting peloton txn : %lu ", current_txn->GetTransactionId());
  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetRWSet();

  // Delete from secondary index here, currently the workaround is just to 
  // invalidate the index entry by setting its timestamp to 0
  if (index::IndexFactory::GetSecondaryIndexType() == SECONDARY_INDEX_TYPE_VERSION) {
    for (auto itr = updated_index_entries.begin(); itr != updated_index_entries.end(); itr++) {
      ((index::RBItemPointer *)itr->second)->timestamp = 0;
    }
  }

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RW_TYPE_UPDATE) {

        // We do not have new version now, no need to mantain it
        assert(tile_group_header->GetNextItemPointer(tuple_slot).IsNull());

        // The master copy under updating must be a valid version
        assert(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);

        // Rollback the master copy
        RollbackTuple(tile_group, tuple_slot);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_DELETE) {

        // We do not have new version now, no need to mantain it
        assert(tile_group_header->GetNextItemPointer(tuple_slot).IsNull());

        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // We may have updated this tuple in the same txn, rollback it
        RollbackTuple(tile_group, tuple_slot);

        // Reset the deleted bit before release the write lock
        ClearDeleteFlag(tile_group_header, tuple_slot);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_INSERT) {
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // recycle the newer version.
        // FIXME: need to delete them in index and free the tuple --jiexi
        // RecycleTupleSlot(tile_group_id, tuple_slot, START_OID);

      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        assert(tile_group_header->GetTransactionId(tuple_slot) == INVALID_TXN_ID);
        // Do nothing for INS_DEL
        // GC this tuple
      }
    }
  }

  current_txn->SetResult(RESULT_ABORTED);
  EndTransaction();
  return Result::RESULT_ABORTED;
}

}  // End storage namespace
}  // End peloton namespace
