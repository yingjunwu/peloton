//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ts_order_rb_txn_manager.cpp
//
// Identification: src/backend/concurrency/ts_order_rb_txn_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "ts_order_rb_txn_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"

namespace peloton {
namespace concurrency {

TsOrderRbTxnManager &TsOrderRbTxnManager::GetInstance() {
  static TsOrderRbTxnManager txn_manager;
  return txn_manager;
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// this function is called by update/delete executors.
bool TsOrderRbTxnManager::IsOwnable(
  const storage::TileGroupHeader *const tile_group_header,
  const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  char *evidence = GetActivatedRB(tile_group_header, tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID && evidence != nullptr;
}

// get write lock on a tuple.
// this is invoked by update/delete executors.
bool TsOrderRbTxnManager::AcquireOwnership(
  const storage::TileGroupHeader *const tile_group_header,
  const oid_t &tile_group_id __attribute__((unused)), const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();

  LockTuple(tile_group_header, tuple_id);
  // change timestamp
  cid_t last_reader_cid = GetLastReaderCid(tile_group_header, tuple_id);

  // If read by others after the current txn begins, can't acquire ownership
  if (last_reader_cid > current_txn->GetBeginCommitId()) {
    UnlockTuple(tile_group_header, tuple_id);
    SetTransactionResult(Result::RESULT_FAILURE);
    return false;
  } else {
    if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {    
      UnlockTuple(tile_group_header, tuple_id);
      SetTransactionResult(Result::RESULT_FAILURE);
      return false;
    } else {
      UnlockTuple(tile_group_header, tuple_id);
      return true;
    }
  }
}

bool TsOrderRbTxnManager::PerformRead(const ItemPointer &location) {
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

Result TsOrderRbTxnManager::CommitTransaction() {
  LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetRWSet();

  //*****************************************************
  // we can optimize read-only transaction.
  if (current_txn->IsReadOnly() == true) {
    Result ret = current_txn->GetResult();
    
    EndTransaction();

    return ret;
  }
  //*****************************************************

  // generate transaction id.
  cid_t end_commit_id = current_txn->GetBeginCommitId();

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
      if (tuple_entry.second == RW_TYPE_READ) {
        continue;
      } else if (tuple_entry.second == RW_TYPE_UPDATE) {
        // // logging.
        // log_manager.LogUpdate(current_txn, end_commit_id, old_version,
        //                       new_version);

        // First set the timestamp of the updated master copy
        // Since we have the rollback segment, it's safe to do so
        assert(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
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
        assert(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
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
        assert(tile_group_header->GetTransactionId(tuple_slot) ==
               current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        // ItemPointer insert_location(tile_group_id, tuple_slot);
        // log_manager.LogInsert(current_txn, end_commit_id, insert_location);

        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RW_TYPE_INS_DEL) {
        assert(tile_group_header->GetTransactionId(tuple_slot) == INVALID_TXN_ID);
        // Do nothing for INS_DEL
      }
    }
  }
  // log_manager.LogCommitTransaction(end_commit_id);
  current_txn->SetEndCommitId(end_commit_id);
  EndTransaction();

  return Result::RESULT_SUCCESS;
}

Result TsOrderRbTxnManager::AbortTransaction() {
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
