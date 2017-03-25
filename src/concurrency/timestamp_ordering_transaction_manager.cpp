//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_ordering_transaction_manager.cpp
//
// Identification: src/concurrency/timestamp_ordering_transaction_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/timestamp_ordering_transaction_manager.h"

#include "catalog/manager.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/platform.h"
#include "concurrency/transaction.h"
#include "gc/gc_manager_factory.h"
#include "logging/log_manager.h"
#include "logging/records/transaction_record.h"

namespace peloton {
namespace concurrency {

// timestamp ordering requires a spinlock field for protecting the atomic access
// to txn_id field and last_reader_cid field.
Spinlock *TimestampOrderingTransactionManager::GetSpinlockField(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  return (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                      LOCK_OFFSET);
}

// in timestamp ordering, the last_reader_cid records the timestamp of the last
// transaction
// that reads the tuple.
cid_t TimestampOrderingTransactionManager::GetLastReaderCommitId(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  return *(cid_t *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                    LAST_READER_OFFSET);
}

bool TimestampOrderingTransactionManager::SetLastReaderCommitId(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id, const cid_t &current_cid) {
  // get the pointer to the last_reader_cid field.
  cid_t *ts_ptr = (cid_t *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                            LAST_READER_OFFSET);

  GetSpinlockField(tile_group_header, tuple_id)->Lock();

  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  if (tuple_txn_id != INITIAL_TXN_ID) {
    // if the write lock has already been acquired by some concurrent
    // transactions,
    // then return without setting the last_reader_cid.
    GetSpinlockField(tile_group_header, tuple_id)->Unlock();
    return false;
  } else {
    // if current_cid is larger than the current value of last_reader_cid field,
    // then set last_reader_cid to current_cid.
    if (*ts_ptr < current_cid) {
      *ts_ptr = current_cid;
    }

    GetSpinlockField(tile_group_header, tuple_id)->Unlock();
    return true;
  }
}

// Initiate reserved area of a tuple
void TimestampOrderingTransactionManager::InitTupleReserved(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t tuple_id) {
  auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);

  new ((reserved_area + LOCK_OFFSET)) Spinlock();
  *(cid_t *)(reserved_area + LAST_READER_OFFSET) = 0;
}


TimestampOrderingTransactionManager &
TimestampOrderingTransactionManager::GetInstance(
      const IsolationLevelType level, 
      const ConflictAvoidanceType conflict) {
  static TimestampOrderingTransactionManager txn_manager(level, conflict);
  return txn_manager;
}


// check whether the current transaction owns the tuple version.
// this function is called by update/delete executors.
bool TimestampOrderingTransactionManager::IsOwner(
    Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);

  return tuple_txn_id == current_txn->GetTransactionId();
}

// This method tests whether the current transaction has created this version of
// the tuple
bool TimestampOrderingTransactionManager::IsWritten(
    Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  return IsOwner(current_txn, tile_group_header, tuple_id) &&
         tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID;
}

// if the tuple is not owned by any transaction and is visible to current
// transaction.
// this function is called by update/delete executors.
bool TimestampOrderingTransactionManager::IsOwnable(
    Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID &&
         tuple_end_cid > current_txn->GetBeginCommitId();
}

bool TimestampOrderingTransactionManager::AcquireOwnership(
    Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();

  // to acquire the ownership, we must guarantee that no other transactions that
  // has read
  // the tuple has a larger timestamp than the current transaction.
  GetSpinlockField(tile_group_header, tuple_id)->Lock();
  // change timestamp
  cid_t last_reader_cid = GetLastReaderCommitId(tile_group_header, tuple_id);

  if (last_reader_cid > current_txn->GetBeginCommitId()) {
    GetSpinlockField(tile_group_header, tuple_id)->Unlock();

    return false;
  } else {
    if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
      GetSpinlockField(tile_group_header, tuple_id)->Unlock();

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
void TimestampOrderingTransactionManager::YieldOwnership(
    UNUSED_ATTRIBUTE Transaction *const current_txn, 
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  PL_ASSERT(IsOwner(current_txn, tile_group_header, tuple_id));
  tile_group_header->SetTransactionId(tuple_id, INITIAL_TXN_ID);
}

bool TimestampOrderingTransactionManager::PerformRead(
    Transaction *const current_txn, const ItemPointer &location,
    bool acquire_ownership) {
  //////////////////////////////////////////////////////////
  //// handle READ_ONLY
  //////////////////////////////////////////////////////////
  if (current_txn->GetIsolationLevel() == IsolationLevelType::READ_ONLY) {
    // do not update read set for read-only transactions.
    return true;
  } // end READ ONLY

  //////////////////////////////////////////////////////////
  //// handle SNAPSHOT
  //////////////////////////////////////////////////////////
  else if (current_txn->GetIsolationLevel() == IsolationLevelType::SNAPSHOT) {
    // Check if it's select for update before we check the ownership 
    // and modify the last reader tid
    if (acquire_ownership == true) {
    
      oid_t tile_group_id = location.block;
      oid_t tuple_id = location.offset;

      LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);
      auto &manager = catalog::Manager::GetInstance();
      auto tile_group = manager.GetTileGroup(tile_group_id);
      auto tile_group_header = tile_group->GetHeader();

      if (IsOwner(current_txn, tile_group_header, tuple_id) == false) {
        // Acquire ownership if we haven't
        if (IsOwnable(current_txn, tile_group_header, tuple_id) == false) {
          // Cannot own
          return false;
        }
        if (AcquireOwnership(current_txn, tile_group_header, tuple_id) == false) {
          // Cannot acquire ownership
          return false;
        }
        // Promote to RWType::READ_OWN
        current_txn->RecordReadOwn(location);
      }
      // if the ownership has already been acquired, 
      // then no need to update read/write set.
      return true;
    
    } else {
      // if it's not select for update, then directly return true.
      // no need to update read/write set.
      return true;
    }

  } // end SNAPSHOT

  //////////////////////////////////////////////////////////
  //// handle READ_COMMITTED and READ_UNCOMMITTED
  //////////////////////////////////////////////////////////
  else if (current_txn->GetIsolationLevel() == IsolationLevelType::READ_COMMITTED || 
           current_txn->GetIsolationLevel() == IsolationLevelType::READ_UNCOMMITTED) {
    // do not update read set for READ_COMMITTED or READ_UNCOMMITTED.

    return true;
  } // end READ_COMMITTED || READ_UNCOMMITTED

  //////////////////////////////////////////////////////////
  //// handle SERIALIZABLE and REPEATABLE_READS
  //////////////////////////////////////////////////////////
  else { 
    PL_ASSERT(current_txn->GetIsolationLevel() == IsolationLevelType::SERIALIZABLE || 
              current_txn->GetIsolationLevel() == IsolationLevelType::REPEATABLE_READS);

    oid_t tile_group_id = location.block;
    oid_t tuple_id = location.offset;

    LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();

    // Check if it's select for update before we check the ownership 
    // and modify the last reader tid.
    if (acquire_ownership == true) {

      if (IsOwner(current_txn, tile_group_header, tuple_id) == false) {

        // if the current transaction does not own this tuple, 
        // then attempt to set last reader cid.
        if (SetLastReaderCommitId(tile_group_header, tuple_id,
                                  current_txn->GetBeginCommitId()) == true) {
          current_txn->RecordRead(location);
          // Increment table read op stats
          if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
            stats::BackendStatsContext::GetInstance()->IncrementTableReads(
                location.block);
          }
          return true;
        } else {
          // if the tuple has been owned by some concurrent transactions, 
          // then read fails.
          LOG_TRACE("Transaction read failed");
          return false;
        }

        // Acquire ownership if we haven't
        if (IsOwnable(current_txn, tile_group_header, tuple_id) == false) {
          // Cannot own
          return false;
        }
        if (AcquireOwnership(current_txn, tile_group_header, tuple_id) == false) {
          // Cannot acquire ownership
          return false;
        }
        // Promote to RWType::READ_OWN
        current_txn->RecordReadOwn(location);
      }

      // if the ownership has already been acquired, 
      // then no need to update read/write set.

      PL_ASSERT(IsOwner(current_txn, tile_group_header, tuple_id) == true);
      PL_ASSERT(GetLastReaderCommitId(tile_group_header, tuple_id) <=
                current_txn->GetBeginCommitId());
      // Increment table read op stats
      if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
        stats::BackendStatsContext::GetInstance()->IncrementTableReads(
            location.block);
      }
      return true;

    } else {

      if (IsOwner(current_txn, tile_group_header, tuple_id) == false) {

        // if the current transaction does not own this tuple, 
        // then attempt to set last reader cid.
        if (SetLastReaderCommitId(tile_group_header, tuple_id,
                                  current_txn->GetBeginCommitId()) == true) {
          
          // update read set.
          current_txn->RecordRead(location);
          
          // Increment table read op stats
          if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
            stats::BackendStatsContext::GetInstance()->IncrementTableReads(
                location.block);
          }
          return true;
        } else {
          // if the tuple has been owned by some concurrent transactions, 
          // then read fails.
          LOG_TRACE("Transaction read failed");
          return false;
        }

      } else {

        // if the current transaction has already owned this tuple, 
        // then perform read directly.
        PL_ASSERT(GetLastReaderCommitId(tile_group_header, tuple_id) <=
                  current_txn->GetBeginCommitId());

        // Increment table read op stats
        if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
          stats::BackendStatsContext::GetInstance()->IncrementTableReads(
              location.block);
        }
        return true;
      }
    }

  } // end SERIALIZABLE || REPEATABLE_READS
}

void TimestampOrderingTransactionManager::PerformInsert(
    Transaction *const current_txn, const ItemPointer &location,
    ItemPointer *index_entry_ptr) {
  PL_ASSERT(current_txn->GetIsolationLevel() != IsolationLevelType::READ_ONLY);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // check MVCC info
  // the tuple slot must be empty.
  PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  PL_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetTransactionId(tuple_id, transaction_id);

  // no need to set next item pointer.

  // Add the new tuple into the insert set
  current_txn->RecordInsert(location);

  InitTupleReserved(tile_group_header, tuple_id);

  // Write down the head pointer's address in tile group header
  tile_group_header->SetIndirection(tuple_id, index_entry_ptr);

  // Increment table insert op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableInserts(
        location.block);
  }
}

void TimestampOrderingTransactionManager::PerformUpdate(
    Transaction *const current_txn, const ItemPointer &old_location,
    const ItemPointer &new_location) {
  PL_ASSERT(current_txn->GetIsolationLevel() == IsolationLevelType::READ_ONLY);

  LOG_TRACE("Performing Write old tuple %u %u", old_location.block,
            old_location.offset);
  LOG_TRACE("Performing Write new tuple %u %u", new_location.block,
            new_location.offset);

  auto tile_group_header = catalog::Manager::GetInstance()
                               .GetTileGroup(old_location.block)
                               ->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
                                   .GetTileGroup(new_location.block)
                                   ->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();
  // if we can perform update, then we must have already locked the older
  // version.
  PL_ASSERT(tile_group_header->GetTransactionId(old_location.offset) ==
            transaction_id);
  PL_ASSERT(new_tile_group_header->GetTransactionId(new_location.offset) ==
            INVALID_TXN_ID);
  PL_ASSERT(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
            MAX_CID);
  PL_ASSERT(new_tile_group_header->GetEndCommitId(new_location.offset) ==
            MAX_CID);

  // if the executor doesn't call PerformUpdate after AcquireOwnership,
  // no one will possibly release the write lock acquired by this txn.
  // Set double linked list
  // old_prev is the version next (newer) to the old version.

  auto old_prev = tile_group_header->GetPrevItemPointer(old_location.offset);

  tile_group_header->SetPrevItemPointer(old_location.offset, new_location);

  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_prev);

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  // we should guarantee that the newer version is all set before linking the
  // newer version to older version.
  COMPILER_MEMORY_FENCE;

  if (old_prev.IsNull() == false) {
    auto old_prev_tile_group_header = catalog::Manager::GetInstance()
                                          .GetTileGroup(old_prev.block)
                                          ->GetHeader();

    // once everything is set, we can allow traversing the new version.
    old_prev_tile_group_header->SetNextItemPointer(old_prev.offset,
                                                   new_location);
  }

  InitTupleReserved(new_tile_group_header, new_location.offset);

  // if the transaction is not updating the latest version,
  // then do not change item pointer header.
  if (old_prev.IsNull() == true) {
    // if we are updating the latest version.
    // Set the header information for the new version
    ItemPointer *index_entry_ptr =
        tile_group_header->GetIndirection(old_location.offset);

    if (index_entry_ptr != nullptr) {

      new_tile_group_header->SetIndirection(new_location.offset,
                                            index_entry_ptr);

      // Set the index header in an atomic way.
      // We do it atomically because we don't want any one to see a half-done
      // pointer.
      // In case of contention, no one can update this pointer when we are
      // updating it
      // because we are holding the write lock. This update should success in
      // its first trial.
      UNUSED_ATTRIBUTE auto res =
          AtomicUpdateItemPointer(index_entry_ptr, new_location);
      PL_ASSERT(res == true);
    }
  }

  // Add the old tuple into the update set
  current_txn->RecordUpdate(old_location);

  // Increment table update op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableUpdates(
        new_location.block);
  }
}

void TimestampOrderingTransactionManager::PerformUpdate(
    Transaction *const current_txn, const ItemPointer &location) {
  PL_ASSERT(current_txn->GetIsolationLevel() == IsolationLevelType::READ_ONLY);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
            current_txn->GetTransactionId());
  PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  PL_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // Add the old tuple into the update set
  auto old_location = tile_group_header->GetNextItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // update an inserted version
    current_txn->RecordUpdate(old_location);
  }

  // Increment table update op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableUpdates(
        location.block);
  }
}

void TimestampOrderingTransactionManager::PerformDelete(
    Transaction *const current_txn, const ItemPointer &old_location,
    const ItemPointer &new_location) {
  PL_ASSERT(current_txn->GetIsolationLevel() == IsolationLevelType::READ_ONLY);

  LOG_TRACE("Performing Delete");

  auto tile_group_header = catalog::Manager::GetInstance()
                               .GetTileGroup(old_location.block)
                               ->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
                                   .GetTileGroup(new_location.block)
                                   ->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();

  PL_ASSERT(GetLastReaderCommitId(tile_group_header, old_location.offset) <=
            current_txn->GetBeginCommitId());

  PL_ASSERT(tile_group_header->GetTransactionId(old_location.offset) ==
            transaction_id);
  PL_ASSERT(new_tile_group_header->GetTransactionId(new_location.offset) ==
            INVALID_TXN_ID);
  PL_ASSERT(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
            MAX_CID);
  PL_ASSERT(new_tile_group_header->GetEndCommitId(new_location.offset) ==
            MAX_CID);

  // Set up double linked list

  auto old_prev = tile_group_header->GetPrevItemPointer(old_location.offset);

  tile_group_header->SetPrevItemPointer(old_location.offset, new_location);

  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_prev);

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

  // we should guarantee that the newer version is all set before linking the
  // newer version to older version.
  COMPILER_MEMORY_FENCE;

  if (old_prev.IsNull() == false) {
    auto old_prev_tile_group_header = catalog::Manager::GetInstance()
                                          .GetTileGroup(old_prev.block)
                                          ->GetHeader();

    old_prev_tile_group_header->SetNextItemPointer(old_prev.offset,
                                                   new_location);
  }

  InitTupleReserved(new_tile_group_header, new_location.offset);

  // if the transaction is not deleting the latest version,
  // then do not change item pointer header.
  if (old_prev.IsNull() == true) {
    // if we are deleting the latest version.
    // Set the header information for the new version
    ItemPointer *index_entry_ptr =
        tile_group_header->GetIndirection(old_location.offset);

    // if there's no primary index on a table, then index_entry_ptry == nullptr.
    if (index_entry_ptr != nullptr) {
      new_tile_group_header->SetIndirection(new_location.offset,
                                            index_entry_ptr);

      // Set the index header in an atomic way.
      // We do it atomically because we don't want any one to see a half-down
      // pointer
      // In case of contention, no one can update this pointer when we are
      // updating it
      // because we are holding the write lock. This update should success in
      // its first trial.
      UNUSED_ATTRIBUTE auto res =
          AtomicUpdateItemPointer(index_entry_ptr, new_location);
      PL_ASSERT(res == true);
    }
  }

  current_txn->RecordDelete(old_location);

  // Increment table delete op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableDeletes(
        old_location.block);
  }
}

void TimestampOrderingTransactionManager::PerformDelete(
    Transaction *const current_txn, const ItemPointer &location) {
  PL_ASSERT(current_txn->GetIsolationLevel() == IsolationLevelType::READ_ONLY);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
            current_txn->GetTransactionId());
  PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

  // Add the old tuple into the delete set
  auto old_location = tile_group_header->GetNextItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // if this version is not newly inserted.
    current_txn->RecordDelete(old_location);
  } else {
    // if this version is newly inserted.
    current_txn->RecordDelete(location);
  }

  // Increment table delete op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableDeletes(
        location.block);
  }
}

ResultType TimestampOrderingTransactionManager::CommitTransaction(
    Transaction *const current_txn) {
  LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  //////////////////////////////////////////////////////////
  //// handle READ_ONLY
  //////////////////////////////////////////////////////////
  if (current_txn->GetIsolationLevel() == IsolationLevelType::READ_ONLY) {
    EndTransaction(current_txn);
    return ResultType::SUCCESS;
  }

  //////////////////////////////////////////////////////////
  //// handle other isolation levels
  //////////////////////////////////////////////////////////


  auto &manager = catalog::Manager::GetInstance();
  auto &log_manager = logging::LogManager::GetInstance();

  // generate transaction id.
  cid_t end_commit_id = current_txn->GetBeginCommitId();
  log_manager.LogBeginTransaction(end_commit_id);

  auto &rw_set = current_txn->GetReadWriteSet();

  auto gc_set = current_txn->GetGCSetPtr();

  oid_t database_id = 0;
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    if (!rw_set.empty()) {
      database_id =
          manager.GetTileGroup(rw_set.begin()->first)->GetDatabaseId();
    }
  }

  // install everything.
  // 1. install a new version for update operations;
  // 2. install an empty version for delete operations;
  // 3. install a new tuple for insert operations.
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RWType::READ_OWN) {
        // A read operation has acquired ownership but hasn't done any further
        // update/delete yet
        // Yield the ownership
        YieldOwnership(current_txn, tile_group_header, tuple_slot);
      } else if (tuple_entry.second == RWType::UPDATE) {
        // we must guarantee that, at any time point, only one version is
        // visible.
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        PL_ASSERT(new_version.IsNull() == false);

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        PL_ASSERT(cid > end_commit_id);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, cid);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INITIAL_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = false;

        // add to log manager
        log_manager.LogUpdate(
            end_commit_id, ItemPointer(tile_group_id, tuple_slot), new_version);

      } else if (tuple_entry.second == RWType::DELETE) {
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        PL_ASSERT(cid > end_commit_id);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, cid);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        // we need to recycle both old and new versions.
        // we require the GC to delete tuple from index only once.
        // recycle old version, delete from index
        gc_set->operator[](tile_group_id)[tuple_slot] = true;
        // recycle new version (which is an empty version), do not delete from index
        gc_set->operator[](new_version.block)[new_version.offset] = false;

        // add to log manager
        log_manager.LogDelete(end_commit_id,
                              ItemPointer(tile_group_id, tuple_slot));

      } else if (tuple_entry.second == RWType::INSERT) {
        PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot) ==
                  current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // nothing to be added to gc set.

        // add to log manager
        log_manager.LogInsert(end_commit_id,
                              ItemPointer(tile_group_id, tuple_slot));

      } else if (tuple_entry.second == RWType::INS_DEL) {
        PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot) ==
                  current_txn->GetTransactionId());

        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        // set the begin commit id to persist insert
        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = true;

        // no log is needed for this case
      }
    }
  }

  ResultType result = current_txn->GetResult();

  EndTransaction(current_txn);

  // Increment # txns committed metric
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTxnCommitted(
        database_id);
  }

  return result;
}

ResultType TimestampOrderingTransactionManager::AbortTransaction(
    Transaction *const current_txn) {
  // It's impossible that a pre-declared read-only transaction aborts
  PL_ASSERT(current_txn->GetIsolationLevel() == IsolationLevelType::READ_ONLY);

  LOG_TRACE("Aborting peloton txn : %lu ", current_txn->GetTransactionId());
  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetReadWriteSet();

  auto gc_set = current_txn->GetGCSetPtr();

  oid_t database_id = 0;
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    if (!rw_set.empty()) {
      database_id =
          manager.GetTileGroup(rw_set.begin()->first)->GetDatabaseId();
    }
  }

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RWType::READ_OWN) {
        // A read operation has acquired ownership but hasn't done any further
        // update/delete yet
        // Yield the ownership
        YieldOwnership(current_txn, tile_group_header, tuple_slot);
      } else if (tuple_entry.second == RWType::UPDATE) {
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();

        // these two fields can be set at any time.
        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        // as the aborted version has already been placed in the version chain,
        // we need to unlink it by resetting the item pointers.
        auto old_prev =
            new_tile_group_header->GetPrevItemPointer(new_version.offset);

        // check whether the previous version exists.
        if (old_prev.IsNull() == true) {
          PL_ASSERT(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
          // if we updated the latest version.
          // We must first adjust the head pointer
          // before we unlink the aborted version from version list
          ItemPointer *index_entry_ptr =
              tile_group_header->GetIndirection(tuple_slot);
          UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(
              index_entry_ptr, ItemPointer(tile_group_id, tuple_slot));
          PL_ASSERT(res == true);
        }
        //////////////////////////////////////////////////

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        if (old_prev.IsNull() == false) {
          auto old_prev_tile_group_header = catalog::Manager::GetInstance()
                                                .GetTileGroup(old_prev.block)
                                                ->GetHeader();
          old_prev_tile_group_header->SetNextItemPointer(
              old_prev.offset, ItemPointer(tile_group_id, tuple_slot));
          tile_group_header->SetPrevItemPointer(tuple_slot, old_prev);
        } else {
          tile_group_header->SetPrevItemPointer(tuple_slot,
                                                INVALID_ITEMPOINTER);
        }

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        gc_set->operator[](new_version.block)[new_version.offset] = false;

      } else if (tuple_entry.second == RWType::DELETE) {
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();

        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        // as the aborted version has already been placed in the version chain,
        // we need to unlink it by resetting the item pointers.
        auto old_prev =
            new_tile_group_header->GetPrevItemPointer(new_version.offset);

        // check whether the previous version exists.
        if (old_prev.IsNull() == true) {
          // if we updated the latest version.
          // We must first adjust the head pointer
          // before we unlink the aborted version from version list
          ItemPointer *index_entry_ptr =
              tile_group_header->GetIndirection(tuple_slot);
          UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(
              index_entry_ptr, ItemPointer(tile_group_id, tuple_slot));
          PL_ASSERT(res == true);
        }
        //////////////////////////////////////////////////

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        if (old_prev.IsNull() == false) {
          auto old_prev_tile_group_header = catalog::Manager::GetInstance()
                                                .GetTileGroup(old_prev.block)
                                                ->GetHeader();
          old_prev_tile_group_header->SetNextItemPointer(
              old_prev.offset, ItemPointer(tile_group_id, tuple_slot));
        }

        tile_group_header->SetPrevItemPointer(tuple_slot, old_prev);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        gc_set->operator[](new_version.block)[new_version.offset] = false;

      } else if (tuple_entry.second == RWType::INSERT) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add to gc set.
        // delete from index
        gc_set->operator[](tile_group_id)[tuple_slot] = true;

      } else if (tuple_entry.second == RWType::INS_DEL) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = true;
      }
    }
  }

  current_txn->SetResult(ResultType::ABORTED);
  EndTransaction(current_txn);

  // Increment # txns aborted metric
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTxnAborted(database_id);
  }

  return ResultType::ABORTED;
}

}  // End storage namespace
}  // End peloton namespace
