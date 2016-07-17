//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ts_order_rb_txn_manager.h
//
// Identification: src/backend/concurrency/ts_order_rb_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/rb_txn_manager.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/rollback_segment.h"
#include "backend/storage/data_table.h"
#include "backend/index/rb_btree_index.h"

namespace peloton {

namespace concurrency {

//===--------------------------------------------------------------------===//
// timestamp ordering with rollback segment
//===--------------------------------------------------------------------===//

class TsOrderRbTxnManager : public RBTxnManager {
public:
  typedef char* RBSegType;

  TsOrderRbTxnManager() {}

  virtual ~TsOrderRbTxnManager() {}

  static TsOrderRbTxnManager &GetInstance();

  virtual bool IsOwnable(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tile_group_id, const oid_t &tuple_id);

  /**
   * Deprecated interfaces
   */
  virtual bool PerformRead(const ItemPointer &location);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  virtual Transaction *BeginTransaction() {
    // Set current transaction
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();

    LOG_TRACE("Beginning transaction %lu", txn_id);


    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;

    auto eid = EpochManagerFactory::GetInstance().EnterEpoch(begin_cid);
    txn->SetEpochId(eid);

    latest_read_ts = begin_cid;
    // Create current transaction poll
    current_segment_pool = new storage::RollbackSegmentPool(BACKEND_TYPE_MM);

    return txn;
  }

  virtual void EndTransaction() {
    auto result = current_txn->GetResult();
    auto end_cid = current_txn->GetEndCommitId();

    if (result == RESULT_SUCCESS) {
      // Committed
      if (current_txn->IsReadOnly()) {
        // read only txn, just delete the segment pool because it's empty
        delete current_segment_pool;
      } else {
        // It's not read only txn
        current_segment_pool->SetPoolTimestamp(end_cid);
        living_pools_[end_cid] = std::shared_ptr<peloton::storage::RollbackSegmentPool>(current_segment_pool);
      }
    } else {
      // Aborted
      // TODO: Add coperative GC
      current_segment_pool->MarkedAsGarbage();
      garbage_pools_[current_txn->GetBeginCommitId()] = std::shared_ptr<peloton::storage::RollbackSegmentPool>(current_segment_pool);
    }

    EpochManagerFactory::GetInstance().ExitEpoch(current_txn->GetEpochId());

    updated_index_entries.clear();
    delete current_txn;
    current_txn = nullptr;
    current_segment_pool = nullptr;
  }

  // Get current segment pool of the transaction manager
  virtual inline storage::RollbackSegmentPool *GetSegmentPool() {return current_segment_pool;}

 protected:
  static const size_t LAST_READER_OFFSET = LOCK_OFFSET + 8; // actually the lock also only occupies one byte.

  inline cid_t GetLastReaderCid(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id) {
    return *(cid_t*)(tile_group_header->GetReservedFieldRef(tuple_id) + LAST_READER_OFFSET);
  }

  inline bool SetLastReaderCid(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id) {
    assert(IsOwner(tile_group_header, tuple_id) == false);

    cid_t *ts_ptr = (cid_t*)(tile_group_header->GetReservedFieldRef(tuple_id) + LAST_READER_OFFSET);
    
    cid_t current_cid = current_txn->GetBeginCommitId();

    LockTuple(tile_group_header, tuple_id);
    
    txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
    
    if(tuple_txn_id != INITIAL_TXN_ID) {
      UnlockTuple(tile_group_header, tuple_id);
      return false;
    } else {
      if (*ts_ptr < current_cid) {
        *ts_ptr = current_cid;
      }
      UnlockTuple(tile_group_header, tuple_id);
      return true;
    }
  }
};
}
}