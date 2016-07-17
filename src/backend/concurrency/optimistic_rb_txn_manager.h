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

#include "backend/concurrency/rb_txn_manager.h"

namespace peloton {

namespace concurrency {
//===--------------------------------------------------------------------===//
// optimistic concurrency control with rollback segment
//===--------------------------------------------------------------------===//

class OptimisticRbTxnManager : public RBTxnManager {
public:
  typedef char* RBSegType;

  OptimisticRbTxnManager() {}

  virtual ~OptimisticRbTxnManager() {}

  static OptimisticRbTxnManager &GetInstance();

  virtual bool IsOwnable(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tile_group_id, const oid_t &tuple_id);

  bool ValidateRead( 
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id,
    const cid_t &end_cid);

  // Get the read timestamp of the latest transaction on this thread, it is 
  // either the begin commit time of current transaction of the just committed
  // transaction.
  cid_t GetLatestReadTimestamp() {
    return latest_read_ts;
  }

  /**
   * Interfaces for rollback segment
   */

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  virtual Transaction *BeginTransaction() {
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = GetNextCommitId();

    LOG_TRACE("Beginning transaction %lu", txn_id);


    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;

    auto eid = EpochManagerFactory::GetInstance().EnterEpoch(begin_cid);
    txn->SetEpochId(eid);

    latest_read_ts = begin_cid;
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
  virtual inline storage::RollbackSegmentPool *GetSegmentPool() {
    return current_segment_pool;
  }
};
}
}