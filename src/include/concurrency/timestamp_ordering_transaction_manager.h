//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_ordering_transaction_manager.h
//
// Identification:
// src/include/concurrency/timestamp_ordering_transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrency/transaction_manager.h"
#include "storage/tile_group.h"
#include "statistics/stats_aggregator.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// timestamp ordering
//===--------------------------------------------------------------------===//

class TimestampOrderingTransactionManager : public TransactionManager {
 public:
  TimestampOrderingTransactionManager() {
    txn_counter = 0;
    stop = false;
    moniter_thread.reset(new std::thread(&TimestampOrderingTransactionManager::Monitor, this));
  }

  virtual ~TimestampOrderingTransactionManager() {
    stop = true;
    moniter_thread->join();
  }

  static TimestampOrderingTransactionManager &GetInstance();

  // This method is used for avoiding concurrent inserts.
  virtual bool IsOccupied(
      Transaction *const current_txn, 
      const void *position);

  virtual VisibilityType IsVisible(
      Transaction *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  // This method test whether the current transaction is the owner of a tuple.
  virtual bool IsOwner(Transaction *const current_txn,
                       const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id);

  // This method tests whether the current transaction has created this version of the tuple
  virtual bool IsWritten(
    Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id
  );

  // This method tests whether it is possible to obtain the ownership.
  virtual bool IsOwnable(
      Transaction *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  // This method is used to acquire the ownership of a tuple for a transaction.
  virtual bool AcquireOwnership(
      Transaction *const current_txn,
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  // This method is used by executor to yield ownership after the acquired
  // ownership.
  virtual void YieldOwnership(Transaction *const current_txn,
                              const oid_t &tile_group_id,
                              const oid_t &tuple_id);

  // The index_entry_ptr is the address of the head node of the version chain,
  // which is directly pointed by the primary index.
  virtual void PerformInsert(Transaction *const current_txn,
                             const ItemPointer &location,
                             ItemPointer *index_entry_ptr = nullptr);

  virtual bool PerformRead(Transaction *const current_txn,
                           const ItemPointer &location,
                           bool acquire_ownership = false);

  virtual void PerformUpdate(Transaction *const current_txn,
                             const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformDelete(Transaction *const current_txn,
                             const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(Transaction *const current_txn,
                             const ItemPointer &location);

  virtual void PerformDelete(Transaction *const current_txn,
                             const ItemPointer &location);

  virtual Result CommitTransaction(Transaction *const current_txn);

  virtual Result AbortTransaction(Transaction *const current_txn);

  virtual Transaction *BeginTransaction();

  virtual Transaction *BeginReadonlyTransaction();

  virtual void EndTransaction(Transaction *current_txn);

  virtual void EndReadonlyTransaction(Transaction *current_txn);

private:
  std::atomic<int> txn_counter;

  static const int LOCK_OFFSET = 0;
  static const int LAST_READER_OFFSET = (LOCK_OFFSET + 8);

  std::unique_ptr<std::thread> moniter_thread;
  bool stop;
  void Monitor() {
    while (!stop) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      printf("Transaction counter: %d， counter in factory: %d\n", txn_counter.load(), concurrency::TransactionManagerFactory::txn_counter.load());
    }
  }

  Spinlock *GetSpinlockField(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  cid_t GetLastReaderCommitId(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  bool SetLastReaderCommitId(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id, const cid_t &current_cid);

  // Initiate reserved area of a tuple
  void InitTupleReserved(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t tuple_id);
};
}
}
