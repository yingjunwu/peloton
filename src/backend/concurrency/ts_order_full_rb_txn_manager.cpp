//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ts_order_full_rb_txn_manager.cpp
//
// Identification: src/backend/concurrency/ts_order_full_rb_txn_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "ts_order_full_rb_txn_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"

namespace peloton {
namespace concurrency {

TsOrderFullRbTxnManager &TsOrderFullRbTxnManager::GetInstance() {
  static TsOrderFullRbTxnManager txn_manager;
  return txn_manager;
}

// This could be optimized, as we already have a full version in each rb, we don't need
// to rollback the RB chain one by one, we can go directly to the last one created
// by the transaction. Now the logic is the same ts_order_rb_txn_manager -- rx
void TsOrderFullRbTxnManager::RollbackTuple(std::shared_ptr<storage::TileGroup> tile_group, const oid_t tuple_id) {
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

}  // End storage namespace
}  // End peloton namespace
