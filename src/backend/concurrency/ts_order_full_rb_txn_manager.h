//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ts_order_full_rb_txn_manager.h
//
// Identification: src/backend/concurrency/ts_order_full_rb_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/ts_order_rb_txn_manager.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/rollback_segment.h"
#include "backend/storage/data_table.h"
#include "backend/index/rb_btree_index.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// timestamp ordering with rollback segment
//===--------------------------------------------------------------------===//

class TsOrderFullRbTxnManager : public TsOrderRbTxnManager {
public:
  typedef char* RBSegType;

  TsOrderFullRbTxnManager() {}

  virtual ~TsOrderFullRbTxnManager() {}

  static TsOrderFullRbTxnManager &GetInstance();

  // Rollback the master copy of a tuple to the status at the begin of the 
  // current transaction
  void RollbackTuple(std::shared_ptr<storage::TileGroup> tile_group,
                            const oid_t tuple_id);

  // Get current segment pool of the transaction manager
  inline storage::RollbackSegmentPool *GetSegmentPool() {return current_segment_pool;}
};
}
}