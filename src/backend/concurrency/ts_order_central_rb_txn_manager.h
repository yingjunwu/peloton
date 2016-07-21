//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ts_order_central_rb_txn_manager.h
//
// Identification: src/backend/concurrency/ts_order_central_rb_txn_manager.h
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

// Each transaction has a RollbackSegmentPool
extern thread_local std::unordered_map<ItemPointer, index::RBItemPointer *> tcrb_updated_index_entries;
//===--------------------------------------------------------------------===//
// timestamp ordering with rollback segment
//===--------------------------------------------------------------------===//

class TsOrderCentralRbTxnManager : public TsOrderRbTxnManager {
public:
  typedef char* RBSegType;

  TsOrderCentralRbTxnManager() {}

  virtual ~TsOrderCentralRbTxnManager() {}

  static TsOrderCentralRbTxnManager &GetInstance();

  // Get current segment pool of the transaction manager
  virtual inline storage::RollbackSegmentPool *GetSegmentPool() {
    static storage::RollbackSegmentPool *segment_pool = new storage::RollbackSegmentPool(BACKEND_TYPE_MM);
    return segment_pool;
  }
};
}
}