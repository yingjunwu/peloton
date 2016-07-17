//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// occ_central_rb_txn_manager.h
//
// Identification: src/backend/concurrency/occ_central_rb_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/optimistic_rb_txn_manager.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/rollback_segment.h"
#include "backend/storage/data_table.h"
#include "backend/index/rb_btree_index.h"

namespace peloton {

namespace concurrency {

//===--------------------------------------------------------------------===//
// optimistic concurrency control with rollback segment
//===--------------------------------------------------------------------===//

class OptimisticCentralRbTxnManager : public OptimisticRbTxnManager {
public:
  OptimisticCentralRbTxnManager() {}

  virtual ~OptimisticCentralRbTxnManager() {}

  static OptimisticCentralRbTxnManager &GetInstance();

  // Get current segment pool of the transaction manager
  virtual inline storage::RollbackSegmentPool *GetSegmentPool() {
    static storage::RollbackSegmentPool *segment_pool = new storage::RollbackSegmentPool(BACKEND_TYPE_MM);
    return segment_pool;
  }
};
}
}