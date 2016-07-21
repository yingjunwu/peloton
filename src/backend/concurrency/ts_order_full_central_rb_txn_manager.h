//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ts_order_full_central_rb_txn_manager.h
//
// Identification: src/backend/concurrency/ts_order_full_central_rb_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/ts_order_full_rb_txn_manager.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// timestamp ordering with rollback segment
//===--------------------------------------------------------------------===//

class TsOrderFullCentralRbTxnManager : public TsOrderFullRbTxnManager {
public:
  typedef char* RBSegType;

  TsOrderFullCentralRbTxnManager() {}

  virtual ~TsOrderFullCentralRbTxnManager() {}

  static TsOrderFullCentralRbTxnManager &GetInstance();

  // Rollback the master copy of a tuple to the status at the begin of the 
  // current transaction
  void RollbackTuple(std::shared_ptr<storage::TileGroup> tile_group,
                            const oid_t tuple_id);

  // Get current segment pool of the transaction manager
  virtual inline storage::RollbackSegmentPool *GetSegmentPool() {
    static storage::RollbackSegmentPool *segment_pool = new storage::RollbackSegmentPool(BACKEND_TYPE_MM);
    return segment_pool;
  }
};
}
}