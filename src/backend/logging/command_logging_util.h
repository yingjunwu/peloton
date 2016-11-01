//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// command_logger.h
//
// Identification: src/backend/logging/command_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <thread>
#include <list>
#include <stack>
#include <unordered_map>

#include "backend/concurrency/transaction.h"
#include "backend/concurrency/epoch_manager.h"
#include "backend/logging/log_buffer.h"
#include "backend/logging/log_record.h"
#include "backend/logging/log_buffer_pool.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/worker_context.h"
#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "backend/common/pool.h"


namespace peloton {
namespace logging {

  struct OperationContext {
    OperationContext(LogRecordType type, storage::Tuple *tuple, storage::DataTable *table) {
      type_ = type;
      tuple_ = tuple;
      table_ = table;
    }

    LogRecordType type_;
    storage::Tuple *tuple_;
    storage::DataTable *table_;
  };

  typedef std::vector<OperationContext> OperationSet;

  struct ParamWrapper {
    ParamWrapper() {
      txn_cid_ = INVALID_CID;
      transaction_type_ = INVALID_TRANSACTION_TYPE;
      param_ = nullptr;
      operation_set_ = nullptr;
    }

    ParamWrapper(const cid_t txn_cid, const int transaction_type, TransactionParameter *param) {
      txn_cid_ = txn_cid;
      transaction_type_ = transaction_type;
      param_ = param;
      operation_set_ = nullptr;
    }

    ParamWrapper(const cid_t txn_cid, const int transaction_type, OperationSet *operation_set) {
      PL_ASSERT(transaction_type == INVALID_TRANSACTION_TYPE);

      txn_cid_ = txn_cid;
      transaction_type_ = transaction_type;
      param_ = nullptr;
      operation_set_ = operation_set;
    }

    cid_t txn_cid_;
    int transaction_type_;
    TransactionParameter *param_;
    OperationSet *operation_set_;
  };

}
}