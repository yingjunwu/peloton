//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/tpcc/workload.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>

#include "backend/benchmark/tatp/tatp_workload.h"
#include "backend/benchmark/tatp/tatp_configuration.h"
#include "backend/benchmark/tatp/tatp_loader.h"

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
#include "backend/common/generator.h"

#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "backend/executor/executor_context.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/container_tuple.h"

#include "backend/index/index_factory.h"

#include "backend/logging/log_manager.h"

#include "backend/planner/abstract_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/index_scan_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace tatp {

/*
 * This function new a Amalgamate, so remember to delete it
 */
GetSubscriberData *GenerateGetSubscriberData(ZipfDistribution &zipf) {

  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // PLAN FOR ACCESS_INFO
  /////////////////////////////////////////////////////////
  std::vector<oid_t> subscriber_key_column_ids;
  std::vector<ExpressionType> subscriber_expr_types;
  subscriber_key_column_ids.push_back(0);  // sID
  subscriber_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> subscriber_key_values;

  auto subscriber_pkey_index =
      subscriber_table->GetIndexWithOid(subscriber_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc subscriber_index_scan_desc(
      subscriber_pkey_index, subscriber_key_column_ids, subscriber_expr_types,
      subscriber_key_values, runtime_keys);

  std::vector<oid_t> sub_column_ids = {0,  1,  2,  3,  4,  5,  6,  7,  8,
                                       9,  10, 11, 12, 13, 14, 15, 16, 17,
                                       18, 19, 20, 21, 22, 23, 24, 25, 26,
                                       27, 28, 29, 30, 31, 32, 33};  // select *

  planner::IndexScanPlan subscriber_index_scan_node(
      subscriber_table, nullptr, sub_column_ids, subscriber_index_scan_desc);

  executor::IndexScanExecutor *subscriber_index_scan_executor =
      new executor::IndexScanExecutor(&subscriber_index_scan_node, nullptr);

  subscriber_index_scan_executor->Init();

  /////////////////////////////////////////////////////////

  GetSubscriberData *sub = new GetSubscriberData();

  sub->sub_index_scan_executor_ = subscriber_index_scan_executor;

  // Set values
  sub->SetValue(zipf);

  // Set txn's region cover
  sub->SetRegionCover();

  return sub;
}

/*
 * Set the parameters needed by execution. Set the W_ID, D_ID, C_ID, I_ID.
 * So when a txn has all of the parameters when enqueue
 */
void GetSubscriberData::SetValue(ZipfDistribution &zipf) {
  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  if (state.zipf_theta > 0) {
    sid_ = zipf.GetNextNumber();
  } else {
    sid_ = GenerateSubscriberId();
  }

  // Take warehouse_id_ as the primary key
  primary_keys_.assign(1, sid_);
}

bool GetSubscriberData::Run() {
  /*
  "GetSubscriberData": {
  "SELECT * FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE s_id = ?"
    }
  */
  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  int sid = sid_;

  /////////////////////////////////////////////////////////
  // BEGIN TRANSACTION
  /////////////////////////////////////////////////////////
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  SetContext(context.get());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // ACCOUNTS SELECTION
  /////////////////////////////////////////////////////////

  // "SELECT1 * FROM " + TABLENAME_ACCOUNTS + " WHERE custid = ?"
  LOG_TRACE("SELECT * FROM SUBSCRIBER WHERE sid = %d", sid);

  sub_index_scan_executor_->ResetState();

  std::vector<Value> subscriber_key_values;

  subscriber_key_values.push_back(ValueFactory::GetIntegerValue(sid));

  sub_index_scan_executor_->SetValues(subscriber_key_values);

  auto ga1_lists_values = ExecuteReadTest(sub_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  /////////////////////////////////////////////////////////
  // TRANSACTION COMMIT
  /////////////////////////////////////////////////////////
  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    // transaction passed commitment.
    return true;

  } else {
    // transaction failed commitment.
    assert(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    return false;
  }
}
}
}
}
