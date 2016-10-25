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
GetAccessData *GenerateGetAccessData(ZipfDistribution &zipf) {

  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // PLAN FOR ACCESS_INFO
  /////////////////////////////////////////////////////////
  std::vector<oid_t> access_key_column_ids = {0, 1};  // pkey: sid, ai_type
  std::vector<ExpressionType> access_expr_types;
  access_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  access_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> access_key_values;

  auto access_pkey_index =
      access_info_table->GetIndexWithOid(access_info_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc access_index_scan_desc(
      access_pkey_index, access_key_column_ids, access_expr_types,
      access_key_values, runtime_keys);

  std::vector<oid_t> access_column_ids = {2, 3, 4, 5};  // select data1,2,3,4

  planner::IndexScanPlan access_index_scan_node(
      access_info_table, nullptr, access_column_ids, access_index_scan_desc);

  executor::IndexScanExecutor *access_index_scan_executor =
      new executor::IndexScanExecutor(&access_index_scan_node, nullptr);

  access_index_scan_executor->Init();

  /////////////////////////////////////////////////////////

  GetAccessData *access = new GetAccessData();

  access->access_index_scan_executor_ = access_index_scan_executor;

  // Set values
  access->SetValue(zipf);

  // Set txn's region cover
  access->SetRegionCover();

  return access;
}

/*
 * Set the parameters needed by execution. Set the W_ID, D_ID, C_ID, I_ID.
 * So when a txn has all of the parameters when enqueue
 */
void GetAccessData::SetValue(ZipfDistribution &zipf) {
  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  if (state.zipf_theta > 0) {
    sid_ = zipf.GetNextNumber();
  } else {
    sid_ = GenerateSubscriberId();
  }

  ai_type_ = GenerateAiTypeId();

  // Take warehouse_id_ as the primary key
  primary_keys_.assign(1, sid_);
}

bool GetAccessData::Run() {
  /*
  "GetAccessData": {
  "SELECT data1, data2, data3, data4 FROM " +
  TATPConstants.TABLENAME_ACCESS_INFO +" WHERE s_id = ? AND ai_type = ?"
  }
  */

  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  int sid = sid_;
  int ai_type = ai_type_;

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
  LOG_TRACE("SELECT * FROM ACCOUNTS WHERE sid = %d", sid);

  access_index_scan_executor_->ResetState();

  std::vector<Value> access_key_values;
  access_key_values.push_back(ValueFactory::GetIntegerValue(sid));
  access_key_values.push_back(ValueFactory::GetIntegerValue(ai_type));

  access_index_scan_executor_->SetValues(access_key_values);

  auto ga1_lists_values = ExecuteReadTest(access_index_scan_executor_);

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
