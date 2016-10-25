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
#include "backend/executor/delete_executor.h"

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
#include "backend/planner/delete_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace tatp {

/*
 * This function new a Amalgamate, so remember to delete it
 */
DeteteCallForwarding *GenerateDeteteCallForwarding(ZipfDistribution &zipf) {

  /*
  "UpdateLocation": {
  "SELECT s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr =
  ?"

   "DELETE FROM " + TATPConstants.TABLENAME_CALL_FORWARDING +
  " WHERE s_id = ? AND sf_type = ? AND start_time = ?"
    }
  */
  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // PLAN FOR SUBSCRIBER
  /////////////////////////////////////////////////////////

  // SELECT
  std::vector<oid_t> sub_key_column_ids;
  std::vector<ExpressionType> sub_expr_types;
  sub_key_column_ids.push_back(0);  // SID
  sub_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> sub_key_values;

  auto sub_pkey_index =
      subscriber_table->GetIndexWithOid(subscriber_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc sub_index_scan_desc(
      sub_pkey_index, sub_key_column_ids, sub_expr_types, sub_key_values,
      runtime_keys);

  // std::vector<oid_t> warehouse_column_ids = {1, 2, 3, 4, 5, 6, 8};
  std::vector<oid_t> sub_column_ids = {1};  // select sub_nbr from

  planner::IndexScanPlan sub_index_scan_node(
      subscriber_table, nullptr, sub_column_ids, sub_index_scan_desc);

  executor::IndexScanExecutor *sub_index_scan_executor =
      new executor::IndexScanExecutor(&sub_index_scan_node, nullptr);

  sub_index_scan_executor->Init();

  /////////////////////////////////////////////////////////
  // PLAN FOR CALL (Delete)
  /////////////////////////////////////////////////////////

  // Construct index scan executor
  std::vector<oid_t> call_delete_column_ids = {0};
  std::vector<oid_t> call_delete_key_column_ids = {0, 1, 2};

  // This is for key lookup
  std::vector<ExpressionType> call_delete_expr_types;
  call_delete_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  call_delete_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  call_delete_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  // This is for key values that are assigned when running
  std::vector<Value> call_delete_key_values;

  // Get the index
  auto call_pkey_index = call_forwarding_table->GetIndexWithOid(
      call_forwarding_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc call_delete_idex_scan_desc(
      call_pkey_index, call_delete_key_column_ids, call_delete_expr_types,
      call_delete_key_values, runtime_keys);

  // Create index scan plan node
  planner::IndexScanPlan call_delete_idex_scan_node(
      call_forwarding_table, nullptr, call_delete_column_ids,
      call_delete_idex_scan_desc);

  // Create executors
  executor::IndexScanExecutor *call_delete_index_scan_executor =
      new executor::IndexScanExecutor(&call_delete_idex_scan_node, nullptr);

  // Construct delete executor
  planner::DeletePlan call_delete_node(call_forwarding_table, false);

  executor::DeleteExecutor *call_delete_executor =
      new executor::DeleteExecutor(&call_delete_node, nullptr);

  call_delete_executor->AddChild(call_delete_index_scan_executor);

  call_delete_executor->Init();

  /////////////////////////////////////////////////////////

  DeteteCallForwarding *ul = new DeteteCallForwarding();

  ul->sub_index_scan_executor_ = sub_index_scan_executor;
  ul->call_del_index_scan_executor_ = call_delete_index_scan_executor;
  ul->call_del_executor_ = call_delete_executor;

  // Set values
  ul->SetValue(zipf);

  // Set txn's region cover
  ul->SetRegionCover();

  return ul;
}

/*
 * Set the parameters needed by execution. Set the W_ID, D_ID, C_ID, I_ID.
 * So when a txn has all of the parameters when enqueue
 */
void DeteteCallForwarding::SetValue(ZipfDistribution &zipf) {
  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  if (state.zipf_theta > 0) {
    sid_ = zipf.GetNextNumber();
  } else {
    sid_ = GenerateSubscriberId();
  }

  sf_type_ = GenerateSfTypeId();

  start_time_1_ = GenerateStartTime();

  // Take warehouse_id_ as the primary key
  primary_keys_.assign(1, sid_);
}

bool DeteteCallForwarding::Run() {
  /*
  "UpdateLocation": {
  "SELECT s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr =
  ?"

   "DELETE FROM " + TATPConstants.TABLENAME_CALL_FORWARDING +
  " WHERE s_id = ? AND sf_type = ? AND start_time = ?"
    }
  */

  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  int sid = sid_;
  int sf_type = sf_type_;
  int start_time = start_time_1_;

  /////////////////////////////////////////////////////////
  // BEGIN TRANSACTION
  /////////////////////////////////////////////////////////
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  SetContext(context.get());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SUBSCRIBER SELECTION
  /////////////////////////////////////////////////////////

  // "SELECT1 s_id FROM " + TABLENAME_SUBSCRIBER + " WHERE sub_nbr = ?"
  LOG_TRACE("SELECT * FROM SUBSCRIBER WHERE custid = %d", sid);

  sub_index_scan_executor_->ResetState();

  std::vector<Value> sub_key_values;

  sub_key_values.push_back(ValueFactory::GetIntegerValue(sid));

  sub_index_scan_executor_->SetValues(sub_key_values);

  auto ga1_lists_values = ExecuteReadTest(sub_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  if (ga1_lists_values.size() != 1) {
    LOG_ERROR("SUBSCRIBER return size incorrect : %lu",
              ga1_lists_values.size());
    // assert(false);
  }

  /////////////////////////////////////////////////////////
  // CALL DELETE
  /////////////////////////////////////////////////////////
  call_del_index_scan_executor_->ResetState();

  std::vector<Value> call_delete_key_values;

  call_delete_key_values.push_back(ValueFactory::GetIntegerValue(sid));
  call_delete_key_values.push_back(ValueFactory::GetIntegerValue(sf_type));
  call_delete_key_values.push_back(ValueFactory::GetIntegerValue(start_time));

  call_del_index_scan_executor_->SetValues(call_delete_key_values);

  // Execute the query
  ExecuteDeleteTest(call_del_index_scan_executor_);

  // Check if aborted
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  // transaction passed execution.
  assert(txn->GetResult() == Result::RESULT_SUCCESS);

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
