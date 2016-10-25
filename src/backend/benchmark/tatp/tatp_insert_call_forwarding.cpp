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
InsertCallForwarding *GenerateInsertCallForwarding(ZipfDistribution &zipf) {

  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // PLAN FOR SUBSCRIBER
  /////////////////////////////////////////////////////////

  // SELECT
  std::vector<oid_t> sub_key_column_ids;
  std::vector<ExpressionType> sub_expr_types;
  sub_key_column_ids.push_back(0);  // sID
  sub_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> sub_key_values;

  auto sub_pkey_index =
      subscriber_table->GetIndexWithOid(subscriber_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc sub_index_scan_desc(
      sub_pkey_index, sub_key_column_ids, sub_expr_types, sub_key_values,
      runtime_keys);

  std::vector<oid_t> sub_column_ids = {1};  // select NBR

  planner::IndexScanPlan sub_index_scan_node(
      subscriber_table, nullptr, sub_column_ids, sub_index_scan_desc);

  executor::IndexScanExecutor *sub_index_scan_executor =
      new executor::IndexScanExecutor(&sub_index_scan_node, nullptr);

  sub_index_scan_executor->Init();

  /////////////////////////////////////////////////////////
  // PLAN FOR SPE
  /////////////////////////////////////////////////////////

  // SELECT
  std::vector<oid_t> spe_key_column_ids;
  std::vector<ExpressionType> spe_expr_types;
  spe_key_column_ids.push_back(0);  // sID
  spe_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> spe_key_values;

  auto spe_skey_index = special_facility_table->GetIndexWithOid(
      special_facility_table_skey_index_oid);

  planner::IndexScanPlan::IndexScanDesc spe_index_scan_desc(
      spe_skey_index, spe_key_column_ids, spe_expr_types, spe_key_values,
      runtime_keys);

  std::vector<oid_t> spe_column_ids = {1};  // select sf_type

  planner::IndexScanPlan spe_index_scan_node(
      special_facility_table, nullptr, spe_column_ids, spe_index_scan_desc);

  executor::IndexScanExecutor *spe_index_scan_executor =
      new executor::IndexScanExecutor(&spe_index_scan_node, nullptr);

  spe_index_scan_executor->Init();

  /////////////////////////////////////////////////////////
  // PLAN FOR CALL
  /////////////////////////////////////////////////////////

  // INSERT
  planner::InsertPlan call_insert_plan_node(call_forwarding_table);
  executor::InsertExecutor *call_insert_executor =
      new executor::InsertExecutor(&call_insert_plan_node, nullptr);

  /////////////////////////////////////////////////////////

  InsertCallForwarding *sub = new InsertCallForwarding();

  sub->sub_index_scan_executor_ = sub_index_scan_executor;
  sub->spe_index_scan_executor_ = spe_index_scan_executor;
  sub->call_insert_executor_ = call_insert_executor;

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
void InsertCallForwarding::SetValue(ZipfDistribution &zipf) {
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

  end_time_1_ = GenerateEndTime();

  numberx_ = GetRandomAlphaNumericString(data_length_15);

  // Take warehouse_id_ as the primary key
  primary_keys_.assign(1, sid_);
}

bool InsertCallForwarding::Run() {
  /*
  "GetSubscriberData": {

  "SELECT s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr =
  ?"

  "SELECT sf_type FROM " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " WHERE
  s_id = ?"

  "INSERT INTO " + TATPConstants.TABLENAME_CALL_FORWARDING + " VALUES (?, ?, ?,
  ?, ?)"

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
  // SUBSCRIBER SELECTION
  /////////////////////////////////////////////////////////

  // "SELECT1 * FROM " + TABLENAME_ACCOUNTS + " WHERE custid = ?"
  LOG_TRACE("SELECT * FROM SUBSCRIBER WHERE sid = %d", sid);

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

  /////////////////////////////////////////////////////////
  // SPE SELECTION
  /////////////////////////////////////////////////////////

  // "SELECT1 * FROM " + TABLENAME_ACCOUNTS + " WHERE custid = ?"
  LOG_TRACE("SELECT * FROM SUBSCRIBER WHERE sid = %d", sid);

  spe_index_scan_executor_->ResetState();

  std::vector<Value> spe_key_values;

  spe_key_values.push_back(ValueFactory::GetIntegerValue(sid));

  spe_index_scan_executor_->SetValues(spe_key_values);

  auto gs_lists_values = ExecuteReadTest(spe_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  /////////////////////////////////////////////////////////
  // CALL INSERT
  /////////////////////////////////////////////////////////
  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));

  auto call_tuple = BuildCallForwardingTuple(sid_, sf_type_, start_time_1_,
                                             end_time_1_, pool);

  planner::InsertPlan call_insert_plan_node(call_forwarding_table,
                                            std::move(call_tuple));
  executor::InsertExecutor *call_insert_executor =
      new executor::InsertExecutor(&call_insert_plan_node, context_);
  call_insert_executor_ = call_insert_executor;

  ExecuteInsertTest(call_insert_executor_);

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
