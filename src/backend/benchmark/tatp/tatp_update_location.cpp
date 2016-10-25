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
UpdateLocation *GenerateUpdateLocation(ZipfDistribution &zipf) {

  /*
  "UpdateLocation": {
  "SELECT s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr =
  ?"

  "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET vlr_location = ? WHERE
  s_id = ?"
        }
  */
  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // PLAN FOR SUBSCRIBER
  /////////////////////////////////////////////////////////

  // SELECT
  std::vector<oid_t> sub_key_column_ids = {0};
  std::vector<ExpressionType> sub_expr_types;
  sub_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> sub_key_values;

  auto sub_pkey_index =
      subscriber_table->GetIndexWithOid(subscriber_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc sub_index_scan_desc(
      sub_pkey_index, sub_key_column_ids, sub_expr_types, sub_key_values,
      runtime_keys);

  // select
  std::vector<oid_t> sub_column_ids = {1};  // select sub from

  planner::IndexScanPlan sub_index_scan_node(
      subscriber_table, nullptr, sub_column_ids, sub_index_scan_desc);

  executor::IndexScanExecutor *sub_index_scan_executor =
      new executor::IndexScanExecutor(&sub_index_scan_node, nullptr);

  sub_index_scan_executor->Init();

  // UPDATE vlr_location
  std::vector<oid_t> sub_update_column_ids = {33};

  planner::IndexScanPlan sub_update_index_scan_node(
      subscriber_table, nullptr, sub_update_column_ids, sub_index_scan_desc);

  executor::IndexScanExecutor *sub_update_index_scan_executor =
      new executor::IndexScanExecutor(&sub_update_index_scan_node, nullptr);

  TargetList sub_target_list;
  DirectMapList sub_direct_map_list;

  // Keep the first 33 columns unchanged
  for (oid_t col_itr = 0; col_itr < 33; ++col_itr) {
    sub_direct_map_list.emplace_back(col_itr,
                                     std::pair<oid_t, oid_t>(0, col_itr));
  }

  std::unique_ptr<const planner::ProjectInfo> sub_project_info(
      new planner::ProjectInfo(std::move(sub_target_list),
                               std::move(sub_direct_map_list)));
  planner::UpdatePlan sub_update_node(subscriber_table,
                                      std::move(sub_project_info));

  executor::UpdateExecutor *sub_update_executor =
      new executor::UpdateExecutor(&sub_update_node, nullptr);

  sub_update_executor->AddChild(sub_update_index_scan_executor);

  sub_update_executor->Init();
  /////////////////////////////////////////////////////////

  UpdateLocation *ul = new UpdateLocation();

  ul->sub_index_scan_executor_ = sub_index_scan_executor;
  ul->sub_update_index_scan_executor_ = sub_update_index_scan_executor;
  ul->sub_update_executor_ = sub_update_executor;

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
void UpdateLocation::SetValue(ZipfDistribution &zipf) {
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

bool UpdateLocation::Run() {
  /*
  "UpdateLocation": {
  "SELECT s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr =
  ?"

  "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET vlr_location = ? WHERE
  s_id = ?"
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

  std::vector<Value> sub_key_values;

  sub_key_values.push_back(ValueFactory::GetIntegerValue(sid));

  // "SELECT1 s_id FROM " + TABLENAME_SUBSCRIBER + " WHERE sub_nbr = ?"

  // Select
  sub_index_scan_executor_->ResetState();

  sub_index_scan_executor_->SetValues(sub_key_values);

  auto ga1_lists_values = ExecuteReadTest(sub_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  // Update
  sub_update_index_scan_executor_->ResetState();

  sub_update_index_scan_executor_->SetValues(sub_key_values);

  TargetList sub_target_list;

  int location = GetRandomInteger(MIN_INT, MAX_INT);

  Value sub_update_val = ValueFactory::GetIntegerValue(location);

  // var location's column is 33
  sub_target_list.emplace_back(
      33, expression::ExpressionUtil::ConstantValueFactory(sub_update_val));

  sub_update_executor_->SetTargetList(sub_target_list);

  ExecuteUpdateTest(sub_update_executor_);

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
