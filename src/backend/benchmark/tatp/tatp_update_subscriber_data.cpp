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
UpdateSubscriberData *GenerateUpdateSubscriberData(ZipfDistribution &zipf) {

  /*
  "UpdateSubscriberData": {
  "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET bit_1 = ? WHERE s_id =
  ?"

  "UPDATE " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " SET data_a = ? WHERE
  s_id = ? AND sf_type = ?"
          }
  */

  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // PLAN FOR SUBSCRIBER
  /////////////////////////////////////////////////////////
  std::vector<oid_t> subscriber_key_column_ids;
  std::vector<ExpressionType> subscriber_expr_types;
  subscriber_key_column_ids.push_back(0);  // SID
  subscriber_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> subscriber_key_values;

  auto subscriber_pkey_index =
      subscriber_table->GetIndexWithOid(subscriber_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc subscriber_index_scan_desc(
      subscriber_pkey_index, subscriber_key_column_ids, subscriber_expr_types,
      subscriber_key_values, runtime_keys);

  // UPDATE bit_1
  std::vector<oid_t> subscriber_update_column_ids = {2};

  planner::IndexScanPlan subscriber_update_index_scan_node(
      subscriber_table, nullptr, subscriber_update_column_ids,
      subscriber_index_scan_desc);

  executor::IndexScanExecutor *subscriber_update_index_scan_executor =
      new executor::IndexScanExecutor(&subscriber_update_index_scan_node,
                                      nullptr);

  TargetList subscriber_target_list;
  DirectMapList subscriber_direct_map_list;

  // Keep the first 2 columns unchanged
  for (oid_t col_itr = 0; col_itr < 2; ++col_itr) {
    subscriber_direct_map_list.emplace_back(
        col_itr, std::pair<oid_t, oid_t>(0, col_itr));
  }

  std::unique_ptr<const planner::ProjectInfo> subscriber_project_info(
      new planner::ProjectInfo(std::move(subscriber_target_list),
                               std::move(subscriber_direct_map_list)));
  planner::UpdatePlan subscriber_update_node(
      subscriber_table, std::move(subscriber_project_info));

  executor::UpdateExecutor *subscriber_update_executor =
      new executor::UpdateExecutor(&subscriber_update_node, nullptr);

  subscriber_update_executor->AddChild(subscriber_update_index_scan_executor);

  subscriber_update_executor->Init();

  /////////////////////////////////////////////////////////
  // PLAN FOR SPECIAL FACILITY
  /////////////////////////////////////////////////////////

  // UPDATE
  std::vector<oid_t> spe_key_column_ids = {0, 1};  // pk: sid, sf_type
  std::vector<ExpressionType> spe_expr_types;
  spe_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  spe_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> spe_key_values;

  auto spe_pkey_index = special_facility_table->GetIndexWithOid(
      special_facility_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc spe_index_scan_desc(
      spe_pkey_index, spe_key_column_ids, spe_expr_types, spe_key_values,
      runtime_keys);

  // UPDATE data_a
  std::vector<oid_t> spe_update_column_ids = {4};

  planner::IndexScanPlan spe_update_index_scan_node(
      special_facility_table, nullptr, spe_update_column_ids,
      spe_index_scan_desc);

  executor::IndexScanExecutor *spe_update_index_scan_executor =
      new executor::IndexScanExecutor(&spe_update_index_scan_node, nullptr);

  TargetList spe_target_list;
  DirectMapList spe_direct_map_list;

  // Keep the 4 columns unchanged
  for (oid_t col_itr = 0; col_itr < 4; ++col_itr) {
    spe_direct_map_list.emplace_back(col_itr,
                                     std::pair<oid_t, oid_t>(0, col_itr));
  }

  std::unique_ptr<const planner::ProjectInfo> spe_project_info(
      new planner::ProjectInfo(std::move(spe_target_list),
                               std::move(spe_direct_map_list)));
  planner::UpdatePlan spe_update_node(special_facility_table,
                                      std::move(spe_project_info));

  executor::UpdateExecutor *spe_update_executor =
      new executor::UpdateExecutor(&spe_update_node, nullptr);

  spe_update_executor->AddChild(spe_update_index_scan_executor);

  spe_update_executor->Init();

  /////////////////////////////////////////////////////////

  UpdateSubscriberData *us = new UpdateSubscriberData();

  us->sub_update_index_scan_executor_ = subscriber_update_index_scan_executor;
  us->sub_update_executor_ = subscriber_update_executor;

  us->spe_update_index_scan_executor_ = spe_update_index_scan_executor;
  us->spe_update_executor_ = spe_update_executor;

  // Set values
  us->SetValue(zipf);

  // Set txn's region cover
  us->SetRegionCover();

  return us;
}

/*
 * Set the parameters needed by execution. Set the W_ID, D_ID, C_ID, I_ID.
 * So when a txn has all of the parameters when enqueue
 */
void UpdateSubscriberData::SetValue(ZipfDistribution &zipf) {
  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  if (state.zipf_theta > 0) {
    sid_ = zipf.GetNextNumber();
  } else {
    sid_ = GenerateSubscriberId();
  }

  sf_type_ = GenerateSfTypeId();

  // Take warehouse_id_ as the primary key
  primary_keys_.assign(1, sid_);
}

bool UpdateSubscriberData::Run() {
  /*
  "UpdateSubscriberData": {
  "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET bit_1 = ? WHERE s_id =
  ?"

  "UPDATE " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " SET data_a = ? WHERE
  s_id = ? AND sf_type = ?"
          }
  */

  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  int sid = sid_;
  int sf_type = sf_type_;

  /////////////////////////////////////////////////////////
  // BEGIN TRANSACTION
  /////////////////////////////////////////////////////////
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  SetContext(context.get());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SUBSCRIBER UPDATE
  /////////////////////////////////////////////////////////
  std::vector<Value> subscriber_key_values;
  subscriber_key_values.push_back(ValueFactory::GetIntegerValue(sid));

  // Update
  sub_update_index_scan_executor_->ResetState();

  sub_update_index_scan_executor_->SetValues(subscriber_key_values);

  TargetList subscriber_target_list;

  int bit = GetRandomInteger(MIN_BIT, MAX_BIT);

  Value subscriber_update_val = ValueFactory::GetIntegerValue(bit);

  // bit_1 column id is 2
  subscriber_target_list.emplace_back(
      2,
      expression::ExpressionUtil::ConstantValueFactory(subscriber_update_val));

  sub_update_executor_->SetTargetList(subscriber_target_list);

  ExecuteUpdateTest(sub_update_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  /////////////////////////////////////////////////////////
  // SPECIAL FACILTY UPDATE
  /////////////////////////////////////////////////////////

  // Update
  spe_update_index_scan_executor_->ResetState();

  std::vector<Value> spe_key_values;
  spe_key_values.push_back(ValueFactory::GetIntegerValue(sid));
  spe_key_values.push_back(ValueFactory::GetIntegerValue(sf_type));

  spe_update_index_scan_executor_->SetValues(spe_key_values);

  TargetList spe_target_list;

  int data_a = GetRandomInteger(MIN_BYTE, MAX_BYTE);

  Value spe_update_val = ValueFactory::GetIntegerValue(data_a);

  // data_a column id is 4
  spe_target_list.emplace_back(
      4, expression::ExpressionUtil::ConstantValueFactory(spe_update_val));

  spe_update_executor_->SetTargetList(spe_target_list);

  ExecuteUpdateTest(spe_update_executor_);

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
