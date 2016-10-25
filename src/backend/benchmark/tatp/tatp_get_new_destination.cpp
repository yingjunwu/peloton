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
GetNewDestination *GenerateGetNewDestination(ZipfDistribution &zipf) {

  /*
         "SELECT cf.numberx " +
         "  FROM " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " sf, " +
         "       " + TATPConstants.TABLENAME_CALL_FORWARDING + " cf " +
         " WHERE sf.s_id = ? " +
         "   AND sf.sf_type = ? " +
         "   AND sf.is_active = 1 " +
         "   AND cf.s_id = sf.s_id " +
         "   AND cf.sf_type = sf.sf_type " +
         "   AND cf.start_time <= ? " +
         "   AND cf.end_time > ?"
  */

  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // PLAN FOR SPE
  /////////////////////////////////////////////////////////

  // SELECT
  std::vector<oid_t> spe_key_column_ids = {0, 1, 2};  // sid, sf_type, is_active
  std::vector<ExpressionType> spe_expr_types;
  spe_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  spe_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  spe_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> spe_key_values;

  // Get the secondary index key 2
  auto spe_skey_index2 = special_facility_table->GetIndexWithOid(
      special_facility_table_skey_index_oid2);

  planner::IndexScanPlan::IndexScanDesc spe_index_scan_desc(
      spe_skey_index2, spe_key_column_ids, spe_expr_types, spe_key_values,
      runtime_keys);

  // std::vector<oid_t> warehouse_column_ids = {1, 2, 3, 4, 5, 6, 8};
  std::vector<oid_t> spe_column_ids = {0};  // select sid

  planner::IndexScanPlan spe_index_scan_node(
      special_facility_table, nullptr, spe_column_ids, spe_index_scan_desc);

  executor::IndexScanExecutor *spe_index_scan_executor =
      new executor::IndexScanExecutor(&spe_index_scan_node, nullptr);

  spe_index_scan_executor->Init();

  /////////////////////////////////////////////////////////
  // PLAN FOR CALL
  /////////////////////////////////////////////////////////

  // SELECT
  std::vector<oid_t> call_key_column_ids = {
      0, 1, 2, 3};  // sid, sf_type, start_time, end_time

  std::vector<ExpressionType> call_expr_types;
  call_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  call_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  call_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  call_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> call_key_values;

  // Get the secondary index key 2
  auto call_skey_index2 = call_forwarding_table->GetIndexWithOid(
      call_forwarding_table_skey_index_oid2);

  planner::IndexScanPlan::IndexScanDesc call_index_scan_desc(
      call_skey_index2, call_key_column_ids, call_expr_types, call_key_values,
      runtime_keys);

  // std::vector<oid_t> warehouse_column_ids = {1, 2, 3, 4, 5, 6, 8};
  std::vector<oid_t> call_column_ids = {4};  // select numberx

  planner::IndexScanPlan call_index_scan_node(
      call_forwarding_table, nullptr, call_column_ids, call_index_scan_desc);

  executor::IndexScanExecutor *call_index_scan_executor =
      new executor::IndexScanExecutor(&call_index_scan_node, nullptr);

  call_index_scan_executor->Init();

  /////////////////////////////////////////////////////////

  GetNewDestination *gn = new GetNewDestination();

  gn->spe_index_scan_executor_ = spe_index_scan_executor;
  gn->call_index_scan_executor_ = call_index_scan_executor;

  // Set values
  gn->SetValue(zipf);

  // Set txn's region cover
  gn->SetRegionCover();

  return gn;
}

/*
 * Set the parameters needed by execution. Set the W_ID, D_ID, C_ID, I_ID.
 * So when a txn has all of the parameters when enqueue
 */
void GetNewDestination::SetValue(ZipfDistribution &zipf) {
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

  is_active_ = 1;

  // Take warehouse_id_ as the primary key
  primary_keys_.assign(1, sid_);
}

bool GetNewDestination::Run() {
  /*
         "SELECT cf.numberx " +
         "  FROM " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " sf, " +
         "       " + TATPConstants.TABLENAME_CALL_FORWARDING + " cf " +
         " WHERE sf.s_id = ? " +
         "   AND sf.sf_type = ? " +
         "   AND sf.is_active = 1 " +
         "   AND cf.s_id = sf.s_id " +
         "   AND cf.sf_type = sf.sf_type " +
         "   AND cf.start_time <= ? " +
         "   AND cf.end_time > ?"
  */

  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  int sid = sid_;
  int sf_type = sf_type_;
  int is_active = is_active_;
  int start_time = start_time_1_;
  int end_time = end_time_1_;

  /////////////////////////////////////////////////////////
  // BEGIN TRANSACTION
  /////////////////////////////////////////////////////////
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  SetContext(context.get());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SPE SELECTION
  /////////////////////////////////////////////////////////

  spe_index_scan_executor_->ResetState();

  std::vector<Value> spe_key_values;
  spe_key_values.push_back(ValueFactory::GetIntegerValue(sid));
  spe_key_values.push_back(ValueFactory::GetIntegerValue(sf_type));
  spe_key_values.push_back(ValueFactory::GetIntegerValue(is_active));

  spe_index_scan_executor_->SetValues(spe_key_values);

  auto spe_lists_values = ExecuteReadTest(spe_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  if (spe_lists_values.size() == 0) {
    goto finish_txn;
  }

  {
    std::vector<Value> call_key_values;
    // start time is 0 or 8 or 16. we need <= start time
    for (int start_times = 0; start_times <= start_time; start_times += 8) {
      // end time is from 1 to 24. we need > end time
      for (int end_times = end_time; end_times <= 24; end_times++) {

        //////////////////////////////////////////////////////////////////
        ///////////// Construct right table index scan ///////////////////
        //////////////////////////////////////////////////////////////////
        call_index_scan_executor_->ResetState();

        call_key_values.clear();
        call_key_values.push_back(ValueFactory::GetIntegerValue(sid));
        call_key_values.push_back(ValueFactory::GetIntegerValue(sf_type));
        call_key_values.push_back(ValueFactory::GetIntegerValue(start_times));
        call_key_values.push_back(ValueFactory::GetIntegerValue(end_times));

        call_index_scan_executor_->SetValues(call_key_values);

        auto call_values = ExecuteReadTest(call_index_scan_executor_);

        if (txn->GetResult() != Result::RESULT_SUCCESS) {
          LOG_TRACE("abort transaction");
          txn_manager.AbortTransaction();
          return false;
        }
      }
    }
  }

finish_txn:
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
