//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/smallbank/workload.cpp
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

#include "backend/benchmark/smallbank/smallbank_workload.h"
#include "backend/benchmark/smallbank/smallbank_configuration.h"
#include "backend/benchmark/smallbank/smallbank_loader.h"

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
namespace smallbank {


DepositCheckingPlans PrepareDepositCheckingPlan() {

  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // PLAN FOR ACCOUNTS
  /////////////////////////////////////////////////////////
  
  std::vector<oid_t> accounts_key_column_ids;
  std::vector<ExpressionType> accounts_expr_types;
  
  accounts_key_column_ids.push_back(0);  // CUSTID
  accounts_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> accounts_key_values;

  auto accounts_pkey_index =
      accounts_table->GetIndexWithOid(accounts_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc accounts_index_scan_desc(
      accounts_pkey_index, accounts_key_column_ids, accounts_expr_types,
      accounts_key_values, runtime_keys);

  std::vector<oid_t> accounts_column_ids = {0, 1};  // CUSTID, NAME

  planner::IndexScanPlan accounts_index_scan_node(
      accounts_table, nullptr, accounts_column_ids, accounts_index_scan_desc);

  executor::IndexScanExecutor *accounts_index_scan_executor =
      new executor::IndexScanExecutor(&accounts_index_scan_node, nullptr);

  accounts_index_scan_executor->Init();

  /////////////////////////////////////////////////////////
  // PLAN FOR CHECKING
  /////////////////////////////////////////////////////////
  
  std::vector<oid_t> checking_key_column_ids;
  std::vector<ExpressionType> checking_expr_types;
  
  checking_key_column_ids.push_back(0);  // CUSTID
  checking_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> checking_key_values;

  auto checking_pkey_index =
      checking_table->GetIndexWithOid(checking_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc checking_index_scan_desc(
      checking_pkey_index, checking_key_column_ids, checking_expr_types,
      checking_key_values, runtime_keys);

  // std::vector<oid_t> warehouse_column_ids = {1, 2, 3, 4, 5, 6, 8};
  std::vector<oid_t> checking_column_ids = {1};  // select BAL from

  planner::IndexScanPlan checking_index_scan_node(
      checking_table, nullptr, checking_column_ids, checking_index_scan_desc);

  executor::IndexScanExecutor *checking_index_scan_executor =
      new executor::IndexScanExecutor(&checking_index_scan_node, nullptr);

  checking_index_scan_executor->Init();

  // UPDATE BAL
  std::vector<oid_t> checking_update_column_ids = {1};

  planner::IndexScanPlan checking_update_index_scan_node(
      checking_table, nullptr, checking_update_column_ids,
      checking_index_scan_desc);

  executor::IndexScanExecutor *checking_update_index_scan_executor =
      new executor::IndexScanExecutor(&checking_update_index_scan_node,
                                      nullptr);

  TargetList checking_target_list;
  DirectMapList checking_direct_map_list;

  // Keep the first 1 columns unchanged
  for (oid_t col_itr = 0; col_itr < 1; ++col_itr) {
    checking_direct_map_list.emplace_back(col_itr,
                                          std::pair<oid_t, oid_t>(0, col_itr));
  }

  std::unique_ptr<const planner::ProjectInfo> checking_project_info(
      new planner::ProjectInfo(std::move(checking_target_list),
                               std::move(checking_direct_map_list)));
  planner::UpdatePlan checking_update_node(checking_table,
                                           std::move(checking_project_info));

  executor::UpdateExecutor *checking_update_executor =
      new executor::UpdateExecutor(&checking_update_node, nullptr);

  checking_update_executor->AddChild(checking_update_index_scan_executor);

  checking_update_executor->Init();
  /////////////////////////////////////////////////////////

  DepositCheckingPlans deposit_checking_plans;

  deposit_checking_plans.accounts_index_scan_executor_ = accounts_index_scan_executor;
  deposit_checking_plans.checking_index_scan_executor_ = checking_index_scan_executor;

  deposit_checking_plans.checking_update_index_scan_executor_ =
      checking_update_index_scan_executor;
  deposit_checking_plans.checking_update_executor_ = checking_update_executor;

  return deposit_checking_plans;
}



void GenerateDepositCheckingParams(ZipfDistribution &zipf, DepositCheckingParams &params) {

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  
  params.custid = zipf.GetNextNumber();

  params.increase = GetRandomInteger(1, 10);
}

bool RunDepositChecking(DepositCheckingPlans &deposit_checking_plans, DepositCheckingParams &params, UNUSED_ATTRIBUTE bool is_adhoc) {
  /*
     "DepositChecking": {
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?" # id 0

        "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
        " WHERE custid = ?" # id 0

        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING +
        "   SET bal = bal + ? " +
        " WHERE custid = ?" # id 0
      }
   */

  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  int custid0 = params.custid;

  /////////////////////////////////////////////////////////
  // BEGIN TRANSACTION
  /////////////////////////////////////////////////////////
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  deposit_checking_plans.SetContext(context.get());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // ACCOUNTS SELECTION
  /////////////////////////////////////////////////////////

  // "SELECT1 * FROM " + TABLENAME_ACCOUNTS + " WHERE custid = ?"
  LOG_TRACE("SELECT * FROM ACCOUNTS WHERE custid = %d", custid0);

  deposit_checking_plans.accounts_index_scan_executor_->ResetState();

  std::vector<Value> accounts_key_values;

  accounts_key_values.push_back(ValueFactory::GetIntegerValue(custid0));

  deposit_checking_plans.accounts_index_scan_executor_->SetValues(accounts_key_values);

  auto ga1_lists_values = ExecuteReadTest(deposit_checking_plans.accounts_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  // FIXME: should we comment out this?
  //  if (ga1_lists_values.size() != 1) {
  //    LOG_ERROR("ACCOUNTS return size incorrect : %lu",
  // ga1_lists_values.size());
  //    assert(false);
  //  }

  /////////////////////////////////////////////////////////
  // CHECKING
  /////////////////////////////////////////////////////////

  // Select
  LOG_TRACE("SELECT bal FROM checking WHERE custid = %d", custid0);

  deposit_checking_plans.checking_index_scan_executor_->ResetState();

  std::vector<Value> checking_key_values;

  checking_key_values.push_back(ValueFactory::GetIntegerValue(custid0));

  deposit_checking_plans.checking_index_scan_executor_->SetValues(checking_key_values);

  auto gc_lists_values = ExecuteReadTest(deposit_checking_plans.checking_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  // FIXME: should we comment out this?
  //  if (gc_lists_values.size() != 1) {
  //    LOG_ERROR("getACCOUNTS return size incorrect : %lu",
  //              gc_lists_values.size());
  //    assert(false);
  //  }

  auto bal_checking = gc_lists_values[0][0];

  // Update
  deposit_checking_plans.checking_update_index_scan_executor_->ResetState();

  deposit_checking_plans.checking_update_index_scan_executor_->SetValues(checking_key_values);

  TargetList checking_target_list;

  int increase = params.increase;
  double total = ValuePeeker::PeekDouble(bal_checking) + increase;

  Value checking_update_val = ValueFactory::GetDoubleValue(total);

  checking_target_list.emplace_back(
      1, expression::ExpressionUtil::ConstantValueFactory(checking_update_val));

  deposit_checking_plans.checking_update_executor_->SetTargetList(checking_target_list);

  ExecuteUpdateTest(deposit_checking_plans.checking_update_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  // transaction passed execution.
  assert(txn->GetResult() == Result::RESULT_SUCCESS);

  if (state.logging_type == LOGGING_TYPE_COMMAND) {
    if (is_adhoc == false) {
      txn->SetTransactionType(SMALLBANK_TRANSACTION_TYPE_DEPOSIT_CHECKING);
      txn->SetTransactionParam(&params);
    } else {
      txn->SetTransactionType(INVALID_TRANSACTION_TYPE);
    }
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