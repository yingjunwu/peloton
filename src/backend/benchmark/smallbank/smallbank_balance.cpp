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

/*
 * This function new a Amalgamate, so remember to delete it
 */
Balance *GenerateBalance(ZipfDistribution &zipf) {

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
  // PLAN FOR SAVINGS
  /////////////////////////////////////////////////////////
  std::vector<oid_t> savings_key_column_ids;
  std::vector<ExpressionType> savings_expr_types;
  savings_key_column_ids.push_back(0);  // CUSTID
  savings_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> savings_key_values;

  auto savings_pkey_index =
      savings_table->GetIndexWithOid(savings_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc savings_index_scan_desc(
      savings_pkey_index, savings_key_column_ids, savings_expr_types,
      savings_key_values, runtime_keys);

  // std::vector<oid_t> warehouse_column_ids = {1, 2, 3, 4, 5, 6, 8};
  std::vector<oid_t> savings_column_ids = {1};  // select BAL FROM

  planner::IndexScanPlan savings_index_scan_node(
      savings_table, nullptr, savings_column_ids, savings_index_scan_desc);

  executor::IndexScanExecutor *savings_index_scan_executor =
      new executor::IndexScanExecutor(&savings_index_scan_node, nullptr);

  savings_index_scan_executor->Init();

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

  /////////////////////////////////////////////////////////

  Balance *balance = new Balance();

  balance->accounts_index_scan_executor_ = accounts_index_scan_executor;

  balance->savings_index_scan_executor_ = savings_index_scan_executor;

  balance->checking_index_scan_executor_ = checking_index_scan_executor;

  // Set values
  balance->SetValue(zipf);

  // Set txn's region cover
  balance->SetRegionCover();

  return balance;
}

/*
 * Set the parameters needed by execution. Set the W_ID, D_ID, C_ID, I_ID.
 * So when a txn has all of the parameters when enqueue
 */
void Balance::SetValue(ZipfDistribution &zipf) {
  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  if (state.zipf_theta > 0) {
    custid_ = zipf.GetNextNumber();
  } else {
    custid_ = GenerateAccountsId();
  }

  // Take warehouse_id_ as the primary key
  primary_keys_.assign(1, custid_);
}

bool Balance::Run() {
  /*
     "Balance": {
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?" # id 0

        "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
        " WHERE custid = ?" # id 0

        "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
        " WHERE custid = ?" # id 0
      }
   */

  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  int custid0 = custid_;

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
  LOG_TRACE("SELECT * FROM ACCOUNTS WHERE custid = %d", custid0);

  accounts_index_scan_executor_->ResetState();

  std::vector<Value> accounts_key_values;

  accounts_key_values.push_back(ValueFactory::GetIntegerValue(custid0));

  accounts_index_scan_executor_->SetValues(accounts_key_values);

  auto ga1_lists_values = ExecuteReadTest(accounts_index_scan_executor_);

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
  // SAVINGS SELECTION (id 0)
  /////////////////////////////////////////////////////////
  LOG_TRACE("SELECT bal FROM savings WHERE custid = %d", custid0);

  savings_index_scan_executor_->ResetState();

  std::vector<Value> savings_key_values;

  savings_key_values.push_back(ValueFactory::GetIntegerValue(custid0));

  savings_index_scan_executor_->SetValues(savings_key_values);

  auto gs_lists_values = ExecuteReadTest(savings_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  // FIXME: should we comment out this?
  //  if (gs_lists_values.size() != 1) {
  //    LOG_ERROR("getACCOUNTS return size incorrect : %lu",
  //              gs_lists_values.size());
  //    assert(false);
  //  }

  // auto bal_saving = gs_lists_values[0][0];

  /////////////////////////////////////////////////////////
  // CHECKING SELECTION (id 0)
  /////////////////////////////////////////////////////////
  LOG_TRACE("SELECT * FROM checking WHERE custid = %d", custid1);

  checking_index_scan_executor_->ResetState();

  std::vector<Value> checking_key_values;

  checking_key_values.push_back(ValueFactory::GetIntegerValue(custid0));

  checking_index_scan_executor_->SetValues(checking_key_values);

  auto gc_lists_values = ExecuteReadTest(checking_index_scan_executor_);

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
