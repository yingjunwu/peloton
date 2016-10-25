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
TransactSaving *GenerateTransactSaving(ZipfDistribution &zipf) {

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
  // PLAN FOR SAVING
  /////////////////////////////////////////////////////////
  std::vector<oid_t> saving_key_column_ids = {0};  // CUSTID
  std::vector<ExpressionType> saving_expr_types;
  saving_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> saving_key_values;

  auto saving_pkey_index =
      savings_table->GetIndexWithOid(savings_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc saving_index_scan_desc(
      saving_pkey_index, saving_key_column_ids, saving_expr_types,
      saving_key_values, runtime_keys);

  // std::vector<oid_t> warehouse_column_ids = {1, 2, 3, 4, 5, 6, 8};
  std::vector<oid_t> saving_column_ids = {1};  // select BAL from

  planner::IndexScanPlan saving_index_scan_node(
      savings_table, nullptr, saving_column_ids, saving_index_scan_desc);

  executor::IndexScanExecutor *saving_index_scan_executor =
      new executor::IndexScanExecutor(&saving_index_scan_node, nullptr);

  saving_index_scan_executor->Init();

  // UPDATE BAL
  std::vector<oid_t> saving_update_column_ids = {1};

  planner::IndexScanPlan saving_update_index_scan_node(
      savings_table, nullptr, saving_update_column_ids, saving_index_scan_desc);

  executor::IndexScanExecutor *saving_update_index_scan_executor =
      new executor::IndexScanExecutor(&saving_update_index_scan_node, nullptr);

  TargetList saving_target_list;
  DirectMapList saving_direct_map_list;

  // Keep the first 1 column unchanged
  for (oid_t col_itr = 0; col_itr < 1; ++col_itr) {
    saving_direct_map_list.emplace_back(col_itr,
                                        std::pair<oid_t, oid_t>(0, col_itr));
  }

  std::unique_ptr<const planner::ProjectInfo> saving_project_info(
      new planner::ProjectInfo(std::move(saving_target_list),
                               std::move(saving_direct_map_list)));
  planner::UpdatePlan saving_update_node(savings_table,
                                         std::move(saving_project_info));

  executor::UpdateExecutor *saving_update_executor =
      new executor::UpdateExecutor(&saving_update_node, nullptr);

  saving_update_executor->AddChild(saving_update_index_scan_executor);

  saving_update_executor->Init();
  /////////////////////////////////////////////////////////

  TransactSaving *ts = new TransactSaving();

  ts->accounts_index_scan_executor_ = accounts_index_scan_executor;

  ts->saving_index_scan_executor_ = saving_index_scan_executor;
  ts->saving_update_index_scan_executor_ = saving_update_index_scan_executor;
  ts->saving_update_executor_ = saving_update_executor;

  // Set values
  ts->SetValue(zipf);

  // Set txn's region cover
  ts->SetRegionCover();

  return ts;
}

/*
 * Set the parameters needed by execution. Set the W_ID, D_ID, C_ID, I_ID.
 * So when a txn has all of the parameters when enqueue
 */
void TransactSaving::SetValue(ZipfDistribution &zipf) {
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

bool TransactSaving::Run() {
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

  /////////////////////////////////////////////////////////
  // CHECKING
  /////////////////////////////////////////////////////////

  // Select
  LOG_TRACE("SELECT bal FROM checking WHERE custid = %d", custid0);

  saving_index_scan_executor_->ResetState();

  std::vector<Value> saving_key_values;

  saving_key_values.push_back(ValueFactory::GetIntegerValue(custid0));

  saving_index_scan_executor_->SetValues(saving_key_values);

  auto gc_lists_values = ExecuteReadTest(saving_index_scan_executor_);

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

  auto bal_saving = gc_lists_values[0][0];

  // Update
  saving_update_index_scan_executor_->ResetState();

  saving_update_index_scan_executor_->SetValues(saving_key_values);

  // Update the 2th column (bal)

  TargetList saving_target_list;

  int increase = GenerateAmount();
  double total = GetDoubleFromValue(bal_saving) + increase;

  Value saving_update_val = ValueFactory::GetDoubleValue(total);

  saving_target_list.emplace_back(
      1, expression::ExpressionUtil::ConstantValueFactory(saving_update_val));

  saving_update_executor_->SetTargetList(saving_target_list);

  ExecuteUpdateTest(saving_update_executor_);

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
