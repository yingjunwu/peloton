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
Amalgamate *GenerateAmalgamate(ZipfDistribution &zipf) {

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

  // UPDATE BAL
  std::vector<oid_t> savings_update_column_ids = {1};

  planner::IndexScanPlan savings_update_index_scan_node(
      savings_table, nullptr, savings_update_column_ids,
      savings_index_scan_desc);

  executor::IndexScanExecutor *savings_update_index_scan_executor =
      new executor::IndexScanExecutor(&savings_update_index_scan_node, nullptr);

  TargetList savings_target_list;
  DirectMapList savings_direct_map_list;

  // Keep the first 1 columns unchanged
  for (oid_t col_itr = 0; col_itr < 1; ++col_itr) {
    savings_direct_map_list.emplace_back(col_itr,
                                         std::pair<oid_t, oid_t>(0, col_itr));
  }

  std::unique_ptr<const planner::ProjectInfo> savings_project_info(
      new planner::ProjectInfo(std::move(savings_target_list),
                               std::move(savings_direct_map_list)));
  planner::UpdatePlan savings_update_node(savings_table,
                                          std::move(savings_project_info));

  executor::UpdateExecutor *savings_update_executor =
      new executor::UpdateExecutor(&savings_update_node, nullptr);

  savings_update_executor->AddChild(savings_update_index_scan_executor);

  savings_update_executor->Init();

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

  Amalgamate *amalgamate = new Amalgamate();

  amalgamate->accounts_index_scan_executor_ = accounts_index_scan_executor;

  amalgamate->savings_index_scan_executor_ = savings_index_scan_executor;

  amalgamate->checking_index_scan_executor_ = checking_index_scan_executor;

  amalgamate->savings_update_index_scan_executor_ =
      savings_update_index_scan_executor;

  amalgamate->savings_update_executor_ = savings_update_executor;

  amalgamate->checking_update_index_scan_executor_ =
      checking_update_index_scan_executor;

  amalgamate->checking_update_executor_ = checking_update_executor;

  // Set values
  amalgamate->SetValue(zipf);

  // Set txn's region cover
  amalgamate->SetRegionCover();

  return amalgamate;
}

/*
 * Set the parameters needed by execution. Set the W_ID, D_ID, C_ID, I_ID.
 * So when a txn has all of the parameters when enqueue
 */
void Amalgamate::SetValue(ZipfDistribution &zipf) {
  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  if (state.zipf_theta > 0) {
    custid_0_ = zipf.GetNextNumber();
    custid_1_ = zipf.GetNextNumber();
  } else {
    custid_0_ = GenerateAccountsId();
    custid_1_ = GenerateAccountsId();
  }

  // Take warehouse_id_ as the primary key
  primary_keys_.assign(1, custid_0_);
}

bool Amalgamate::Run() {
  /*
     "Amalgamate": {
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?" # id 0

        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?" # id 1

        "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
        " WHERE custid = ?" # id 0

        "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
        " WHERE custid = ?" # id 0

        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING +
        "   SET bal = 0.0 " +
        " WHERE custid = ?" # id 0

        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING +
        "   SET bal = 0.0 " +
        " WHERE custid = ?" # id 0

        "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS +
        "   SET bal = bal + ? " +
        " WHERE custid = ?" # total (bal + bal of id0) , id 1
      }
   */

  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  int custid0 = custid_0_;
  int custid1 = custid_1_;

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

  // "SELECT2 * FROM " + TABLENAME_ACCOUNTS + " WHERE custid = ?"
  LOG_TRACE("SELECT * FROM ACCOUNTS WHERE custid = %d", custid1);

  accounts_index_scan_executor_->ResetState();

  std::vector<Value> accounts_key_values2;

  accounts_key_values2.push_back(ValueFactory::GetIntegerValue(custid1));

  accounts_index_scan_executor_->SetValues(accounts_key_values2);

  auto ga2_lists_values = ExecuteReadTest(accounts_index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  // FIXME: should we comment out this?
  //  if (ga2_lists_values.size() != 1) {
  //    LOG_ERROR("ACCOUNTS return size incorrect : %lu",
  // ga2_lists_values.size());
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

  // FIXME: COMMENT OUT
  //  if (gs_lists_values.size() != 1) {
  //    LOG_ERROR("getACCOUNTS return size incorrect : %lu",
  //              gs_lists_values.size());
  //    assert(false);
  //  }

  auto bal_saving = gs_lists_values[0][0];

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

  auto bal_checking = gc_lists_values[0][0];

  // Total
  double total =
      GetDoubleFromValue(bal_saving) + GetDoubleFromValue(bal_checking);

  /////////////////////////////////////////////////////////
  // SAVINGS UPDATE (to 0)
  /////////////////////////////////////////////////////////
  savings_update_index_scan_executor_->ResetState();

  savings_update_index_scan_executor_->SetValues(savings_key_values);

  TargetList savings_target_list;

  Value savings_update_val = ValueFactory::GetIntegerValue(0);

  savings_target_list.emplace_back(
      1, expression::ExpressionUtil::ConstantValueFactory(savings_update_val));

  savings_update_executor_->SetTargetList(savings_target_list);

  ExecuteUpdateTest(savings_update_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  /////////////////////////////////////////////////////////
  // CHECKING UPDATE (to 0)
  /////////////////////////////////////////////////////////
  checking_update_index_scan_executor_->ResetState();

  checking_update_index_scan_executor_->SetValues(checking_key_values);

  TargetList checking_target_list;

  Value checking_update_val = ValueFactory::GetIntegerValue(0);

  checking_target_list.emplace_back(
      1, expression::ExpressionUtil::ConstantValueFactory(checking_update_val));

  checking_update_executor_->SetTargetList(checking_target_list);

  ExecuteUpdateTest(checking_update_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction();
    return false;
  }

  /////////////////////////////////////////////////////////
  // SAVING UPDATE (total)
  /////////////////////////////////////////////////////////
  savings_update_index_scan_executor_->ResetState();

  std::vector<Value> savings_key_values_id1;

  savings_key_values_id1.push_back(ValueFactory::GetIntegerValue(custid1));

  savings_update_index_scan_executor_->SetValues(savings_key_values_id1);

  TargetList savings_target_list_id1;

  Value savings_update_val_id1 = ValueFactory::GetIntegerValue(total);

  savings_target_list_id1.emplace_back(
      1,
      expression::ExpressionUtil::ConstantValueFactory(savings_update_val_id1));

  savings_update_executor_->SetTargetList(savings_target_list_id1);

  ExecuteUpdateTest(savings_update_executor_);

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
