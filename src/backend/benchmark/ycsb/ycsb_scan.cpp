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

#include "backend/benchmark/ycsb/ycsb_workload.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"

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
#include "backend/executor/seq_scan_executor.h"

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
namespace ycsb {

bool RunScan() {
  if (state.scan_mock_duration != 0) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    txn_manager.BeginReadonlyTransaction();
    std::this_thread::sleep_for(std::chrono::seconds(state.scan_mock_duration));
    txn_manager.EndReadonlyTransaction();
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));
  
  // Column ids to be added to logical tile after scan.

  oid_t begin_read_column_id = 1;
  oid_t end_read_column_id = begin_read_column_id + state.read_column_count - 1;
  
  std::vector<oid_t> read_column_ids;
  for (oid_t col_itr = begin_read_column_id; col_itr <= end_read_column_id; col_itr++) {
    read_column_ids.push_back(col_itr);
  }

  // std::vector<oid_t> column_ids;
  // oid_t column_count = state.column_count + 1;

  // for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
  //   column_ids.push_back(col_itr);
  // }
  
  // Create plan node.
  planner::SeqScanPlan node(user_table, nullptr, read_column_ids);
  
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  
  auto txn = txn_manager.BeginReadonlyTransaction();

  executor::SeqScanExecutor executor(&node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  executor.Init();
  auto ret_result = ExecuteReadTest(&executor);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  if (ret_result.size() != 1) {
    //LOG_ERROR("result incorrect: ret_result size = %lu", ret_result.size());
    assert(false);
  }

  if ((int) ret_result.size() != state.scale_factor * 1000) {
    LOG_ERROR("Read only result in correct: table_size = %d, res_size = %d",
    state.scale_factor * 1000, (int) ret_result.size());
    assert(false);
  }

  // transaction passed execution.
  assert(txn->GetResult() == Result::RESULT_SUCCESS);
  auto result = txn_manager.EndReadonlyTransaction();
  // auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
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