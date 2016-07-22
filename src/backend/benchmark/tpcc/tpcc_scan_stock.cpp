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


#include "backend/benchmark/tpcc/tpcc_workload.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"

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
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/seq_scan_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/container_tuple.h"

#include "backend/planner/seq_scan_plan.h"

#include "backend/logging/log_manager.h"
#include "backend/planner/abstract_plan.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"


namespace peloton {
namespace benchmark {
namespace tpcc {

bool RunScanStock() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginReadonlyTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
    new executor::ExecutorContext(txn));

  LOG_TRACE("getStockCount: SELECT S_QUANTITY FROM STOCK");

  //////////////////////////////////////////////////////////////
  std::vector<oid_t> stock_column_ids = {COL_IDX_S_QUANTITY};
  planner::SeqScanPlan stock_seq_scan_node(stock_table, nullptr, stock_column_ids);

  executor::SeqScanExecutor stock_seq_scan_executor(&stock_seq_scan_node, context.get());

  stock_seq_scan_executor.Init();

  auto ret_result = ExecuteReadTest(&stock_seq_scan_executor);

  txn_manager.EndReadonlyTransaction();

  return true;
}

}
}
}