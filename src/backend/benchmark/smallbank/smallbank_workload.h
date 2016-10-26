//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/smallbank/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/benchmark_common.h"
#include "backend/benchmark/smallbank/smallbank_loader.h"
#include "backend/benchmark/smallbank/smallbank_configuration.h"


#include "backend/benchmark/smallbank/smallbank_amalgamate.h"
#include "backend/benchmark/smallbank/smallbank_balance.h"
#include "backend/benchmark/smallbank/smallbank_deposit_checking.h"
#include "backend/benchmark/smallbank/smallbank_transact_saving.h"
#include "backend/benchmark/smallbank/smallbank_write_check.h"

#include "backend/executor/abstract_executor.h"
#include "backend/storage/data_table.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace benchmark {
namespace smallbank {

extern configuration state;

void RunWorkload();

/////////////////////////////////////////////////////////

std::vector<std::vector<Value>>
ExecuteReadTest(executor::AbstractExecutor* executor);

void ExecuteUpdateTest(executor::AbstractExecutor* executor);

void ExecuteDeleteTest(executor::AbstractExecutor* executor);

}  // namespace smallbank
}  // namespace benchmark
}  // namespace peloton
