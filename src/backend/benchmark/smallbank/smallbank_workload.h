//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/tpcc/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/benchmark_common.h"
#include "backend/benchmark/smallbank/smallbank_loader.h"
#include "backend/benchmark/smallbank/smallbank_amalgamate.h"
#include "backend/benchmark/smallbank/smallbank_balance.h"
#include "backend/benchmark/smallbank/smallbank_deposit_checking.h"
#include "backend/benchmark/smallbank/smallbank_transact_saving.h"
#include "backend/benchmark/smallbank/smallbank_write_check.h"
#include "backend/benchmark/smallbank/smallbank_configuration.h"
#include "backend/executor/abstract_executor.h"
#include "backend/storage/data_table.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/concurrency/transaction_scheduler.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace benchmark {
namespace smallbank {

extern configuration state;
extern int RUNNING_REF_THRESHOLD;

void RunWorkload();

/////////////////////////////////////////////////////////
// TRANSACTION TYPES
/////////////////////////////////////////////////////////

size_t GenerateAccountsId(const size_t& thread_id);
size_t GenerateAccountsId();
size_t GenerateAmount();

/////////////////////////////////////////////////////////
void GenerateAndCacheQuery(ZipfDistribution& zipf);
void GenerateALLAndCache(ZipfDistribution& zipf);
// void GenerateALLAndCache(bool new_order);
bool EnqueueCachedUpdate(
    std::chrono::system_clock::time_point& delay_start_time);
std::unordered_map<int, ClusterRegion> ClusterAnalysis();

/////////////////////////////////////////////////////////

std::vector<std::vector<Value>> ExecuteReadTest(
    executor::AbstractExecutor* executor);

void ExecuteUpdateTest(executor::AbstractExecutor* executor);

void ExecuteDeleteTest(executor::AbstractExecutor* executor);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
