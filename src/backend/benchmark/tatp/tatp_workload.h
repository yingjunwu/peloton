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
#include "backend/benchmark/tatp/tatp_loader.h"
#include "backend/benchmark/tatp/tatp_get_new_destination.h"
#include "backend/benchmark/tatp/tatp_get_subscriber_data.h"
#include "backend/benchmark/tatp/tatp_get_access_data.h"
#include "backend/benchmark/tatp/tatp_update_subscriber_data.h"
#include "backend/benchmark/tatp/tatp_update_location.h"
#include "backend/benchmark/tatp/tatp_insert_call_forwarding.h"
#include "backend/benchmark/tatp/tatp_delete_call_forwarding.h"
#include "backend/benchmark/tatp/tatp_configuration.h"
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
namespace tatp {

extern configuration state;
extern int RUNNING_REF_THRESHOLD;

void RunWorkload();

/////////////////////////////////////////////////////////
// TRANSACTION TYPES
/////////////////////////////////////////////////////////

size_t GenerateSubscriberId();
size_t GenerateAiTypeId();
size_t GenerateSfTypeId();
size_t GenerateStartTime();
size_t GenerateEndTime();
size_t GenerateAmount();

/////////////////////////////////////////////////////////
void GenerateAndCacheQuery(ZipfDistribution& zipf);
void GenerateALLAndCache(ZipfDistribution& zipf);
// void GenerateALLAndCache(bool new_order);
bool EnqueueCachedUpdate(
    std::chrono::system_clock::time_point& delay_start_time, oid_t thread_id);
std::unordered_map<int, ClusterRegion> ClusterAnalysis();

/////////////////////////////////////////////////////////

std::vector<std::vector<Value>> ExecuteReadTest(
    executor::AbstractExecutor* executor);

void ExecuteUpdateTest(executor::AbstractExecutor* executor);
void ExecuteInsertTest(executor::AbstractExecutor* executor);
void ExecuteDeleteTest(executor::AbstractExecutor* executor);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
