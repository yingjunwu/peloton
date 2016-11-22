//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ycsb_workload.cpp
//
// Identification: benchmark/ycsb/ycsb_workload.cpp
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

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/expression_util.h"

#include "backend/index/index_factory.h"

#include "backend/logging/durability_factory.h"
#include "backend/logging/worker_context.h"
#include "backend/logging/worker_context.h"

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


/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

oid_t *abort_counts;
oid_t *commit_counts;
double *commit_latencies;
LatSummary *commit_lat_summaries;
logging::WorkerContext **log_worker_ctxs;

oid_t *ro_abort_counts;
oid_t *ro_commit_counts;

oid_t scan_count;
double scan_avg_latency;

void RunBackend(oid_t thread_id) {
  PinToCore(thread_id);

  auto &log_manager = logging::DurabilityFactory::GetLoggerInstance();
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();

  epoch_manager.RegisterTxnWorker(false);

  log_manager.RegisterWorker();
  log_worker_ctxs[thread_id] = logging::tl_worker_ctx;

  auto update_ratio = state.update_ratio;
  auto operation_count = state.operation_count;

  oid_t &execution_count_ref = abort_counts[thread_id];
  oid_t &transaction_count_ref = commit_counts[thread_id];
  double &commit_latency_ref = commit_latencies[thread_id];
  LatSummary &commit_lat_summary_ref = commit_lat_summaries[thread_id];

  ZipfDistribution zipf(state.scale_factor * 1000 - 1,
                        state.zipf_theta);

  fast_random rng(rand());

  MixedPlans mixed_plans = PrepareMixedPlan();
  // backoff
  uint32_t backoff_shifts = 0;

  while (true) {
    if (is_running == false) {
      break;
    }
    while (RunMixed(mixed_plans, zipf, rng, update_ratio, operation_count, false) == false) {
      if (is_running == false) {
        break;
      }
      execution_count_ref++;
      // backoff
      if (state.run_backoff) {
        if (backoff_shifts < 63) {
          ++backoff_shifts;
        }
        uint64_t spins = 1UL << backoff_shifts;
        spins *= 100;
        while (spins) {
          _mm_pause();
          --spins;
        }
      }
    }
    backoff_shifts >>= 1;

    transaction_count_ref++;
  }

  if (logging::DurabilityFactory::GetLoggingType() != LOGGING_TYPE_INVALID) {
    commit_latency_ref = logging::tl_worker_ctx->txn_summary.GetAverageLatencyInMs();
    if (thread_id == 0) {
      commit_lat_summary_ref = logging::tl_worker_ctx->txn_summary.GetLatSummary();
      if (logging::DurabilityFactory::GenerateDetailedCsv() == true) {
        logging::tl_worker_ctx->txn_summary.GenerateDetailedCsv();
      }
    }
  }
  
  log_manager.DeregisterWorker();

}

void RunReadOnlyBackend(oid_t thread_id) {
  PinToCore(thread_id);
  bool is_read_only = state.declared;
  double update_ratio = 0;
  auto operation_count = state.operation_count;
  // double &commit_latency_ref = commit_latencies[thread_id];
  // LatSummary &commit_lat_summary_ref = commit_lat_summaries[thread_id];

  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.RegisterTxnWorker(is_read_only);

  if (is_read_only == false) {
    auto &log_manager = logging::DurabilityFactory::GetLoggerInstance();
    log_manager.RegisterWorker();
  } else {
    auto SLEEP_TIME = std::chrono::milliseconds(300);
    std::this_thread::sleep_for(SLEEP_TIME);
  }

  log_worker_ctxs[thread_id] = logging::tl_worker_ctx;
  oid_t &ro_execution_count_ref = ro_abort_counts[thread_id];
  oid_t &ro_transaction_count_ref = ro_commit_counts[thread_id];

  ZipfDistribution zipf(state.scale_factor * 1000 - 1,
                        state.zipf_theta);

  fast_random rng(rand());

  MixedPlans mixed_plans = PrepareMixedPlan();
  // backoff
  uint32_t backoff_shifts = 0;
  while (true) {
    if (is_running == false) {
      break;
    }

    if (state.ro_sleep_between_txn != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(state.ro_sleep_between_txn));
    }

    while (RunMixed(mixed_plans, zipf, rng, update_ratio, operation_count, is_read_only, state.scan_mock_duration) == false) {
      if (is_running == false) {
        break;
      }
      ro_execution_count_ref++;
      // backoff
      if (state.run_backoff) {
        if (backoff_shifts < 63) {
          ++backoff_shifts;
        }
        uint64_t spins = 1UL << backoff_shifts;
        spins *= 100;
        while (spins) {
          _mm_pause();
          --spins;
        }
      }
    }
    backoff_shifts >>= 1;

    ro_transaction_count_ref++;
  }

  if (is_read_only == false) {
    auto &log_manager = logging::DurabilityFactory::GetLoggerInstance();
    log_manager.DeregisterWorker();
  }
}

void RunScanBackend(oid_t thread_id) {
  PinToCore(thread_id);
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.RegisterTxnWorker(true);


  bool slept = false;
  auto SLEEP_TIME = std::chrono::milliseconds(500);

  // backoff
  uint32_t backoff_shifts = 0;
  while (true) {
    if (is_running == false) {
      break;
    }
    if (!slept) {
      slept = true;
      std::this_thread::sleep_for(SLEEP_TIME);
    }
    std::chrono::steady_clock::time_point start_time;
    if (thread_id == 0) {
      start_time = std::chrono::steady_clock::now();
    }
    while (RunScan() == false) {
      if (is_running == false) {
        break;
      }
      // backoff
      if (state.run_backoff) {
        if (backoff_shifts < 63) {
          ++backoff_shifts;
        }
        uint64_t spins = 1UL << backoff_shifts;
        spins *= 100;
        while (spins) {
          _mm_pause();
          --spins;
        }
      }
    }
    if (thread_id == 0) {
      std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
      double diff = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
      scan_avg_latency = (scan_avg_latency * scan_count + diff) / (scan_count + 1);
      scan_count++;
    }
    backoff_shifts >>= 1;
  }

}


void RunWorkload() {
  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  oid_t num_ro_threads = state.ro_backend_count;
  oid_t num_scan_threads = state.scan_backend_count;

  abort_counts = new oid_t[num_threads];
  memset(abort_counts, 0, sizeof(oid_t) * num_threads);

  commit_counts = new oid_t[num_threads];
  memset(commit_counts, 0, sizeof(oid_t) * num_threads);

  commit_latencies = new double[num_threads];
  memset(commit_latencies, 0, sizeof(double) * num_threads);

  commit_lat_summaries = new LatSummary[num_threads];

  ro_abort_counts = new oid_t[num_threads];
  memset(ro_abort_counts, 0, sizeof(oid_t) * num_threads);

  ro_commit_counts = new oid_t[num_threads];
  memset(ro_commit_counts, 0, sizeof(oid_t) * num_threads);

  log_worker_ctxs = new logging::WorkerContext *[num_threads];

  scan_count = 0;
  scan_avg_latency = 0.0;

  size_t snapshot_round = (size_t)(state.duration / state.snapshot_duration);

  oid_t **abort_counts_snapshots = new oid_t *[snapshot_round];
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    abort_counts_snapshots[round_id] = new oid_t[num_threads];
  }

  oid_t **commit_counts_snapshots = new oid_t *[snapshot_round];
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    commit_counts_snapshots[round_id] = new oid_t[num_threads];
  }

  // Launch a group of threads
  // thread count settings should pass the parameter validation
  oid_t rw_backend_count = num_threads - num_ro_threads - num_scan_threads;
  oid_t thread_itr;

  for (thread_itr = 0; thread_itr < rw_backend_count; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
  }

  for (; thread_itr < num_ro_threads + rw_backend_count; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunReadOnlyBackend, thread_itr)));
  }

  for (; thread_itr < num_ro_threads + rw_backend_count + num_scan_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunScanBackend, thread_itr)));
  }

  //////////////////////////////////////
  oid_t last_tile_group_id = 0;
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(int(state.snapshot_duration * 1000)));
    memcpy(abort_counts_snapshots[round_id], abort_counts,
           sizeof(oid_t) * num_threads);
    memcpy(commit_counts_snapshots[round_id], commit_counts,
           sizeof(oid_t) * num_threads);

    auto& manager = catalog::Manager::GetInstance();
    
    oid_t current_tile_group_id = manager.GetLastTileGroupId();
    if (round_id != 0) {
      state.snapshot_memory.push_back(current_tile_group_id - last_tile_group_id);
      // Get the latency of thread 0
      if (logging::DurabilityFactory::GetLoggingType() != LOGGING_TYPE_INVALID) {
        state.snapshot_latency.push_back(log_worker_ctxs[0]->txn_summary.GetRecentAvgLatency());
      }
    }
    last_tile_group_id = current_tile_group_id;
  }
  state.snapshot_memory.push_back(state.snapshot_memory.at(state.snapshot_memory.size() - 1));
  // Get the latency of thread 0
  if (logging::DurabilityFactory::GetLoggingType() != LOGGING_TYPE_INVALID) {
    state.snapshot_latency.push_back(state.snapshot_latency.back());
  } else {
    state.snapshot_latency.push_back(0);    
  }

  is_running = false;
  // Join the threads with the main thread
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
  // calculate the throughput and abort rate for the first round.
  oid_t total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_snapshots[0][i];
  }

  oid_t total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_snapshots[0][i];
  }

  state.snapshot_throughput.push_back(total_commit_count * 1.0 /
                                      state.snapshot_duration);
  state.snapshot_abort_rate.push_back(total_abort_count * 1.0 /
                                      total_commit_count);

  // calculate the throughput and abort rate for the remaining rounds.
  for (size_t round_id = 0; round_id < snapshot_round - 1; ++round_id) {
    total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_commit_count += commit_counts_snapshots[round_id + 1][i] -
                            commit_counts_snapshots[round_id][i];
    }

    total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_abort_count += abort_counts_snapshots[round_id + 1][i] -
                           abort_counts_snapshots[round_id][i];
    }

    state.snapshot_throughput.push_back(total_commit_count * 1.0 /
                                        state.snapshot_duration);
    state.snapshot_abort_rate.push_back(total_abort_count * 1.0 /
                                        total_commit_count);
  }

  //////////////////////////////////////////////////
  // calculate the aggregated throughput and abort rate.
  total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_snapshots[snapshot_round - 1][i];
  }

  total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_snapshots[snapshot_round - 1][i];
  }

  state.throughput = total_commit_count * 1.0 / state.duration;
  state.abort_rate = total_abort_count * 1.0 / total_commit_count;

  //////////////////////////////////////////////////
  oid_t total_ro_commit_count = 0;
  oid_t total_to_abort_count = 0;

  if (num_ro_threads != 0) {
    for (size_t i = 0; i < num_ro_threads; ++i) {
      total_ro_commit_count += ro_commit_counts[i + rw_backend_count];
    }

    for (size_t i = 0; i < num_ro_threads; ++i) {
      total_to_abort_count += ro_abort_counts[i + rw_backend_count];
    }

    state.ro_throughput = total_ro_commit_count * 1.0 / state.duration;
    state.ro_abort_rate = total_to_abort_count * 1.0 / total_ro_commit_count;
  }
  
  //////////////////////////////////////////////////

  state.scan_latency = scan_avg_latency;

  //////////////////////////////////////////////////

  state.commit_latency = 0.0;
  for (size_t i = 0; i < num_threads; ++i) {
    // Weighted avg
    state.commit_latency += (commit_latencies[i] * (commit_counts_snapshots[snapshot_round - 1][i] * 1.0 / total_commit_count));
  }

  //////////////////////////////////////////////////

  state.latency_summary = commit_lat_summaries[0];

  //////////////////////////////////////////////////

  // cleanup everything.
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    delete[] abort_counts_snapshots[round_id];
    abort_counts_snapshots[round_id] = nullptr;
  }

  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    delete[] commit_counts_snapshots[round_id];
    commit_counts_snapshots[round_id] = nullptr;
  }

  delete[] abort_counts_snapshots;
  abort_counts_snapshots = nullptr;
  delete[] commit_counts_snapshots;
  commit_counts_snapshots = nullptr;

  delete[] abort_counts;
  abort_counts = nullptr;
  delete[] commit_counts;
  commit_counts = nullptr;
  delete[] commit_latencies;
  commit_latencies = nullptr;
  delete[] commit_lat_summaries;
  commit_lat_summaries = nullptr;

  delete[] ro_abort_counts;
  ro_abort_counts = nullptr;
  delete[] ro_commit_counts;
  ro_commit_counts = nullptr;

  delete [] log_worker_ctxs;
  log_worker_ctxs = nullptr;
}


/////////////////////////////////////////////////////////
// HARNESS
/////////////////////////////////////////////////////////


std::vector<std::vector<Value>>
ExecuteReadTest(executor::AbstractExecutor* executor) {

  std::vector<std::vector<Value>> logical_tile_values;

  // Execute stuff
  while (executor->Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());

    // is this possible?
    if(result_tile == nullptr)
      break;

    auto column_count = result_tile->GetColumnCount();
    LOG_TRACE("result column count = %d\n", (int)column_count);

    for (oid_t tuple_id : *result_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
                                                                  tuple_id);
      std::vector<Value> tuple_values;
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
         auto value = cur_tuple.GetValue(column_itr);
         tuple_values.push_back(value);
      }

      // Move the tuple list
      logical_tile_values.push_back(std::move(tuple_values));
    }
  }

  return std::move(logical_tile_values);
}

void ExecuteUpdateTest(executor::AbstractExecutor* executor) {
  
  // Execute stuff
  while (executor->Execute() == true);
}


}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
