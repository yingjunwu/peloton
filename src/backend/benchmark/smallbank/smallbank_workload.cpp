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

#include "backend/logging/durability_factory.h"

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

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////


#define FREQUENCY_AMALGAMATE 0.15
#define FREQUENCY_BALANCE 0.15
#define FREQUENCY_DEPOSIT_CHECKING 0.15
#define FREQUENCY_SEND_PAYMENT 0.25
#define FREQUENCY_TRANSACT_SAVINGS 0.15
#define FREQUENCY_WRITE_CHECK 0.15

volatile bool is_running = true;

oid_t *abort_counts;
oid_t *commit_counts;
double *commit_latencies;
LatSummary *commit_lat_summaries;


void RunBackend(oid_t thread_id) {
  PinToCore(thread_id);

  auto &log_manager = logging::DurabilityFactory::GetLoggerInstance();
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();

  epoch_manager.RegisterTxnWorker(false);
  log_manager.RegisterWorker();

  oid_t &execution_count_ref = abort_counts[thread_id];
  oid_t &transaction_count_ref = commit_counts[thread_id];
  double &commit_latency_ref = commit_latencies[thread_id];
  LatSummary &lat_summary_ref = commit_lat_summaries[thread_id];

  AmalgamatePlans amalgamate_plans = PrepareAmalgamatePlan();
  BalancePlans balance_plans = PrepareBalancePlan();
  DepositCheckingPlans deposit_checking_plans = PrepareDepositCheckingPlan();
  TransactSavingPlans transact_saving_plans = PrepareTransactSavingPlan();
  WriteCheckPlans write_check_plans = PrepareWriteCheckPlan();
  
  // backoff
  uint32_t backoff_shifts = 0;

  ZipfDistribution zipf(state.account_count - 1,
                        state.zipf_theta);

  fast_random rng(rand());

  while (true) {

    if (is_running == false) {
      break;
    }
    
    auto rng_val = rng.next_uniform();
    if (rng_val <= FREQUENCY_AMALGAMATE) {
      bool is_adhoc = false;
      if (rng.next_uniform() < state.adhoc_ratio) {
        is_adhoc = true;
      } else {
        is_adhoc = false;
      }

      AmalgamateParams amalgamate_params;
      GenerateAmalgamateParams(zipf, amalgamate_params);
       while (RunAmalgamate(amalgamate_plans, amalgamate_params, is_adhoc) == false) {
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

     } 
     else if (rng_val <= FREQUENCY_AMALGAMATE + FREQUENCY_BALANCE) {
       bool is_adhoc = false;
       if (rng.next_uniform() < state.adhoc_ratio) {
         is_adhoc = true;
       } else {
         is_adhoc = false;
       }
       BalanceParams balance_params;
       GenerateBalanceParams(zipf, balance_params);
       while (RunBalance(balance_plans, balance_params, is_adhoc) == false) {
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

     } 
     else if (rng_val <= FREQUENCY_AMALGAMATE + FREQUENCY_BALANCE + FREQUENCY_DEPOSIT_CHECKING) {
       bool is_adhoc = false;
       if (rng.next_uniform() < state.adhoc_ratio) {
         is_adhoc = true;
       } else {
         is_adhoc = false;
       }
       DepositCheckingParams deposit_checking_params;
       GenerateDepositCheckingParams(zipf, deposit_checking_params);
       while (RunDepositChecking(deposit_checking_plans, deposit_checking_params, is_adhoc) == false) {
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
       
     } 
     // else if (rng_val <= FREQUENCY_AMALGAMATE + FREQUENCY_BALANCE + FREQUENCY_DEPOSIT_CHECKING + FREQUENCY_SEND_PAYMENT) {
     //   bool is_adhoc = false;
     //   if (rng.next_uniform() < state.adhoc_ratio) {
     //     is_adhoc = true;
     //   } else {
     //     is_adhoc = false;
     //   }
     //   SendPaymentParams send_payment_params;
     //   GenerateSendPaymentParams(zipf, send_payment_params);
     //   while (RunSendPayment(send_payment_plans, send_payment_params, is_adhoc) == false) {
     //      if (is_running == false) {
     //        break;
     //      }
     //     execution_count_ref++;

     //    // backoff
     //    if (state.run_backoff) {
     //      if (backoff_shifts < 63) {
     //        ++backoff_shifts;
     //      }
     //      uint64_t spins = 1UL << backoff_shifts;
     //      spins *= 100;
     //      while (spins) {
     //        _mm_pause();
     //        --spins;
     //      }
     //    }
     //   }
       
     // } 
     else if (rng_val <= FREQUENCY_AMALGAMATE + FREQUENCY_BALANCE + FREQUENCY_DEPOSIT_CHECKING + FREQUENCY_SEND_PAYMENT + FREQUENCY_TRANSACT_SAVINGS) {
       bool is_adhoc = false;
       if (rng.next_uniform() < state.adhoc_ratio) {
         is_adhoc = true;
       } else {
         is_adhoc = false;
       }
       TransactSavingParams transact_saving_params;
       GenerateTransactSavingParams(zipf, transact_saving_params);
       while (RunTransactSaving(transact_saving_plans, transact_saving_params, is_adhoc) == false) {
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
       
     } else {
       bool is_adhoc = false;
       if (rng.next_uniform() < state.adhoc_ratio) {
         is_adhoc = true;
       } else {
         is_adhoc = false;
       }
       WriteCheckParams write_check_params;
       GenerateWriteCheckParams(zipf, write_check_params);
       while (RunWriteCheck(write_check_plans, write_check_params, is_adhoc) == false) {
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
       
     }

    backoff_shifts >>= 1;
    transaction_count_ref++;

  }

  if (logging::DurabilityFactory::GetLoggingType() != LOGGING_TYPE_INVALID) {
    commit_latency_ref = logging::tl_worker_ctx->txn_summary.GetAverageLatencyInMs();
    if (thread_id == 0) {
      lat_summary_ref = logging::tl_worker_ctx->txn_summary.GetLatSummary();
    }
  }

  log_manager.DeregisterWorker();
}

void RunWorkload() {

  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  
  abort_counts = new oid_t[num_threads];
  memset(abort_counts, 0, sizeof(oid_t) * num_threads);

  commit_counts = new oid_t[num_threads];
  memset(commit_counts, 0, sizeof(oid_t) * num_threads);

  commit_latencies = new double[num_threads];
  memset(commit_latencies, 0, sizeof(double) * num_threads);

  commit_lat_summaries = new LatSummary[num_threads];

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
  oid_t thread_itr;

  for (thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
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
    }
    last_tile_group_id = current_tile_group_id;
  }
  state.snapshot_memory.push_back(state.snapshot_memory.at(state.snapshot_memory.size() - 1));

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

  state.snapshot_throughput
      .push_back(total_commit_count * 1.0 / state.snapshot_duration);
  state.snapshot_abort_rate
      .push_back(total_abort_count * 1.0 / total_commit_count);

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

    state.snapshot_throughput
        .push_back(total_commit_count * 1.0 / state.snapshot_duration);
    state.snapshot_abort_rate
        .push_back(total_abort_count * 1.0 / total_commit_count);
  }

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


  state.commit_latency = 0.0;
  for (size_t i = 0; i < num_threads; ++i) {
    // Weighted avg
    state.commit_latency += (commit_latencies[i] * (commit_counts_snapshots[snapshot_round - 1][i] * 1.0 / total_commit_count));
  }

  // Use the latency summary of worker 0
  state.latency_summary = commit_lat_summaries[0];

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


void ExecuteDeleteTest(executor::AbstractExecutor* executor) {
  
  // Execute stuff
  while (executor->Execute() == true);
}

}  // namespace smallbank
}  // namespace benchmark
}  // namespace peloton