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

#include "backend/benchmark/smallbank/smallbank_amalgamate.h"
#include "backend/benchmark/smallbank/smallbank_balance.h"
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

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

#define FREQUENCY_AMALGAMATE 0.04
#define FREQUENCY_BALANCE 0.24
#define FREQUENCY_DEPOSIT_CHECKING 0.24
#define FREQUENCY_TRANSACT_SAVINGS 0.24
#define FREQUENCY_WRITE_CHECK 0.24
#define FREQUENCY_SEND_PAYMENT 0  // No send payment in original doc

volatile bool is_running = true;
volatile bool is_run_table = false;

oid_t *abort_counts;
oid_t *commit_counts;
oid_t *steal_counts;
oid_t *generate_counts;
uint64_t *delay_totals;
uint64_t *delay_maxs;
uint64_t *delay_mins;

// execute time
uint64_t *exe_totals;

oid_t *ama_abort_counts;
oid_t *ama_commit_counts;
uint64_t *ama_delays;

oid_t *bal_abort_counts;
oid_t *bal_commit_counts;
uint64_t *bal_delays;

oid_t *dep_abort_counts;
oid_t *dep_commit_counts;
uint64_t *dep_delays;

oid_t *tra_abort_counts;
oid_t *tra_commit_counts;
uint64_t *tra_delays;

oid_t *wri_abort_counts;
oid_t *wri_commit_counts;
uint64_t *wri_delays;

oid_t stock_level_count;
double stock_level_avg_latency;

oid_t order_status_count;
double order_status_avg_latency;

oid_t scan_stock_count;
double scan_stock_avg_latency;

size_t GenerateAccountsId(const size_t &thread_id) {

  if ((int)NUM_ACCOUNTS <= state.backend_count) {
    return thread_id % NUM_ACCOUNTS;
  } else {
    int accounts_per_partition = NUM_ACCOUNTS / state.backend_count;
    int start_id = accounts_per_partition * thread_id;
    int end_id = ((int)thread_id != (state.backend_count - 1))
                     ? start_id + accounts_per_partition - 1
                     : NUM_ACCOUNTS - 1;
    return GetRandomInteger(start_id, end_id);
  }
}

size_t GenerateAccountsId() {
  size_t id = 0;

  // hot spot : 0 - 99
  if (GetRandomInteger(0, 99) < state.hot_spot) {
    id = GetRandomInteger(0, HOTSPOT_FIXED_SIZE - 1);
  }
  // return : 100 - others
  else {
    id = GetRandomInteger(0, (NUM_ACCOUNTS - HOTSPOT_FIXED_SIZE) - 1);
  }

  return id;
}

size_t GenerateAmount() { return GetRandomInteger(1, 10); }

// Only generate New-Order
void GenerateAndCacheQuery(ZipfDistribution &zipf) {
  // Generate query
  // Amalgamate *txn = GenerateAmalgamate(zipf);
  // Balance *txn = GenerateBalance(zipf);
  // DepositChecking *txn = GenerateDepositChecking(zipf);
  TransactSaving *txn = GenerateTransactSaving(zipf);
  // WriteCheck *txn = GenerateWriteCheck(zipf);

  /////////////////////////////////////////////////////////
  // Call txn scheduler to queue this executor
  /////////////////////////////////////////////////////////
  concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
}

// Generate txns according proportion
void GenerateALLAndCache(ZipfDistribution &zipf) {
  fast_random rng(rand());

  auto rng_val = rng.next_uniform();

  // Amalgamate
  if (rng_val <= FREQUENCY_AMALGAMATE) {
    Amalgamate *txn = GenerateAmalgamate(zipf);
    concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
  }
  // Balance
  else if (rng_val <= FREQUENCY_BALANCE + FREQUENCY_AMALGAMATE) {
    Balance *txn = GenerateBalance(zipf);
    concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
  }
  // DEPOSIT_CHECKING
  else if (rng_val <= FREQUENCY_DEPOSIT_CHECKING + FREQUENCY_BALANCE +
                          FREQUENCY_AMALGAMATE) {
    DepositChecking *txn = GenerateDepositChecking(zipf);
    concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
  }
  // TRANSACT_SAVINGS
  else if (rng_val <= FREQUENCY_TRANSACT_SAVINGS + FREQUENCY_DEPOSIT_CHECKING +
                          FREQUENCY_BALANCE + FREQUENCY_AMALGAMATE) {
    TransactSaving *txn = GenerateTransactSaving(zipf);
    concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
  }
  // WRITE_CHECK
  else {
    WriteCheck *txn = GenerateWriteCheck(zipf);
    concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
  }
}

std::unordered_map<int, ClusterRegion> ClusterAnalysis() {
  std::vector<SingleRegion> txn_regions;

  int size = concurrency::TransactionScheduler::GetInstance().GetCacheSize();

  concurrency::TransactionQuery *query = nullptr;

  // Pop all txns and transform them into regions
  for (int i = 0; i < size; i++) {
    bool ret =
        concurrency::TransactionScheduler::GetInstance().DequeueCache(query);

    if (ret == false) {
      LOG_INFO("Error when dequeue cache: is the cache empty??");
    }

    // LOG_INFO("Dequeue a query %d and begin to transform", i);

    // Transform the query into a region
    SingleRegion *region = query->RegionTransform();
    txn_regions.push_back(*region);
  }

  LOG_INFO("Finish transform, begin to clustering");

  // Cluster all txn_regions
  DBScan dbs(txn_regions, state.min_pts);

  LOG_INFO("Finish generate dbscan, begin to PreProcess");

  // Transform the regions into a graph. That is to mark each region's
  // neighbors
  dbs.PreProcess(1);
  // dbs.DebugPrintRegion();

  int re = dbs.Clustering();

  std::cout << "The number of cluster: " << re << std::endl;

  dbs.SetClusterRegion();
  // dbs.DebugPrintRegion();
  dbs.DebugPrintCluster();

  std::cout << "Meta:" << std::endl;

  dbs.DebugPrintClusterMeta();

  return dbs.GetClusters();
}

bool EnqueueCachedUpdate(
    std::chrono::system_clock::time_point &delay_start_time) {

  concurrency::TransactionQuery *query = nullptr;

  bool ret =
      concurrency::TransactionScheduler::GetInstance().DequeueCache(query);

  if (ret == false) {
    // LOG_INFO("Error when dequeue cache: is the cache empty??");
    return false;
  }

  // Start counting the response time when entering the queue
  query->SetStartTime(delay_start_time);

  // Push the query into the queue
  // Note: when popping the query and after executing it, the update_executor
  // and
  // index_executor should be deleted, then query itself should be deleted
  if (state.scheduler == SCHEDULER_TYPE_CONFLICT_DETECT) {
    concurrency::TransactionScheduler::GetInstance().CounterEnqueue(query);
  } else if (state.scheduler == SCHEDULER_TYPE_HASH) {
    // If run table is not ready
    if (state.log_table) {
      // concurrency::TransactionScheduler::GetInstance().SingleEnqueue(query);
      concurrency::TransactionScheduler::GetInstance().RandomEnqueue(
          query, state.single_ref);
    }
    // Run table is ready
    else if (state.online) {  // ONLINE means Run table

      // Debug
      // concurrency::TransactionScheduler::GetInstance().DumpRunTable();
      // LOG_INFO("================================================");

      if (state.lock_free) {
        // enqueue
        concurrency::TransactionScheduler::GetInstance().OOHashEnqueue(
            query, true, true, state.single_ref, state.canonical,
            state.fraction, state.pure_balance);
      }
      // lock run table
      else {
        concurrency::TransactionScheduler::GetInstance().RunTableLock();

        // enqueue
        concurrency::TransactionScheduler::GetInstance().OOHashEnqueue(
            query, true, true, state.single_ref, state.canonical,
            state.fraction, state.pure_balance);

        concurrency::TransactionScheduler::GetInstance().RunTableUnlock();
      }
    } else {  // otherwise use OOHASH method
      if (state.lock_free) {
        // enqueue
        concurrency::TransactionScheduler::GetInstance().OOHashEnqueue(
            query, true, false, state.single_ref, state.canonical,
            state.fraction, state.pure_balance);
      }
      // lock run table
      else {
        concurrency::TransactionScheduler::GetInstance().RunTableLock();

        // enqueue
        concurrency::TransactionScheduler::GetInstance().OOHashEnqueue(
            query, true, false, state.single_ref, state.canonical,
            state.fraction, state.pure_balance);

        concurrency::TransactionScheduler::GetInstance().RunTableUnlock();
      }
    }
  } else if (state.scheduler == SCHEDULER_TYPE_CONFLICT_LEANING) {
    // concurrency::TransactionScheduler::GetInstance().RouterRangeEnqueue(query);
    concurrency::TransactionScheduler::GetInstance().Enqueue(query);
  } else if (state.scheduler == SCHEDULER_TYPE_CLUSTER) {
    concurrency::TransactionScheduler::GetInstance().ClusterEnqueue(query);
  } else if (state.scheduler == SCHEDULER_TYPE_CONFLICT_RANGE) {
    // concurrency::TransactionScheduler::GetInstance().RangeEnqueue(query);
    concurrency::TransactionScheduler::GetInstance().Enqueue(query);
  } else {  // Control
    concurrency::TransactionScheduler::GetInstance().SingleEnqueue(query);
  }

  return true;
}

void UpdateCommitAbortCouter(concurrency::TransactionQuery *query, oid_t &ama,
                             oid_t &bal, oid_t &dep, oid_t &tra, oid_t &wri) {
  switch (query->GetTxnType()) {
    case TXN_TYPE_AMALGAMATE: {
      ama++;
      break;
    }
    case TXN_TYPE_BALANCE: {
      bal++;
      break;
    }
    case TXN_TYP_DEPOSIT_CHECKING: {
      dep++;
      break;
    }
    case TXN_TYPE_TRANSACT_SAVING: {
      tra++;
      break;
    }
    case TXN_TYPE_WRITE_CHECK: {
      wri++;
      break;
    }
    default: {
      LOG_INFO("GetTxnType:: Unsupported scheduler: %d", query->GetTxnType());
      break;
    }
  }
}

void UpdateDelayCounter(concurrency::TransactionQuery *query, uint64_t &ama,
                        uint64_t &bal, uint64_t &dep, uint64_t &tra,
                        uint64_t &wri) {
  switch (query->GetTxnType()) {
    case TXN_TYPE_AMALGAMATE: {
      query->RecordDelay(ama);
      break;
    }
    case TXN_TYPE_BALANCE: {
      query->RecordDelay(bal);
      break;
    }
    case TXN_TYP_DEPOSIT_CHECKING: {
      query->RecordDelay(dep);
      break;
    }
    case TXN_TYPE_TRANSACT_SAVING: {
      query->RecordDelay(tra);
      break;
    }
    case TXN_TYPE_WRITE_CHECK: {
      query->RecordDelay(wri);
      break;
    }
    default: {
      LOG_INFO("GetTxnType:: Unsupported scheduler: %d", query->GetTxnType());
      break;
    }
  }
}

void RunBackend(oid_t thread_id) {
  PinToCore(thread_id);

  oid_t &steal_count_ref = steal_counts[thread_id];
  oid_t &abort_count_ref = abort_counts[thread_id];
  oid_t &commit_count_ref = commit_counts[thread_id];

  uint64_t &delay_total_ref = delay_totals[thread_id];
  uint64_t &delay_max_ref = delay_maxs[thread_id];
  uint64_t &delay_min_ref = delay_mins[thread_id];

  uint64_t &exe_total_ref = exe_totals[thread_id];

  oid_t &ama_abort_count_ref = ama_abort_counts[thread_id];
  oid_t &ama_commit_count_ref = ama_commit_counts[thread_id];
  uint64_t &ama_delay_ref = ama_delays[thread_id];

  oid_t &bal_abort_count_ref = bal_abort_counts[thread_id];
  oid_t &bal_commit_count_ref = bal_commit_counts[thread_id];
  uint64_t &bal_delay_ref = bal_delays[thread_id];

  oid_t &dep_abort_count_ref = dep_abort_counts[thread_id];
  oid_t &dep_commit_count_ref = dep_commit_counts[thread_id];
  uint64_t &dep_delay_ref = dep_delays[thread_id];

  oid_t &tra_abort_count_ref = tra_abort_counts[thread_id];
  oid_t &tra_commit_count_ref = tra_commit_counts[thread_id];
  uint64_t &tra_delay_ref = tra_delays[thread_id];

  oid_t &wri_abort_count_ref = wri_abort_counts[thread_id];
  oid_t &wri_commit_count_ref = wri_commit_counts[thread_id];
  uint64_t &wri_delay_ref = wri_delays[thread_id];

  while (true) {
    if (is_running == false) {
      break;
    }

    // Pop a query from a queue and execute
    concurrency::TransactionQuery *ret_query = nullptr;
    bool ret_pop = false;
    bool ret_steal = false;

    //////////////////////////////////////////
    // Pop a query
    //////////////////////////////////////////
    switch (state.scheduler) {
      case SCHEDULER_TYPE_NONE:
      case SCHEDULER_TYPE_CONTROL:
      case SCHEDULER_TYPE_ABORT_QUEUE: {
        ret_pop =
            concurrency::TransactionScheduler::GetInstance().SingleDequeue(
                ret_query);
        break;
      }
      case SCHEDULER_TYPE_CONFLICT_DETECT: {
        ret_pop =
            concurrency::TransactionScheduler::GetInstance().CounterDequeue(
                ret_query, thread_id);
        break;
      }
      case SCHEDULER_TYPE_HASH: {
        if (state.log_table) {
          ret_pop =
              concurrency::TransactionScheduler::GetInstance().PartitionDequeue(
                  ret_query, thread_id, ret_steal);
        }
        ret_pop =
            concurrency::TransactionScheduler::GetInstance().PartitionDequeue(
                ret_query, thread_id, ret_steal);

        //        ret_pop =
        //            concurrency::TransactionScheduler::GetInstance().SimpleDequeue(
        //                ret_query, thread_id);

        break;
      }
      case SCHEDULER_TYPE_CONFLICT_LEANING: {
        ret_pop =
            concurrency::TransactionScheduler::GetInstance().PartitionDequeue(
                ret_query, thread_id, ret_steal);
        //        ret_pop =
        //            concurrency::TransactionScheduler::GetInstance().SimpleDequeue(
        //                ret_query, thread_id);
        break;
      }
      case SCHEDULER_TYPE_CLUSTER: {
        ret_pop =
            concurrency::TransactionScheduler::GetInstance().PartitionDequeue(
                ret_query, thread_id, ret_steal);
        break;
      }
      case SCHEDULER_TYPE_CONFLICT_RANGE: {
        ret_pop = concurrency::TransactionScheduler::GetInstance().Dequeue(
            ret_query, thread_id);
        break;
      }
      default: {
        LOG_ERROR("plan_type :: Unsupported scheduler: %u ", state.scheduler);
        break;
      }
    }  // end switch

    // process the pop result. If queue is empty, continue loop
    if (ret_pop == false) {
      // LOG_INFO("Queue is empty");
      continue;
    }

    PL_ASSERT(ret_query != nullptr);

    // Record stealing
    if (ret_steal) {
      steal_count_ref++;
    }

    // Now record start time for execution
    std::chrono::system_clock::time_point exe_start_time =
        std::chrono::system_clock::now();
    ret_query->SetExeStartTime(exe_start_time);

    //////////////////////////////////////////////////////////////////////////////////////
    // Execute query
    //////////////////////////////////////////////////////////////////////////////////////

    // Re-exeucte a txn until success
    while (ret_query->Run() == false) {
      if (is_running == false) {
        break;
      }

      // Increase the counter
      abort_count_ref++;

      // Increase abort counter for others
      UpdateCommitAbortCouter(ret_query, ama_abort_count_ref,
                              bal_abort_count_ref, dep_abort_count_ref,
                              tra_abort_count_ref, wri_abort_count_ref);

      switch (state.scheduler) {
        case SCHEDULER_TYPE_NONE: {
          // We do nothing in this case.Just delete the query
          // Since we discard the txn, do not record the throughput and delay
          goto program_end;
          // break;
        }
        case SCHEDULER_TYPE_CONTROL:
        case SCHEDULER_TYPE_CONFLICT_LEANING:
        case SCHEDULER_TYPE_CLUSTER:
        case SCHEDULER_TYPE_CONFLICT_RANGE:
        case SCHEDULER_TYPE_ABORT_QUEUE:
        case SCHEDULER_TYPE_CONFLICT_DETECT: {
          // Control: The txn re-executed immediately
          break;
        }
        case SCHEDULER_TYPE_HASH: {
          if (state.log_table) {
            if (state.fraction) {
              ret_query->UpdateLogTableFullConflict(state.single_ref,
                                                    state.canonical);
            } else {
              ret_query->UpdateLogTable(state.single_ref, state.canonical);
            }
          }
          // The txn re-executed immediately
          break;
        }
        default: {
          LOG_INFO("Scheduler_type :: Unsupported scheduler: %u ",
                   state.scheduler);
          break;
        }
      }  // end switch
    }    // end if execute fail

    /////////////////////////////////////////////////
    // Execute success: the memory should be deleted
    /////////////////////////////////////////////////

    // First compute the delay
    ret_query->RecordDelay(delay_total_ref, delay_max_ref, delay_min_ref);

    // Update txn type
    UpdateDelayCounter(ret_query, ama_delay_ref, bal_delay_ref, dep_delay_ref,
                       tra_delay_ref, wri_delay_ref);

    // For execution time
    ret_query->RecordExetime(exe_total_ref);

    // Increase commit counter
    commit_count_ref++;

    // Increase commit counter for others
    UpdateCommitAbortCouter(ret_query, ama_commit_count_ref,
                            bal_commit_count_ref, dep_commit_count_ref,
                            tra_commit_count_ref, wri_commit_count_ref);

    // clean up the hash table
    if (state.scheduler == SCHEDULER_TYPE_HASH) {
      // Update Log Table when success
      if (state.log_table && state.fraction) {
        ret_query->UpdateLogTableFullSuccess(state.single_ref, state.canonical);
      }
      // Remove txn from Run Table
      if (!state.lock_free) {
        ret_query->DecreaseRunTable(state.single_ref, state.canonical);
      }
    }

  program_end:
    // Finally, clean up
    if (ret_query != nullptr) {
      ret_query->Cleanup();
      delete ret_query;
    }

  }  // end big while
}

void QueryBackend(oid_t thread_id) {

  PinToCore(thread_id);

  oid_t &generate_count_ref = generate_counts[thread_id - state.backend_count];
  LOG_INFO("Enqueue thread---%d---", thread_id);

  int speed = state.generate_speed;
  int count = 0;
  std::chrono::steady_clock::time_point start_time =
      std::chrono::steady_clock::now();

  // used for each round
  std::chrono::system_clock::time_point delay_start_time =
      std::chrono::system_clock::now();

  while (true) {
    if (is_running == false) {
      break;
    }

    if (EnqueueCachedUpdate(delay_start_time) == false) {
      _mm_pause();
      continue;
    }
    generate_count_ref++;

    // For OOAHSH continue
    if (state.run_continue && state.log_table) {
      if (generate_count_ref >= 240000) {
        // If log_table is false, that means the first phase is done
        while (state.log_table == true) {
          _mm_pause();
        }
      }
    }

    // If there is no speed limit, ignore the speed control
    if (speed == 0) {
      continue;
    }

    count++;

    // If executed txns exceeds the speed, compute elapsed time
    if (count >= speed) {
      // Reset the counter
      count = 0;
      // SleepMilliseconds(1000);

      // Compute the elapsed time
      std::chrono::steady_clock::time_point now_time =
          std::chrono::steady_clock::now();

      int elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(
          now_time - start_time).count();

      std::cout << "elapsed_time: " << elapsed_time << std::endl;

      // If elapsed time is still less than 1 second, sleep the rest of the time
      if ((elapsed_time) < 1000) {
        SleepMilliseconds(1000 - (elapsed_time));

        // Reset start time
        start_time = std::chrono::steady_clock::now();
      }
      // Otherwise, if elapsed time is larger then 1 second, re-set start time
      // and continue to enqueue txns
      else {
        // Rest start time
        start_time = std::chrono::steady_clock::now();
      }

      delay_start_time = std::chrono::system_clock::now();
    }
  }
}

void RunWorkload() {

  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  // oid_t num_scan_threads = state.scan_backend_count;
  oid_t num_generate = state.generate_count;

  steal_counts = new oid_t[num_threads];
  memset(steal_counts, 0, sizeof(oid_t) * num_threads);

  abort_counts = new oid_t[num_threads];
  memset(abort_counts, 0, sizeof(oid_t) * num_threads);
  commit_counts = new oid_t[num_threads];
  memset(commit_counts, 0, sizeof(oid_t) * num_threads);

  generate_counts = new oid_t[num_generate];
  memset(generate_counts, 0, sizeof(oid_t) * num_generate);

  // Initiate Delay
  delay_totals = new uint64_t[num_threads];
  memset(delay_totals, 0, sizeof(uint64_t) * num_threads);
  delay_maxs = new uint64_t[num_threads];
  memset(delay_maxs, 0, sizeof(uint64_t) * num_threads);
  delay_mins = new uint64_t[num_threads];
  std::fill_n(delay_mins, num_threads, 1000000);

  exe_totals = new uint64_t[num_threads];
  memset(exe_totals, 0, sizeof(uint64_t) * num_threads);

  // amalgmate
  ama_abort_counts = new oid_t[num_threads];
  memset(ama_abort_counts, 0, sizeof(oid_t) * num_threads);
  ama_commit_counts = new oid_t[num_threads];
  memset(ama_commit_counts, 0, sizeof(oid_t) * num_threads);
  ama_delays = new uint64_t[num_threads];
  memset(ama_delays, 0, sizeof(uint64_t) * num_threads);

  // bal
  bal_abort_counts = new oid_t[num_threads];
  memset(bal_abort_counts, 0, sizeof(oid_t) * num_threads);
  bal_commit_counts = new oid_t[num_threads];
  memset(bal_commit_counts, 0, sizeof(oid_t) * num_threads);
  bal_delays = new uint64_t[num_threads];
  memset(bal_delays, 0, sizeof(uint64_t) * num_threads);

  dep_abort_counts = new oid_t[num_threads];
  memset(dep_abort_counts, 0, sizeof(oid_t) * num_threads);
  dep_commit_counts = new oid_t[num_threads];
  memset(dep_commit_counts, 0, sizeof(oid_t) * num_threads);
  dep_delays = new uint64_t[num_threads];
  memset(dep_delays, 0, sizeof(uint64_t) * num_threads);

  tra_abort_counts = new oid_t[num_threads];
  memset(tra_abort_counts, 0, sizeof(oid_t) * num_threads);
  tra_commit_counts = new oid_t[num_threads];
  memset(tra_commit_counts, 0, sizeof(oid_t) * num_threads);
  tra_delays = new uint64_t[num_threads];
  memset(tra_delays, 0, sizeof(uint64_t) * num_threads);

  wri_abort_counts = new oid_t[num_threads];
  memset(wri_abort_counts, 0, sizeof(oid_t) * num_threads);
  wri_commit_counts = new oid_t[num_threads];
  memset(wri_commit_counts, 0, sizeof(oid_t) * num_threads);
  wri_delays = new uint64_t[num_threads];
  memset(wri_delays, 0, sizeof(uint64_t) * num_threads);

  stock_level_count = 0;
  stock_level_avg_latency = 0.0;

  order_status_count = 0;
  order_status_avg_latency = 0.0;

  scan_stock_count = 0;
  scan_stock_avg_latency = 0.0;

  // snapshot_duration = 1 by defaul
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
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
  }

  // Launch a bunch of threads to queue the query
  for (oid_t thread_itr = num_threads; thread_itr < num_threads + num_generate;
       ++thread_itr) {
    thread_group.push_back(std::move(std::thread(QueryBackend, thread_itr)));
  }

  oid_t last_tile_group_id = 0;

  ////////////////This is only for OOHASH//////////////
  if (state.run_continue) {
    for (size_t round_id = 0; round_id < snapshot_round / 2; ++round_id) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(int(state.snapshot_duration * 1000)));
      memcpy(abort_counts_snapshots[round_id], abort_counts,
             sizeof(oid_t) * num_threads);
      memcpy(commit_counts_snapshots[round_id], commit_counts,
             sizeof(oid_t) * num_threads);
      auto &manager = catalog::Manager::GetInstance();

      oid_t current_tile_group_id = manager.GetLastTileGroupId();
      if (round_id != 0) {
        state.snapshot_memory.push_back(current_tile_group_id -
                                        last_tile_group_id);
      }
      last_tile_group_id = current_tile_group_id;
    }
    state.snapshot_memory.push_back(
        state.snapshot_memory.at(state.snapshot_memory.size() - 1));

    LOG_INFO("Change mode to OOHASH");
    state.log_table = false;

    for (size_t round_id = snapshot_round / 2; round_id < snapshot_round;
         ++round_id) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(int(state.snapshot_duration * 1000)));
      memcpy(abort_counts_snapshots[round_id], abort_counts,
             sizeof(oid_t) * num_threads);
      memcpy(commit_counts_snapshots[round_id], commit_counts,
             sizeof(oid_t) * num_threads);
      auto &manager = catalog::Manager::GetInstance();

      state.snapshot_memory.push_back(manager.GetLastTileGroupId());
    }
  }
  // For other policy run_continue is false, execute from here
  else {

    for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(int(state.snapshot_duration * 1000)));
      memcpy(abort_counts_snapshots[round_id], abort_counts,
             sizeof(oid_t) * num_threads);
      memcpy(commit_counts_snapshots[round_id], commit_counts,
             sizeof(oid_t) * num_threads);
      auto &manager = catalog::Manager::GetInstance();

      oid_t current_tile_group_id = manager.GetLastTileGroupId();
      if (round_id != 0) {
        state.snapshot_memory.push_back(current_tile_group_id -
                                        last_tile_group_id);
      }
      last_tile_group_id = current_tile_group_id;
    }
    state.snapshot_memory.push_back(
        state.snapshot_memory.at(state.snapshot_memory.size() - 1));
  }

  is_running = false;

  // Join the threads with the main thread
  for (oid_t thread_itr = 0; thread_itr < num_threads + num_generate;
       ++thread_itr) {
    thread_group[thread_itr].join();
  }

  std::cout << "Is running: " << is_running << std::endl;

  // If this is offline analysis, write Log Table into a file. It is a
  // map: int-->int (reference-key, conflict-counts)
  if (state.scheduler == SCHEDULER_TYPE_HASH) {
    if (state.log_table || state.run_continue) {
      if (state.fraction) {
        concurrency::TransactionScheduler::GetInstance().OutputLogTableFull(
            LOGTABLE);
      } else {
        concurrency::TransactionScheduler::GetInstance().OutputLogTable(
            LOGTABLE);
      }
    }
  }

  // calculate the generate rate
  oid_t total_generate_count = 0;
  for (size_t i = 0; i < num_generate; ++i) {
    total_generate_count += generate_counts[i];
  }
  state.generate_rate = total_generate_count * 1.0 / state.duration;

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
                                      (total_commit_count + total_abort_count));

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
    state.snapshot_abort_rate.push_back(
        total_abort_count * 1.0 / (total_commit_count + total_abort_count));
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

  state.throughput1 = total_commit_count * 1.0 / state.duration;
  state.abort_rate1 =
      total_abort_count * 1.0 / (total_commit_count + total_abort_count);

  total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts[i];
  }
  total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts[i];
  }
  oid_t total_steal_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_steal_count += steal_counts[i];
  }

  state.steal = total_steal_count * 1.0 / state.duration;
  state.steal_rate = total_steal_count * 1.0 / total_commit_count;

  state.throughput2 = total_commit_count * 1.0 / state.duration;
  state.abort_rate2 =
      total_abort_count * 1.0 / (total_commit_count + total_abort_count);

  std::cout << "total commit : " << total_commit_count << std::endl;

  // calculate the average delay: ms
  uint64_t total_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_delay += delay_totals[i];
  }
  state.delay_ave = (total_delay * 1.0) / (total_commit_count * 1000);

  // calculate the exe time: ms
  uint64_t total_exe = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_exe += exe_totals[i];
  }
  state.exe_time = (total_exe * 1.0) / (total_commit_count * 1000);

  // calculate the max delay
  uint64_t max_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    if (max_delay < delay_maxs[i]) {
      max_delay = delay_maxs[i];
    }
  }
  state.delay_max = max_delay * 1.0 / 1000;

  // calculate the min delay
  uint64_t min_delay = delay_mins[0];
  for (size_t i = 1; i < num_threads; ++i) {
    if (min_delay > delay_mins[i]) {
      min_delay = delay_mins[i];
    }
  }
  state.delay_min = min_delay * 1.0 / 1000;

  std::cout << "delay_total: " << total_delay
            << ". total_commit_count: " << total_commit_count << std::endl;

  // No use for now
  state.stock_level_latency = stock_level_avg_latency;
  state.order_status_latency = order_status_avg_latency;
  state.scan_stock_latency = scan_stock_avg_latency;

  // Amalgamate
  oid_t total_ama_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_ama_commit_count += ama_commit_counts[i];
  }
  oid_t total_ama_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_ama_abort_count += ama_abort_counts[i];
  }
  uint64_t total_ama_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_ama_delay += ama_delays[i];
  }

  state.ama_throughput = total_ama_commit_count * 1.0 / state.duration;
  state.ama_abort_rate = total_ama_abort_count * 1.0 /
                         (total_ama_commit_count + total_ama_abort_count);
  state.ama_delay = total_ama_delay * 1.0 / (total_ama_commit_count * 1000);

  // Balance
  oid_t total_bal_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_bal_commit_count += bal_commit_counts[i];
  }
  oid_t total_bal_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_bal_abort_count += bal_abort_counts[i];
  }
  uint64_t total_bal_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_bal_delay += bal_delays[i];
  }

  state.bal_throughput = total_bal_commit_count * 1.0 / state.duration;
  state.bal_abort_rate = total_bal_abort_count * 1.0 /
                         (total_bal_commit_count + total_bal_abort_count);
  state.bal_delay = total_bal_delay * 1.0 / (total_bal_commit_count * 1000);

  // Dep
  oid_t total_dep_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_dep_commit_count += dep_commit_counts[i];
  }
  oid_t total_dep_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_dep_abort_count += dep_abort_counts[i];
  }
  uint64_t total_dep_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_dep_delay += dep_delays[i];
  }

  state.dep_throughput = total_dep_commit_count * 1.0 / state.duration;
  state.dep_abort_rate = total_dep_abort_count * 1.0 /
                         (total_dep_commit_count + total_dep_abort_count);
  state.dep_delay = total_dep_delay * 1.0 / (total_dep_commit_count * 1000);

  // tra
  oid_t total_tra_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_tra_commit_count += tra_commit_counts[i];
  }
  oid_t total_tra_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_tra_abort_count += tra_abort_counts[i];
  }
  uint64_t total_tra_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_tra_delay += tra_delays[i];
  }

  state.tra_throughput = total_tra_commit_count * 1.0 / state.duration;
  state.tra_abort_rate = total_tra_abort_count * 1.0 /
                         (total_tra_commit_count + total_tra_abort_count);
  state.tra_delay = total_tra_delay * 1.0 / (total_tra_commit_count * 1000);

  std::cout << "tra_delay_total: " << total_tra_delay
            << ". total_tra_commit_count: " << total_tra_commit_count
            << std::endl;

  // wri
  oid_t total_wri_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_wri_commit_count += wri_commit_counts[i];
  }
  oid_t total_wri_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_wri_abort_count += wri_abort_counts[i];
  }
  uint64_t total_wri_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_wri_delay += wri_delays[i];
  }

  state.wri_throughput = total_wri_commit_count * 1.0 / state.duration;
  state.wri_abort_rate = total_wri_abort_count * 1.0 /
                         (total_wri_commit_count + total_wri_abort_count);
  state.wri_delay = total_wri_delay * 1.0 / (total_wri_commit_count * 1000);

  LOG_INFO("============Delete Everything==========");
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

  delete[] steal_counts;
  steal_counts = nullptr;

  delete[] generate_counts;
  generate_counts = nullptr;

  delete[] delay_totals;
  delay_totals = nullptr;
  delete[] delay_maxs;
  delay_maxs = nullptr;
  delete[] delay_mins;
  delay_mins = nullptr;

  delete[] exe_totals;
  exe_totals = nullptr;

  // For others
  delete[] ama_abort_counts;
  ama_abort_counts = nullptr;
  delete[] ama_commit_counts;
  ama_commit_counts = nullptr;
  delete[] ama_delays;
  ama_delays = nullptr;

  delete[] bal_abort_counts;
  bal_abort_counts = nullptr;
  delete[] bal_commit_counts;
  bal_commit_counts = nullptr;
  delete[] bal_delays;
  bal_delays = nullptr;

  delete[] dep_abort_counts;
  dep_abort_counts = nullptr;
  delete[] dep_commit_counts;
  dep_commit_counts = nullptr;
  delete[] dep_delays;
  dep_delays = nullptr;

  delete[] tra_abort_counts;
  tra_abort_counts = nullptr;
  delete[] tra_commit_counts;
  tra_commit_counts = nullptr;
  delete[] tra_delays;
  tra_delays = nullptr;

  delete[] wri_abort_counts;
  wri_abort_counts = nullptr;
  delete[] wri_commit_counts;
  wri_commit_counts = nullptr;
  delete[] wri_delays;
  wri_delays = nullptr;

  LOG_INFO("============TABLE SIZES==========");
  LOG_INFO("accounts count = %u", accounts_table->GetAllCurrentTupleCount());
  LOG_INFO("savings count  = %u", savings_table->GetAllCurrentTupleCount());
  LOG_INFO("checking count = %u", checking_table->GetAllCurrentTupleCount());

  LOG_INFO("============TILEGROUP SIZES==========");
  LOG_INFO("accounts tile group = %lu", accounts_table->GetTileGroupCount());
  LOG_INFO("savings tile group  = %lu", savings_table->GetTileGroupCount());
  LOG_INFO("checking tile group = %lu", checking_table->GetTileGroupCount());
}

/////////////////////////////////////////////////////////
// HARNESS
/////////////////////////////////////////////////////////

std::vector<std::vector<Value>> ExecuteReadTest(
    executor::AbstractExecutor *executor) {

  std::vector<std::vector<Value>> logical_tile_values;

  // Execute stuff
  while (executor->Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());

    // is this possible?
    if (result_tile == nullptr) break;

    auto column_count = result_tile->GetColumnCount();

    for (oid_t tuple_id : *result_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(
          result_tile.get(), tuple_id);
      std::vector<Value> tuple_values;
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
        auto value = cur_tuple.GetValue(column_itr);
        tuple_values.push_back(value);
      }

      // Move the tuple list
      logical_tile_values.push_back(std::move(tuple_values));
    }
  }

  return std::move(logical_tile_values);
}

void ExecuteUpdateTest(executor::AbstractExecutor *executor) {

  // Execute stuff
  while (executor->Execute() == true)
    ;
}

void ExecuteDeleteTest(executor::AbstractExecutor *executor) {

  // Execute stuff
  while (executor->Execute() == true)
    ;
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
