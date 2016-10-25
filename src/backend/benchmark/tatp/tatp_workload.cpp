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

#include "backend/benchmark/tatp/tatp_workload.h"
#include "backend/benchmark/tatp/tatp_configuration.h"
#include "backend/benchmark/tatp/tatp_loader.h"

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
namespace tatp {

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////
#define RANDOM_ASSIGN 200000

#define FREQUENCY_GET_ACCESS_DATA 0.03         // 0.03
#define FREQUENCY_GET_NEW_DESTINATION 0.03     // 0.03
#define FREQUENCY_GET_SUBSCRIBER_DATA 0.4      // 0.40
#define FREQUENCY_INSERT_CALL_FORWARDING 0.02  // 0.02
#define FREQUENCY_DELETE_CALL_FORWARDING 0.02  // 0.02
#define FREQUENCY_UPDATE_SUBSCRIBER_DATA 0.10  // 0.1
#define FREQUENCY_UPDATE_LOCATION 0.4          // 0.40

volatile bool is_running = true;
volatile bool is_run_table = false;

oid_t *abort_counts;
oid_t *commit_counts;
oid_t *generate_counts;
uint64_t *delay_totals;
uint64_t *delay_maxs;
uint64_t *delay_mins;
uint64_t *assign_delays;
oid_t *steal_counts;

// execute time
uint64_t *exe_totals;

oid_t *del_abort_counts;
oid_t *del_commit_counts;
uint64_t *del_delays;

oid_t *acc_abort_counts;
oid_t *acc_commit_counts;
uint64_t *acc_delays;

oid_t *new_abort_counts;
oid_t *new_commit_counts;
uint64_t *new_delays;

oid_t *sub_abort_counts;
oid_t *sub_commit_counts;
uint64_t *sub_delays;

oid_t *ins_abort_counts;
oid_t *ins_commit_counts;
uint64_t *ins_delays;

oid_t *upl_abort_counts;
oid_t *upl_commit_counts;
uint64_t *upl_delays;

oid_t *ups_abort_counts;
oid_t *ups_commit_counts;
uint64_t *ups_delays;

oid_t stock_level_count;
double stock_level_avg_latency;

oid_t order_status_count;
double order_status_avg_latency;

oid_t scan_stock_count;
double scan_stock_avg_latency;

size_t GenerateSubscriberId() {
  // hot spot : 0 - 99
  if (GetRandomInteger(0, 99) < state.hot_spot) {
    return GetRandomInteger(0, HOTSPOT_FIXED_SIZE - 1);
  }
  // return : 100 - others
  else {
    return GetRandomInteger(0, (NUM_SUBSCRIBERS - HOTSPOT_FIXED_SIZE) - 1);
  }
}

size_t GenerateAiTypeId() { return GetRandomInteger(1, AI_TYPE.size()); }
size_t GenerateSfTypeId() { return GetRandomInteger(1, SF_TYPE.size()); }
size_t GenerateStartTime() { return GetRandomIntegerFromArr(START_TIME); }
size_t GenerateEndTime() { return GetRandomInteger(1, 24); }
size_t GenerateAmount() { return GetRandomInteger(1, 10); }

// Only generate New-Order
void GenerateAndCacheQuery(ZipfDistribution &zipf) {
  // Generate query
  // GetNewDestination *txn = GenerateGetNewDestination(zipf);
  // GetAccessData *txn = GenerateGetAccessData(zipf);  // 80000
  // GetSubscriberData *txn = GenerateGetSubscriberData(zipf);
  //
  // InsertCallForwarding *txn = GenerateInsertCallForwarding(zipf);
  // DeteteCallForwarding *txn = GenerateDeteteCallForwarding(zipf);
  UpdateSubscriberData *txn = GenerateUpdateSubscriberData(zipf);  // 5176
  // UpdateLocation *txn = GenerateUpdateLocation(zipf);  // 5303
  /////////////////////////////////////////////////////////
  // Call txn scheduler to queue this executor
  /////////////////////////////////////////////////////////
  concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
}

// Generate txns according proportion
void GenerateALLAndCache(ZipfDistribution &zipf) {
  fast_random rng(rand());

  auto rng_val = rng.next_uniform();

  // GET_ACCESS_DATA
  if (rng_val <= FREQUENCY_GET_ACCESS_DATA) {
    GetAccessData *txn = GenerateGetAccessData(zipf);
    concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
  }
  // GET_NEW_DESTINATION
  else if (rng_val <=
           FREQUENCY_GET_ACCESS_DATA + FREQUENCY_GET_NEW_DESTINATION) {
    GetNewDestination *txn = GenerateGetNewDestination(zipf);
    concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
  }
  // GET_SUBSCRIBER_DATA
  else if (rng_val <= FREQUENCY_GET_ACCESS_DATA +
                          FREQUENCY_GET_NEW_DESTINATION +
                          FREQUENCY_GET_SUBSCRIBER_DATA) {
    GetSubscriberData *txn = GenerateGetSubscriberData(zipf);
    concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
  }
  // INSERT_CALL_FORWARDING
  else if (rng_val <= FREQUENCY_GET_ACCESS_DATA +
                          FREQUENCY_GET_NEW_DESTINATION +
                          FREQUENCY_GET_SUBSCRIBER_DATA +
                          FREQUENCY_INSERT_CALL_FORWARDING) {
    InsertCallForwarding *txn = GenerateInsertCallForwarding(zipf);
    concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
  }
  // DELETE_CALL_FORWARDING
  else if (rng_val <= FREQUENCY_GET_ACCESS_DATA +
                          FREQUENCY_GET_NEW_DESTINATION +
                          FREQUENCY_GET_SUBSCRIBER_DATA +
                          FREQUENCY_INSERT_CALL_FORWARDING +
                          FREQUENCY_DELETE_CALL_FORWARDING) {
    DeteteCallForwarding *txn = GenerateDeteteCallForwarding(zipf);
    concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
  }
  // UPDATE_SUBSCRIBER_DATA
  else if (rng_val <= FREQUENCY_GET_ACCESS_DATA +
                          FREQUENCY_GET_NEW_DESTINATION +
                          FREQUENCY_GET_SUBSCRIBER_DATA +
                          FREQUENCY_INSERT_CALL_FORWARDING +
                          FREQUENCY_DELETE_CALL_FORWARDING +
                          FREQUENCY_UPDATE_SUBSCRIBER_DATA) {
    UpdateSubscriberData *txn = GenerateUpdateSubscriberData(zipf);
    concurrency::TransactionScheduler::GetInstance().CacheQuery(txn);
  }
  // UPDATE_LOCATION
  else {
    UpdateLocation *txn = GenerateUpdateLocation(zipf);
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
    std::chrono::system_clock::time_point &delay_start_time, oid_t thread_id) {

  assert(thread_id != 0);

  uint64_t &assign_delay_ref = assign_delays[thread_id - state.backend_count];

  concurrency::TransactionQuery *query = nullptr;

  bool ret =
      concurrency::TransactionScheduler::GetInstance().DequeueCache(query);

  if (ret == false) {
    // LOG_INFO("Error when dequeue cache: is the cache empty??");
    return false;
  }

  PL_ASSERT(query != nullptr);

  // Start counting the response time when entering the queue
  query->SetStartTime(delay_start_time);

  // Push the query into the queue
  // Note: when popping the query and after executing it, the update_executor
  // and
  // index_executor should be deleted, then query itself should be deleted

  // Now record start time for execution
  std::chrono::system_clock::time_point assign_start_time =
      std::chrono::system_clock::now();

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
    else if (state.online) {  // ONLINE means MAX
      if (state.lock_free) {
        concurrency::TransactionScheduler::GetInstance().OOHashEnqueue(
            query, true, true, state.single_ref, state.canonical,
            state.fraction, state.pure_balance);
      }
      // lock run table
      else {
        concurrency::TransactionScheduler::GetInstance().RunTableLock();

        concurrency::TransactionScheduler::GetInstance().OOHashEnqueue(
            query, true, true, state.single_ref, state.canonical,
            state.fraction, state.pure_balance);

        concurrency::TransactionScheduler::GetInstance().RunTableUnlock();
      }
    } else {  // otherwise SUM
      if (state.lock_free) {
        concurrency::TransactionScheduler::GetInstance().OOHashEnqueue(
            query, true, false, state.single_ref, state.canonical,
            state.fraction, state.pure_balance);
      }
      // lock run table
      else {
        concurrency::TransactionScheduler::GetInstance().RunTableLock();

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
  } else if (state.scheduler == SCHEDULER_TYPE_ABORT_QUEUE) {
    concurrency::TransactionScheduler::GetInstance().RandomEnqueue(
        query, state.single_ref);
  } else {  // Control None
    concurrency::TransactionScheduler::GetInstance().SingleEnqueue(query);
  }

  // Now record end
  uint64_t assign_delay = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now() - assign_start_time).count();

  assign_delay_ref = assign_delay_ref + assign_delay;

  return true;
}

void UpdateCommitAbortCouter(concurrency::TransactionQuery *query, oid_t &del,
                             oid_t &acc, oid_t &new_des, oid_t &sub, oid_t &ins,
                             oid_t &upl, oid_t &ups) {
  switch (query->GetTxnType()) {
    case TXN_TYPE_DELETE_CALL_FORWARDING: {
      del++;
      break;
    }
    case TXN_TYPE_GET_ACCESS_DATA: {
      acc++;
      break;
    }
    case TXN_TYPE_GET_NEW_DESTINATION: {
      new_des++;
      break;
    }
    case TXN_TYPE_GET_SUBSCRIBER_DATA: {
      sub++;
      break;
    }
    case TXN_TYPE_INSERT_CALL_FORWARDING: {
      ins++;
      break;
    }
    case TXN_TYPE_UPDATE_LOCATION: {
      upl++;
      break;
    }
    case TXN_TYPE_UPDATE_SUBSCRIBER_DATA: {
      ups++;
      break;
    }
    default: {
      LOG_INFO("GetTxnType:: Unsupported scheduler: %d", query->GetTxnType());
      break;
    }
  }
}

void UpdateDelayCounter(concurrency::TransactionQuery *query, uint64_t &del,
                        uint64_t &acc, uint64_t &new_des, uint64_t &sub,
                        uint64_t &ins, uint64_t &upl, uint64_t &ups) {
  switch (query->GetTxnType()) {
    case TXN_TYPE_DELETE_CALL_FORWARDING: {
      query->RecordDelay(del);
      break;
    }
    case TXN_TYPE_GET_ACCESS_DATA: {
      query->RecordDelay(acc);
      break;
    }
    case TXN_TYPE_GET_NEW_DESTINATION: {
      query->RecordDelay(new_des);
      break;
    }
    case TXN_TYPE_GET_SUBSCRIBER_DATA: {
      query->RecordDelay(sub);
      break;
    }
    case TXN_TYPE_INSERT_CALL_FORWARDING: {
      query->RecordDelay(ins);
      break;
    }
    case TXN_TYPE_UPDATE_LOCATION: {
      query->RecordDelay(upl);
      break;
    }
    case TXN_TYPE_UPDATE_SUBSCRIBER_DATA: {
      query->RecordDelay(ups);
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

  oid_t &del_abort_count_ref = del_abort_counts[thread_id];
  oid_t &del_commit_count_ref = del_commit_counts[thread_id];
  uint64_t &del_delay_ref = del_delays[thread_id];

  oid_t &acc_abort_count_ref = acc_abort_counts[thread_id];
  oid_t &acc_commit_count_ref = acc_commit_counts[thread_id];
  uint64_t &acc_delay_ref = acc_delays[thread_id];

  oid_t &new_abort_count_ref = new_abort_counts[thread_id];
  oid_t &new_commit_count_ref = new_commit_counts[thread_id];
  uint64_t &new_delay_ref = new_delays[thread_id];

  oid_t &sub_abort_count_ref = sub_abort_counts[thread_id];
  oid_t &sub_commit_count_ref = sub_commit_counts[thread_id];
  uint64_t &sub_delay_ref = sub_delays[thread_id];

  oid_t &ins_abort_count_ref = ins_abort_counts[thread_id];
  oid_t &ins_commit_count_ref = ins_commit_counts[thread_id];
  uint64_t &ins_delay_ref = ins_delays[thread_id];

  oid_t &upl_abort_count_ref = upl_abort_counts[thread_id];
  oid_t &upl_commit_count_ref = upl_commit_counts[thread_id];
  uint64_t &upl_delay_ref = upl_delays[thread_id];

  oid_t &ups_abort_count_ref = ups_abort_counts[thread_id];
  oid_t &ups_commit_count_ref = ups_commit_counts[thread_id];
  uint64_t &ups_delay_ref = ups_delays[thread_id];

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
      case SCHEDULER_TYPE_CONTROL: {
        ret_pop =
            concurrency::TransactionScheduler::GetInstance().SingleDequeue(
                ret_query);
        break;
      }
      case SCHEDULER_TYPE_ABORT_QUEUE: {
        //        ret_pop =
        //            concurrency::TransactionScheduler::GetInstance().SingleDequeue(
        //                ret_query);
        ret_pop =
            concurrency::TransactionScheduler::GetInstance().PartitionDequeue(
                ret_query, thread_id, ret_steal);
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

    //////////////////////////////////////////
    // Execute query
    //////////////////////////////////////////

    // Re-exeucte a txn until success
    while (ret_query->Run() == false) {
      if (is_running == false) {
        break;
      }

      // Increase the counter
      abort_count_ref++;

      // Increase abort counter for others
      UpdateCommitAbortCouter(ret_query, del_abort_count_ref,
                              acc_abort_count_ref, new_abort_count_ref,
                              sub_abort_count_ref, ins_abort_count_ref,
                              upl_abort_count_ref, ups_abort_count_ref);

      switch (state.scheduler) {
        case SCHEDULER_TYPE_NONE: {
          // We do nothing in this case.Just delete the query
          // Since we discard the txn, do not record the throughput and delay
          goto program_end;
        }
        case SCHEDULER_TYPE_CONTROL:
        case SCHEDULER_TYPE_CONFLICT_LEANING:
        case SCHEDULER_TYPE_CLUSTER:
        case SCHEDULER_TYPE_CONFLICT_RANGE:
        case SCHEDULER_TYPE_ABORT_QUEUE:
        case SCHEDULER_TYPE_CONFLICT_DETECT: {
          // Note: otherwise it will cause failed insert continuly execute
          if (ret_query->GetTxnType() == TXN_TYPE_INSERT_CALL_FORWARDING) {
            goto program_end;
          }

          // Control: The txn re-executed immediately
          break;
        }
        case SCHEDULER_TYPE_HASH: {
          // Note: otherwise it will cause failed insert continuly execute
          if (ret_query->GetTxnType() == TXN_TYPE_INSERT_CALL_FORWARDING) {
            goto program_end;
          }

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

      _mm_pause();
    }  // end if execute fail

    /////////////////////////////////////////////////
    // Execute success: the memory should be deleted
    /////////////////////////////////////////////////

    // First compute the delay
    ret_query->RecordDelay(delay_total_ref, delay_max_ref, delay_min_ref);

    // Update txn type
    UpdateDelayCounter(ret_query, del_delay_ref, acc_delay_ref, new_delay_ref,
                       sub_delay_ref, ins_delay_ref, upl_delay_ref,
                       ups_delay_ref);

    // For execution time
    ret_query->RecordExetime(exe_total_ref);

    // Increase the counter
    commit_count_ref++;

    // Increase abort counter for others
    UpdateCommitAbortCouter(ret_query, del_commit_count_ref,
                            acc_commit_count_ref, new_commit_count_ref,
                            sub_commit_count_ref, ins_commit_count_ref,
                            upl_commit_count_ref, ups_commit_count_ref);

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
  std::chrono::system_clock::time_point start_time =
      std::chrono::system_clock::now();

  // used for each round
  std::chrono::system_clock::time_point delay_start_time =
      std::chrono::system_clock::now();

  while (true) {
    if (is_running == false) {
      break;
    }

    if (EnqueueCachedUpdate(delay_start_time, thread_id) == false) {
      _mm_pause();
      continue;
    }
    generate_count_ref++;

    // For OOAHSH continue
    if (state.run_continue && state.log_table) {
      if (generate_count_ref >= state.random_assign) {
        std::cout << "Already generate txns: " << generate_count_ref
                  << std::endl;
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
      std::chrono::system_clock::time_point now_time =
          std::chrono::system_clock::now();

      int elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(
          now_time - start_time).count();

      std::cout << "elapsed_time: " << elapsed_time << std::endl;

      // If elapsed time is still less than 1 second, sleep the rest of the time
      if ((elapsed_time) < 1000) {
        SleepMilliseconds(1000 - (elapsed_time));

        // Reset start time
        start_time = std::chrono::system_clock::now();
      }
      // Otherwise, if elapsed time is larger then 1 second, re-set start time
      // and continue to enqueue txns
      else {
        // Rest start time
        start_time = std::chrono::system_clock::now();
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

  stock_level_count = 0;
  stock_level_avg_latency = 0.0;

  order_status_count = 0;
  order_status_avg_latency = 0.0;

  scan_stock_count = 0;
  scan_stock_avg_latency = 0.0;

  generate_counts = new oid_t[num_generate];
  memset(generate_counts, 0, sizeof(oid_t) * num_generate);

  assign_delays = new uint64_t[num_generate];
  memset(assign_delays, 0, sizeof(uint64_t) * num_generate);

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
  del_abort_counts = new oid_t[num_threads];
  memset(del_abort_counts, 0, sizeof(oid_t) * num_threads);
  del_commit_counts = new oid_t[num_threads];
  memset(del_commit_counts, 0, sizeof(oid_t) * num_threads);
  del_delays = new uint64_t[num_threads];
  memset(del_delays, 0, sizeof(uint64_t) * num_threads);

  // acc
  acc_abort_counts = new oid_t[num_threads];
  memset(acc_abort_counts, 0, sizeof(oid_t) * num_threads);
  acc_commit_counts = new oid_t[num_threads];
  memset(acc_commit_counts, 0, sizeof(oid_t) * num_threads);
  acc_delays = new uint64_t[num_threads];
  memset(acc_delays, 0, sizeof(uint64_t) * num_threads);

  new_abort_counts = new oid_t[num_threads];
  memset(new_abort_counts, 0, sizeof(oid_t) * num_threads);
  new_commit_counts = new oid_t[num_threads];
  memset(new_commit_counts, 0, sizeof(oid_t) * num_threads);
  new_delays = new uint64_t[num_threads];
  memset(new_delays, 0, sizeof(uint64_t) * num_threads);

  sub_abort_counts = new oid_t[num_threads];
  memset(sub_abort_counts, 0, sizeof(oid_t) * num_threads);
  sub_commit_counts = new oid_t[num_threads];
  memset(sub_commit_counts, 0, sizeof(oid_t) * num_threads);
  sub_delays = new uint64_t[num_threads];
  memset(sub_delays, 0, sizeof(uint64_t) * num_threads);

  ins_abort_counts = new oid_t[num_threads];
  memset(ins_abort_counts, 0, sizeof(oid_t) * num_threads);
  ins_commit_counts = new oid_t[num_threads];
  memset(ins_commit_counts, 0, sizeof(oid_t) * num_threads);
  ins_delays = new uint64_t[num_threads];
  memset(ins_delays, 0, sizeof(uint64_t) * num_threads);

  upl_abort_counts = new oid_t[num_threads];
  memset(upl_abort_counts, 0, sizeof(oid_t) * num_threads);
  upl_commit_counts = new oid_t[num_threads];
  memset(upl_commit_counts, 0, sizeof(oid_t) * num_threads);
  upl_delays = new uint64_t[num_threads];
  memset(upl_delays, 0, sizeof(uint64_t) * num_threads);

  ups_abort_counts = new oid_t[num_threads];
  memset(ups_abort_counts, 0, sizeof(oid_t) * num_threads);
  ups_commit_counts = new oid_t[num_threads];
  memset(ups_commit_counts, 0, sizeof(oid_t) * num_threads);
  ups_delays = new uint64_t[num_threads];
  memset(ups_delays, 0, sizeof(uint64_t) * num_threads);

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
      // state.snapshot_memory.push_back(manager.GetLastTileGroupId());
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

  // Join the threads with the main thread
  for (oid_t thread_itr = 0; thread_itr < num_threads + num_generate;
       ++thread_itr) {
    thread_group[thread_itr].join();
  }

  std::cout << "Is running: " << is_running << std::endl;

  // calculate the generate rate
  oid_t total_generate_count = 0;
  for (size_t i = 0; i < num_generate; ++i) {
    total_generate_count += generate_counts[i];
  }
  state.generate_rate = total_generate_count * 1.0 / state.duration;

  // calculate the generate rate
  uint64_t total_assign_delay = 0;
  for (size_t i = 0; i < num_generate; ++i) {
    total_assign_delay += assign_delays[i];
  }
  state.assign_delay = total_assign_delay * 1.0 / total_generate_count;

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

  oid_t total_steal_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_steal_count += steal_counts[i];
  }

  state.steal = total_steal_count * 1.0 / state.duration;
  state.steal_rate = total_steal_count * 1.0 / total_commit_count;

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

  state.throughput = total_commit_count * 1.0 / state.duration;
  state.abort_rate =
      total_abort_count * 1.0 / (total_commit_count + total_abort_count);
  // state.abort_rate = total_abort_count * 1.0 / total_commit_count;

  // calculate the average delay: ms
  uint64_t total_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_delay += delay_totals[i];
  }
  state.delay_ave = (total_delay * 1.0) / (total_commit_count * 1000);

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

  // calculate the exe time: ms
  uint64_t total_exe = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_exe += exe_totals[i];
  }
  state.exe_time = (total_exe * 1.0) / (total_commit_count * 1000);

  // del
  oid_t total_del_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_del_commit_count += del_commit_counts[i];
  }
  oid_t total_del_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_del_abort_count += del_abort_counts[i];
  }
  uint64_t total_del_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_del_delay += del_delays[i];
  }

  state.del_throughput = total_del_commit_count * 1.0 / state.duration;
  state.del_abort_rate = total_del_abort_count * 1.0 /
                         (total_del_commit_count + total_del_abort_count);
  state.del_delay = total_del_delay * 1.0 / (total_del_commit_count * 1000);

  // acc
  oid_t total_acc_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_acc_commit_count += acc_commit_counts[i];
  }
  oid_t total_acc_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_acc_abort_count += acc_abort_counts[i];
  }
  uint64_t total_acc_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_acc_delay += acc_delays[i];
  }

  state.acc_throughput = total_acc_commit_count * 1.0 / state.duration;
  state.acc_abort_rate = total_acc_abort_count * 1.0 /
                         (total_acc_commit_count + total_acc_abort_count);
  state.acc_delay = total_acc_delay * 1.0 / (total_acc_commit_count * 1000);

  // new
  oid_t total_new_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_new_commit_count += new_commit_counts[i];
  }
  oid_t total_new_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_new_abort_count += new_abort_counts[i];
  }
  uint64_t total_new_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_new_delay += new_delays[i];
  }

  state.new_throughput = total_new_commit_count * 1.0 / state.duration;
  state.new_abort_rate = total_new_abort_count * 1.0 /
                         (total_new_commit_count + total_new_abort_count);
  state.new_delay = total_new_delay * 1.0 / (total_new_commit_count * 1000);

  // sub
  oid_t total_sub_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_sub_commit_count += sub_commit_counts[i];
  }
  oid_t total_sub_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_sub_abort_count += sub_abort_counts[i];
  }
  uint64_t total_sub_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_sub_delay += sub_delays[i];
  }

  state.sub_throughput = total_sub_commit_count * 1.0 / state.duration;
  state.sub_abort_rate = total_sub_abort_count * 1.0 /
                         (total_sub_commit_count + total_sub_abort_count);
  state.sub_delay = total_sub_delay * 1.0 / (total_sub_commit_count * 1000);

  std::cout << "sub_delay_total: " << total_sub_delay
            << ". total_sub_commit_count: " << total_sub_commit_count
            << std::endl;

  // ins
  oid_t total_ins_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_ins_commit_count += ins_commit_counts[i];
  }
  oid_t total_ins_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_ins_abort_count += ins_abort_counts[i];
  }
  uint64_t total_ins_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_ins_delay += ins_delays[i];
  }

  state.ins_throughput = total_ins_commit_count * 1.0 / state.duration;
  state.ins_abort_rate = total_ins_abort_count * 1.0 /
                         (total_ins_commit_count + total_ins_abort_count);
  state.ins_delay = total_ins_delay * 1.0 / (total_ins_commit_count * 1000);

  // upl
  oid_t total_upl_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_upl_commit_count += upl_commit_counts[i];
  }
  oid_t total_upl_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_upl_abort_count += upl_abort_counts[i];
  }
  uint64_t total_upl_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_upl_delay += upl_delays[i];
  }

  state.upl_throughput = total_upl_commit_count * 1.0 / state.duration;
  state.upl_abort_rate = total_upl_abort_count * 1.0 /
                         (total_upl_commit_count + total_upl_abort_count);
  state.upl_delay = total_upl_delay * 1.0 / (total_upl_commit_count * 1000);

  // ins
  oid_t total_ups_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_ups_commit_count += ups_commit_counts[i];
  }
  oid_t total_ups_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_ups_abort_count += ups_abort_counts[i];
  }
  uint64_t total_ups_delay = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_ups_delay += ups_delays[i];
  }

  state.ups_throughput = total_ups_commit_count * 1.0 / state.duration;
  state.ups_abort_rate = total_ups_abort_count * 1.0 /
                         (total_ups_commit_count + total_ups_abort_count);
  state.ups_delay = total_ups_delay * 1.0 / (total_ups_commit_count * 1000);

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

  delete[] assign_delays;
  assign_delays = nullptr;

  delete[] delay_totals;
  delay_totals = nullptr;
  delete[] delay_maxs;
  delay_maxs = nullptr;
  delete[] delay_mins;
  delay_mins = nullptr;

  delete[] exe_totals;
  exe_totals = nullptr;

  // For others
  delete[] del_abort_counts;
  del_abort_counts = nullptr;
  delete[] del_commit_counts;
  del_commit_counts = nullptr;
  delete[] del_delays;
  del_delays = nullptr;

  delete[] acc_abort_counts;
  acc_abort_counts = nullptr;
  delete[] acc_commit_counts;
  acc_commit_counts = nullptr;
  delete[] acc_delays;
  acc_delays = nullptr;

  delete[] new_abort_counts;
  new_abort_counts = nullptr;
  delete[] new_commit_counts;
  new_commit_counts = nullptr;
  delete[] new_delays;
  new_delays = nullptr;

  delete[] sub_abort_counts;
  sub_abort_counts = nullptr;
  delete[] sub_commit_counts;
  sub_commit_counts = nullptr;
  delete[] sub_delays;
  sub_delays = nullptr;

  delete[] ins_abort_counts;
  ins_abort_counts = nullptr;
  delete[] ins_commit_counts;
  ins_commit_counts = nullptr;
  delete[] ins_delays;
  ins_delays = nullptr;

  delete[] upl_abort_counts;
  upl_abort_counts = nullptr;
  delete[] upl_commit_counts;
  upl_commit_counts = nullptr;
  delete[] upl_delays;
  upl_delays = nullptr;

  delete[] ups_abort_counts;
  ups_abort_counts = nullptr;
  delete[] ups_commit_counts;
  ups_commit_counts = nullptr;
  delete[] ups_delays;
  ups_delays = nullptr;

  LOG_INFO("============TABLE SIZES==========");
  LOG_INFO("subscriber count = %u",
           subscriber_table->GetAllCurrentTupleCount());
  LOG_INFO("access_info count = %u",
           access_info_table->GetAllCurrentTupleCount());
  LOG_INFO("special_facility count  = %u",
           special_facility_table->GetAllCurrentTupleCount());
  LOG_INFO("call_forwarding count = %u",
           call_forwarding_table->GetAllCurrentTupleCount());
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

void ExecuteInsertTest(executor::AbstractExecutor *executor) {

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
