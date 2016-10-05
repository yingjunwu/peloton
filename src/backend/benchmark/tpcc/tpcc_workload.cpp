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
namespace tpcc {

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

#define STOCK_LEVEL_RATIO     0.04
#define ORDER_STATUS_RATIO    0.04
#define DELIVERY_RATIO        0.00
#define PAYMENT_RATIO         0.46

volatile bool is_running = true;

oid_t *abort_counts;
oid_t *commit_counts;
double *commit_latencies;

oid_t *payment_abort_counts;
oid_t *payment_commit_counts;

oid_t *new_order_abort_counts;
oid_t *new_order_commit_counts;

oid_t *delivery_abort_counts;
oid_t *delivery_commit_counts;

oid_t *stock_level_abort_counts;
oid_t *stock_level_commit_counts;

oid_t *order_status_abort_counts;
oid_t *order_status_commit_counts;

oid_t stock_level_count;
double stock_level_avg_latency;

oid_t order_status_count;
double order_status_avg_latency;

oid_t scan_stock_count;
double scan_stock_avg_latency;

size_t GenerateWarehouseId(const size_t &thread_id) {
  if (state.run_affinity) {
    if (state.warehouse_count <= state.backend_count) {
      return thread_id % state.warehouse_count;
    } else {
      int warehouse_per_partition = state.warehouse_count / state.backend_count;
      int start_warehouse = warehouse_per_partition * thread_id;
      int end_warehouse = ((int)thread_id != (state.backend_count - 1)) ? 
        start_warehouse + warehouse_per_partition - 1 : state.warehouse_count - 1;
      return GetRandomInteger(start_warehouse, end_warehouse);
    }
  } else {
    return GetRandomInteger(0, state.warehouse_count - 1);
  }
}

void RunScanBackend(oid_t thread_id) {
  PinToCore(thread_id);

  bool slept = false;
  auto SLEEP_TIME = std::chrono::milliseconds(500);

  // backoff
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
    RunScanStock();
    if (thread_id == 0) {
      std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
      double diff = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
      scan_stock_avg_latency = (scan_stock_avg_latency * scan_stock_count + diff) / (scan_stock_count + 1);
      scan_stock_count++;
    }
  }
}


void RunBackend(oid_t thread_id) {
  PinToCore(thread_id);

  auto &log_manager = logging::DurabilityFactory::GetLoggerInstance();
  log_manager.RegisterWorkerToLogger();

  oid_t &execution_count_ref = abort_counts[thread_id];
  oid_t &transaction_count_ref = commit_counts[thread_id];
  double &commit_latency_ref = commit_latencies[thread_id];

  NewOrderPlans new_order_plans = PrepareNewOrderPlan();
  PaymentPlans payment_plans = PreparePaymentPlan();
  DeliveryPlans delivery_plans = PrepareDeliveryPlan();
  
  // backoff
  uint32_t backoff_shifts = 0;

  // bool slept = false;
  // auto SLEEP_TIME = std::chrono::milliseconds(100);

  oid_t &payment_execution_count_ref = payment_abort_counts[thread_id];
  oid_t &payment_transaction_count_ref = payment_commit_counts[thread_id];

  oid_t &new_order_execution_count_ref = new_order_abort_counts[thread_id];
  oid_t &new_order_transaction_count_ref = new_order_commit_counts[thread_id];

  oid_t &delivery_execution_count_ref = delivery_abort_counts[thread_id];
  oid_t &delivery_transaction_count_ref = delivery_commit_counts[thread_id];

  oid_t &stock_level_execution_count_ref = stock_level_abort_counts[thread_id];
  oid_t &stock_level_transaction_count_ref = stock_level_commit_counts[thread_id];

  oid_t &order_status_execution_count_ref = order_status_abort_counts[thread_id];
  oid_t &order_status_transaction_count_ref = order_status_commit_counts[thread_id];

  while (true) {

    if (is_running == false) {
      break;
    }

    fast_random rng(rand());
    
    auto rng_val = rng.next_uniform();
    if (rng_val <= STOCK_LEVEL_RATIO) {
      std::chrono::steady_clock::time_point start_time;
      if (thread_id == 0) {
        start_time = std::chrono::steady_clock::now();
      }
      while (RunStockLevel(thread_id, state.order_range) == false) {
        if (is_running == false) {
          break;
        }
        execution_count_ref++;
        stock_level_execution_count_ref++;
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
      stock_level_transaction_count_ref++;
      if (thread_id == 0) {
        std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
        double diff = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        stock_level_avg_latency = (stock_level_avg_latency * stock_level_count + diff) / (stock_level_count + 1);
        stock_level_count++;
      }
    } else if (rng_val <= ORDER_STATUS_RATIO + STOCK_LEVEL_RATIO) {
      std::chrono::steady_clock::time_point start_time;
      if (thread_id == 0) {
        start_time = std::chrono::steady_clock::now();
      }
       while (RunOrderStatus(thread_id) == false) {
          if (is_running == false) {
            break;
          }
         execution_count_ref++;
         order_status_execution_count_ref++;
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
       order_status_transaction_count_ref++;
       if (thread_id == 0) {
          std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
          double diff = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
          order_status_avg_latency = (order_status_avg_latency * order_status_count + diff) / (order_status_count + 1);
          order_status_count++;
       }
     } 
     else if (rng_val <= DELIVERY_RATIO + ORDER_STATUS_RATIO + STOCK_LEVEL_RATIO) {
       while (RunDelivery(delivery_plans, thread_id) == false) {
          if (is_running == false) {
            break;
          }
         execution_count_ref++;
         delivery_execution_count_ref++;
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
       delivery_transaction_count_ref++;
     } 
      else if (rng_val <= PAYMENT_RATIO + DELIVERY_RATIO + ORDER_STATUS_RATIO + STOCK_LEVEL_RATIO) {
       while (RunPayment(payment_plans, thread_id) == false) {
          if (is_running == false) {
            break;
          }
         execution_count_ref++;
         payment_execution_count_ref++;
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
       payment_transaction_count_ref++;
     } else {
       while (RunNewOrder(new_order_plans, thread_id) == false) {
          if (is_running == false) {
            break;
          }
         execution_count_ref++;
         new_order_execution_count_ref++;
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
       new_order_transaction_count_ref++;
     }

    backoff_shifts >>= 1;
    transaction_count_ref++;

  }

  if (logging::DurabilityFactory::GetLoggingType() == LOGGING_TYPE_PHYLOG) {
    commit_latency_ref = logging::tl_phylog_worker_ctx->txn_summary.GetAverageLatencyInMs();
  }

  log_manager.DeregisterWorkerFromLogger();
}

void RunWorkload() {

  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  oid_t num_scan_threads = state.scan_backend_count;
  
  abort_counts = new oid_t[num_threads];
  memset(abort_counts, 0, sizeof(oid_t) * num_threads);

  commit_counts = new oid_t[num_threads];
  memset(commit_counts, 0, sizeof(oid_t) * num_threads);

  commit_latencies = new double[num_threads];
  memset(commit_latencies, 0, sizeof(double) * num_threads);

  payment_abort_counts = new oid_t[num_threads];
  memset(payment_abort_counts, 0, sizeof(oid_t) * num_threads);

  payment_commit_counts = new oid_t[num_threads];
  memset(payment_commit_counts, 0, sizeof(oid_t) * num_threads);

  new_order_abort_counts = new oid_t[num_threads];
  memset(new_order_abort_counts, 0, sizeof(oid_t) * num_threads);

  new_order_commit_counts = new oid_t[num_threads];
  memset(new_order_commit_counts, 0, sizeof(oid_t) * num_threads);

  delivery_abort_counts = new oid_t[num_threads];
  memset(delivery_abort_counts, 0, sizeof(oid_t) * num_threads);

  delivery_commit_counts = new oid_t[num_threads];
  memset(delivery_commit_counts, 0, sizeof(oid_t) * num_threads);

  stock_level_abort_counts = new oid_t[num_threads];
  memset(stock_level_abort_counts, 0, sizeof(oid_t) * num_threads);

  stock_level_commit_counts = new oid_t[num_threads];
  memset(stock_level_commit_counts, 0, sizeof(oid_t) * num_threads);

  order_status_abort_counts = new oid_t[num_threads];
  memset(order_status_abort_counts, 0, sizeof(oid_t) * num_threads);

  order_status_commit_counts = new oid_t[num_threads];
  memset(order_status_commit_counts, 0, sizeof(oid_t) * num_threads);

  stock_level_count = 0;
  stock_level_avg_latency = 0.0;

  order_status_count = 0;
  order_status_avg_latency = 0.0;

  scan_stock_count = 0;
  scan_stock_avg_latency = 0.0;

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
  for (oid_t thread_itr = 0; thread_itr < num_scan_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunScanBackend, thread_itr)));
  }

  for (oid_t thread_itr = num_scan_threads; thread_itr < num_threads; ++thread_itr) {
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

  oid_t total_payment_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_payment_commit_count += payment_commit_counts[i];
  }

  oid_t total_payment_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_payment_abort_count += payment_abort_counts[i];
  }

  state.payment_throughput = total_payment_commit_count * 1.0 / state.duration;
  state.payment_abort_rate = total_payment_abort_count * 1.0 / total_payment_commit_count;

  oid_t total_new_order_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_new_order_commit_count += new_order_commit_counts[i];
  }

  oid_t total_new_order_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_new_order_abort_count += new_order_abort_counts[i];
  }

  state.new_order_throughput = total_new_order_commit_count * 1.0 / state.duration;
  state.new_order_abort_rate = total_new_order_abort_count * 1.0 / total_new_order_commit_count;

  oid_t total_delivery_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_delivery_commit_count += delivery_commit_counts[i];
  }

  oid_t total_delivery_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_delivery_abort_count += delivery_abort_counts[i];
  }

  state.delivery_throughput = total_delivery_commit_count * 1.0 / state.duration;
  state.delivery_abort_rate = total_delivery_abort_count * 1.0 / total_delivery_commit_count;


  oid_t total_stock_level_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_stock_level_commit_count += stock_level_commit_counts[i];
  }

  oid_t total_stock_level_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_stock_level_abort_count += stock_level_abort_counts[i];
  }

  state.stock_level_throughput = total_stock_level_commit_count * 1.0 / state.duration;
  state.stock_level_abort_rate = total_stock_level_abort_count * 1.0 / total_stock_level_commit_count;


  oid_t total_order_status_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_order_status_commit_count += order_status_commit_counts[i];
  }

  oid_t total_order_status_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_order_status_abort_count += order_status_abort_counts[i];
  }

  state.order_status_throughput = total_order_status_commit_count * 1.0 / state.duration;
  state.order_status_abort_rate = total_order_status_abort_count * 1.0 / total_order_status_commit_count;


  state.stock_level_latency = stock_level_avg_latency;
  state.order_status_latency = order_status_avg_latency;
  state.scan_stock_latency = scan_stock_avg_latency;

  state.commit_latency = 0.0;
  for (size_t i = 0; i < num_threads; ++i) {
    // Weighted avg
    state.commit_latency += (commit_latencies[i] * (commit_counts_snapshots[snapshot_round - 1][i] * 1.0 / total_commit_count));
  }

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

  delete[] payment_abort_counts;
  payment_abort_counts = nullptr;
  delete[] payment_commit_counts;
  payment_commit_counts = nullptr;

  delete[] new_order_abort_counts;
  new_order_abort_counts = nullptr;
  delete[] new_order_commit_counts;
  new_order_commit_counts = nullptr;

  delete[] delivery_abort_counts;
  delivery_abort_counts = nullptr;
  delete[] delivery_commit_counts;
  delivery_commit_counts = nullptr;

  delete[] stock_level_abort_counts;
  stock_level_abort_counts = nullptr;
  delete[] stock_level_commit_counts;
  stock_level_commit_counts = nullptr;

  delete[] order_status_abort_counts;
  order_status_abort_counts = nullptr;
  delete[] order_status_commit_counts;
  order_status_commit_counts = nullptr;

  // LOG_INFO("============TABLE SIZES==========");
  // LOG_INFO("warehouse count = %u", warehouse_table->GetAllCurrentTupleCount());
  // LOG_INFO("district count  = %u", district_table->GetAllCurrentTupleCount());
  // LOG_INFO("item count = %u", item_table->GetAllCurrentTupleCount());
  // LOG_INFO("customer count = %u", customer_table->GetAllCurrentTupleCount());
  // LOG_INFO("history count = %u", history_table->GetAllCurrentTupleCount());
  // LOG_INFO("stock count = %u", stock_table->GetAllCurrentTupleCount());
  // LOG_INFO("orders count = %u", orders_table->GetAllCurrentTupleCount());
  // LOG_INFO("new order count = %u", new_order_table->GetAllCurrentTupleCount());
  // LOG_INFO("order line count = %u", order_line_table->GetAllCurrentTupleCount());
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

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton