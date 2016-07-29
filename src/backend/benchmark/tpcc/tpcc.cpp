//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tpcc.cpp
//
// Identification: benchmark/tpcc/tpcc.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>
#include <iomanip>

#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"
#include "backend/benchmark/tpcc/tpcc_workload.h"

#include "backend/gc/gc_manager_factory.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "backend/common/logger.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

configuration state;

std::ofstream out("outputfile.summary");

static void WriteOutput() {

  oid_t total_snapshot_memory = 0;
  for (auto &entry : state.snapshot_memory) {
    total_snapshot_memory += entry;
  }

  LOG_INFO("%lf tps, %lf, %d", 
    state.throughput, state.abort_rate, 
    total_snapshot_memory);
  LOG_INFO("payment: %lf tps, %lf", state.payment_throughput, state.payment_abort_rate);
  LOG_INFO("new_order: %lf tps, %lf", state.new_order_throughput, state.new_order_abort_rate);
  LOG_INFO("stock_level latency: %lf us", state.stock_level_latency);
  LOG_INFO("order_status latency: %lf us", state.order_status_latency);
  LOG_INFO("scan_stock latency: %lf us", state.scan_stock_latency);

  for (size_t round_id = 0; round_id < state.snapshot_throughput.size();
       ++round_id) {
    out << "[" << std::setw(3) << std::left
        << state.snapshot_duration * round_id << " - " << std::setw(3)
        << std::left << state.snapshot_duration * (round_id + 1)
        << " s]: " << state.snapshot_throughput[round_id] << " "
        << state.snapshot_abort_rate[round_id] << " "
        << state.snapshot_memory[round_id] << "\n";
  }


  out << "scalefactor=" << state.scale_factor << " ";
  out << "warehouse_count=" << state.warehouse_count << " ";
  if (state.protocol == CONCURRENCY_TYPE_OPTIMISTIC) {
    out << "proto=occ ";
  } else if (state.protocol == CONCURRENCY_TYPE_PESSIMISTIC) {
    out << "proto=pcc ";
  } else if (state.protocol == CONCURRENCY_TYPE_SSI) {
    out << "proto=ssi ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO) {
    out << "proto=to ";
  } else if (state.protocol == CONCURRENCY_TYPE_EAGER_WRITE) {
    out << "proto=ewrite ";
  } else if (state.protocol == CONCURRENCY_TYPE_OCC_RB) {
    out << "proto=occrb ";
  } else if (state.protocol == CONCURRENCY_TYPE_OCC_CENTRAL_RB) {
    out << "proto=occ_central_rb ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO_CENTRAL_RB) {
    out << "proto=to_central_rb ";
  } else if (state.protocol == CONCURRENCY_TYPE_SPECULATIVE_READ) {
    out << "proto=sread ";
  } else if (state.protocol == CONCURRENCY_TYPE_OCC_N2O) {
    out << "proto=occn2o ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO_RB) {
    out << "proto=torb ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO_N2O) {
    out << "proto=ton2o ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO_FULL_RB) {
    out << "proto=tofullrb ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO_FULL_CENTRAL_RB) {
    out << "proto=to_full_central_rb ";
  }
  if (state.gc_protocol == GC_TYPE_OFF) {
    out << "gc=off ";
  }else if (state.gc_protocol == GC_TYPE_VACUUM) {
    out << "gc=va ";
  }else if (state.gc_protocol == GC_TYPE_CO) {
    out << "gc=co ";
  }else if (state.gc_protocol == GC_TYPE_N2O) {
    out << "gc=n2o ";
  } else if (state.gc_protocol == GC_TYPE_N2O_TXN) {
    out << "gc=n2otxn ";
  }
  out << "core_cnt=" << state.backend_count << " ";
  if (state.sindex == SECONDARY_INDEX_TYPE_VERSION) {
    out << "sindex=version ";
  } else {
    out << "sindex=tuple ";
  }
  out << "\n";

  out << state.throughput << " ";
  out << state.abort_rate << " ";
  
  out << state.payment_throughput << " ";
  out << state.payment_abort_rate << " ";
  
  out << state.new_order_throughput << " ";
  out << state.new_order_abort_rate << " ";

  out << state.stock_level_latency << " ";
  out << state.order_status_latency << " ";
  out << state.scan_stock_latency << " ";

  out << total_snapshot_memory <<"\n";
  out.flush();
  out.close();
}

// Main Entry Point
void RunBenchmark() {
  gc::GCManagerFactory::Configure(state.gc_protocol, state.gc_thread_count);
  concurrency::TransactionManagerFactory::Configure(state.protocol);
  index::IndexFactory::Configure(state.sindex);

  // Create the database
  CreateTPCCDatabase();

  // Load the database
  LoadTPCCDatabase();

  // Run the workload
  RunWorkload();

  WriteOutput();
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::tpcc::ParseArguments(
      argc, argv, peloton::benchmark::tpcc::state);

  peloton::benchmark::tpcc::RunBenchmark();

  return 0;
}