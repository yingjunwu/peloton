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
#include "backend/logging/durability_factory.h"

#include "backend/common/logger.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

configuration state;


// Main Entry Point
void RunBenchmark() {

  auto epoch_type = EPOCH_SINGLE_QUEUE;
  if (state.gc_protocol == GC_TYPE_N2O_SNAPSHOT) {
    epoch_type = EPOCH_SNAPSHOT;
  }
  concurrency::EpochManagerFactory::Configure(epoch_type, state.epoch_length);
  // Force init
  // TODO: We should force the init order of singleton -- Jiexi

  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  (void) epoch_manager;

  gc::GCManagerFactory::Configure(state.gc_protocol, state.gc_thread_count);
  concurrency::TransactionManagerFactory::Configure(state.protocol);
  index::IndexFactory::Configure(state.sindex);

  // Create and load the database
  CreateTPCCDatabase();
  LoadTPCCDatabase();

  logging::DurabilityFactory::Configure(state.logging_type, CHECKPOINT_TYPE_INVALID, state.timer_type);

  auto &log_manager = logging::DurabilityFactory::GetLoggerInstance();
  log_manager.SetDirectories(state.log_directories);
  log_manager.StartLoggers();

  // Run the workload
  RunWorkload();

  log_manager.StopLoggers();

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