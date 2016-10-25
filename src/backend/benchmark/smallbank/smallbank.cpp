//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// smallbank.cpp
//
// Identification: benchmark/smallbank/smallbank.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>
#include <iomanip>

#include "backend/benchmark/smallbank/smallbank_configuration.h"
#include "backend/benchmark/smallbank/smallbank_loader.h"
//#include "backend/benchmark/smallbank/smallbank_workload.h"

#include "backend/gc/gc_manager_factory.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/logging/durability_factory.h"

#include "backend/common/logger.h"
#include "backend/benchmark/smallbank/smallbank_command_log_manager.h"

namespace peloton {
namespace benchmark {
namespace smallbank {

configuration state;


// Main Entry Point
void RunBenchmark() {

  if (state.replay_log == true) {
    CreateSmallbankDatabase();
    
    logging::DurabilityFactory::Configure(state.logging_type, state.checkpoint_type, state.timer_type);
    
    if (state.logging_type == LOGGING_TYPE_COMMAND) {
      // auto &log_manager = SmallbankCommandLogManager::GetInstance();
      // log_manager.SetDirectories(state.log_directories);
      // log_manager.SetRecoveryThreadCount(state.replay_log_num);

      // log_manager.DoRecovery(0);

    } else {
      auto &log_manager = logging::DurabilityFactory::GetLoggerInstance();
      log_manager.SetDirectories(state.log_directories);
      log_manager.SetRecoveryThreadCount(state.replay_log_num);

      log_manager.DoRecovery(0);
    }
    return;
  }

  // perform recovery
  if (state.recover_checkpoint == true) {
    CreateSmallbankDatabase();
    
    logging::DurabilityFactory::Configure(state.logging_type, state.checkpoint_type, state.timer_type);
    auto &checkpoint_manager = logging::DurabilityFactory::GetCheckpointerInstance();
    checkpoint_manager.SetDirectories(state.checkpoint_directories);
    checkpoint_manager.SetRecoveryThreadCount(state.recover_checkpoint_num);

    checkpoint_manager.DoRecovery();

    if (state.replay_log == true) {

      if (state.logging_type == LOGGING_TYPE_COMMAND) {
        // auto &log_manager = SmallbankCommandLogManager::GetInstance();
        // log_manager.SetDirectories(state.log_directories);
        // log_manager.SetRecoveryThreadCount(state.replay_log_num);

        // log_manager.DoRecovery(0);

      } else {
        auto &log_manager = logging::DurabilityFactory::GetLoggerInstance();
        log_manager.SetDirectories(state.log_directories);
        log_manager.SetRecoveryThreadCount(state.replay_log_num);

        log_manager.DoRecovery(0);
      }
    }

    return;
  }

  concurrency::EpochManagerFactory::Configure(state.epoch_type, state.epoch_length);
  // Force init
  // TODO: We should force the init order of singleton -- Jiexi

  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();

  gc::GCManagerFactory::Configure(state.gc_protocol, state.gc_thread_count);
  concurrency::TransactionManagerFactory::Configure(state.protocol);
  index::IndexFactory::Configure(state.sindex);

  // Create and load the database
  epoch_manager.RegisterTxnWorker(false);
  CreateSmallbankDatabase();
  LoadSmallbankDatabase();

  logging::DurabilityFactory::Configure(state.logging_type, CHECKPOINT_TYPE_INVALID, state.timer_type);

  auto &log_manager = logging::DurabilityFactory::GetLoggerInstance();
  log_manager.SetDirectories(state.log_directories);
  log_manager.StartLoggers();

  auto &checkpoint_manager = logging::DurabilityFactory::GetCheckpointerInstance();
  checkpoint_manager.SetCheckpointInterval(state.checkpoint_interval);
  checkpoint_manager.SetDirectories(state.checkpoint_directories);
  checkpoint_manager.StartCheckpointing();
  
  // Run the workload
  // RunWorkload();

  // Stop the logger
  log_manager.StopLoggers();
  checkpoint_manager.StopCheckpointing();
 
  WriteOutput();
}

}  // namespace smallbank
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::smallbank::ParseArguments(
      argc, argv, peloton::benchmark::smallbank::state);

  peloton::benchmark::smallbank::RunBenchmark();

  return 0;
}