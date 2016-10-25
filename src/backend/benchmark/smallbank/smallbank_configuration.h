//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.h
//
// Identification: benchmark/smallbank/configuration.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "backend/common/types.h"

#include "backend/benchmark/benchmark_common.h"

namespace peloton {
namespace benchmark {
namespace smallbank {

static const oid_t smallbank_database_oid = 100;

static const oid_t accounts_table_oid = 1001;
static const oid_t accounts_table_pkey_index_oid = 20010;  // CUSTID

static const oid_t savings_table_oid = 1002;
static const oid_t savings_table_pkey_index_oid = 20021;  // CUSTID

static const oid_t checking_table_oid = 1003;
static const oid_t checking_table_pkey_index_oid = 20030;  // CUSTID

extern size_t NUM_ACCOUNTS;

class configuration {
 public:

  // scale factor
  double scale_factor;

  // execution duration
  double duration;

  // snapshot duration
  double snapshot_duration;

  // number of accounts
  int account_count;

  // number of backends
  int backend_count;

  std::vector<double> snapshot_throughput;

  std::vector<double> snapshot_abort_rate;

  std::vector<int> snapshot_memory;

  double throughput;

  double abort_rate;

  // enable exponential backoff
  bool run_backoff;

  // protocol type
  ConcurrencyType protocol;

  // gc protocol type
  GCType gc_protocol;

  // index type
  IndexType index;

  // secondary index type
  SecondaryIndexType sindex;

  // Logging type
  LoggingType logging_type;

  // Log directories
  std::vector<std::string> log_directories;

  // Checkpoint type
  CheckpointType checkpoint_type;

  // Checkpoint directories
  std::vector<std::string> checkpoint_directories;

  int checkpoint_interval;
  
  // number of threads used in GC,
  // Only available when gc type is n2o and va
  int gc_thread_count;

  int epoch_length;

  double adhoc_ratio;

  double commit_latency;
  LatSummary latency_summary;

  // Timer on
  TimerType timer_type;

  EpochType epoch_type;

  bool recover_checkpoint;

  bool replay_log;

  int recover_checkpoint_num = 1;
  
  int replay_log_num = 1;
};

extern configuration state;

void Usage(FILE *out);

void ValidateScaleFactor(const configuration &state);

void ValidateDuration(const configuration &state);

void ValidateSnapshotDuration(const configuration &state);

void ValidateBackendCount(const configuration &state);

void ValidateProtocol(const configuration &state);

void ValidateIndex(const configuration &state);

void ValidateEpoch(const configuration &state);

void ValidateEpochType(configuration &state);

void ValidateLoggingType(configuration &state);

void ValidateAdhocRatio(const configuration &state);

void ParseArguments(int argc, char *argv[], configuration &state);

void WriteOutput();

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton