//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.h
//
// Identification: benchmark/ycsb/configuration.h
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
namespace ycsb {

static const oid_t ycsb_database_oid = 100;

static const oid_t user_table_oid = 1001;

static const oid_t user_table_pkey_index_oid = 2001;

static const oid_t ycsb_field_length = 100;

static const oid_t ycsb_table_sindex_begin_oid = 3000;

class configuration {
 public:
  // size of the table
  int scale_factor;

  // column count
  int column_count;

  // update column count
  int update_column_count;

  // read column count
  int read_column_count;

  // operation count
  int operation_count;

  // number of scan backends
  int scan_backend_count;

  // scan mock duration
  int scan_mock_duration;

  // number of read-only backends
  int ro_backend_count;

  // update ratio
  double update_ratio;

  // execution duration
  double duration;

  // snapshot duration
  double snapshot_duration;

  // number of backends
  int backend_count;

  // number of secondary index
  int sindex_count;

  std::vector<double> snapshot_throughput;

  std::vector<double> snapshot_abort_rate;

  std::vector<int> snapshot_memory;

  double throughput = 0;

  double abort_rate = 0;

  double ro_throughput = 0;

  double ro_abort_rate = 0;

  double scan_latency = 0;

  // Theta in zipf distribution to control skewness
  double zipf_theta;

  // enable declared read-only transaction
  bool declared;

  // enable exponential backoff
  bool run_backoff;

  // enable blind write
  bool blind_write;

  // use secondary index to scan
  bool sindex_scan;

  // Timer on
  TimerType timer_type;

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

  double epoch_length;

  double commit_latency;

  LatSummary latency_summary;

  int ro_sleep_between_txn;

  EpochType epoch_type;

};

extern configuration state;

void Usage(FILE *out);

void ValidateScaleFactor(const configuration &state);

void ValidateColumnCount(const configuration &state);

void ValidateUpdateColumnCount(const configuration &state);

void ValidateReadColumnCount(const configuration &state);

void ValidateOperationCount(const configuration &state);

void ValidateUpdateRatio(const configuration &state);

void ValidateBackendCount(const configuration &state);

void ValidateScanMockDuration(const configuration &state);

void ValidateDuration(const configuration &state);

void ValidateSnapshotDuration(const configuration &state);

void ValidateProtocol(const configuration &state);

void ValidateIndex(const configuration &state);

void ValidateEpoch(const configuration &state);

void ValidateZipfTheta(const configuration &state);

void ValidateSecondaryIndex(const configuration &state);

void ValidateSecondaryIndexScan(const configuration &state);

void ValidateRoSleepInterval(const configuration &state);

void ValidateEpochType(configuration &state);

void ParseArguments(int argc, char *argv[], configuration &state);

void WriteOutput();

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
