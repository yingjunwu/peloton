//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.h
//
// Identification: benchmark/tpcc/configuration.h
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
namespace tpcc {

static const oid_t tpcc_database_oid = 100;

static const oid_t warehouse_table_oid = 1001;
static const oid_t warehouse_table_pkey_index_oid = 20010; // W_ID

static const oid_t district_table_oid = 1002;
static const oid_t district_table_pkey_index_oid = 20021; // D_ID, D_W_ID

static const oid_t item_table_oid = 1003;
static const oid_t item_table_pkey_index_oid = 20030; // I_ID

static const oid_t customer_table_oid = 1004;
static const oid_t customer_table_pkey_index_oid = 20040; // C_W_ID, C_D_ID, C_ID
static const oid_t customer_table_skey_index_oid = 20041; // C_W_ID, C_D_ID, C_LAST

static const oid_t history_table_oid = 1005;

static const oid_t stock_table_oid = 1006;
static const oid_t stock_table_pkey_index_oid = 20060; // S_W_ID, S_I_ID

static const oid_t orders_table_oid = 1007;
static const oid_t orders_table_pkey_index_oid = 20070; // O_W_ID, O_D_ID, O_ID
static const oid_t orders_table_skey_index_oid = 20071; // O_W_ID, O_D_ID, O_C_ID

static const oid_t new_order_table_oid = 1008;
static const oid_t new_order_table_pkey_index_oid = 20080; // NO_D_ID, NO_W_ID, NO_O_ID

static const oid_t order_line_table_oid = 1009;
static const oid_t order_line_table_pkey_index_oid = 20090; // OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER
static const oid_t order_line_table_skey_index_oid = 20091; // OL_W_ID, OL_D_ID, OL_O_ID

class configuration {
 public:

  // scale factor
  double scale_factor;

  // num of warehouses
  int warehouse_count;

  // item count
  int item_count;

  int districts_per_warehouse;

  int customers_per_district;

  int new_orders_per_district;

  int order_range;

  // execution duration
  double duration;

  // snapshot duration
  double snapshot_duration;

  // number of backends
  int backend_count;

  // number of scan backends
  int scan_backend_count;

  std::vector<double> snapshot_throughput;

  std::vector<double> snapshot_abort_rate;

  std::vector<int> snapshot_memory;

  double throughput;

  double abort_rate;


  double payment_throughput;

  double payment_abort_rate;

  double new_order_throughput;

  double new_order_abort_rate;

  double delivery_throughput;

  double delivery_abort_rate;

  double stock_level_throughput;

  double stock_level_abort_rate;

  double order_status_throughput;

  double order_status_abort_rate;


  double stock_level_latency;

  double order_status_latency;

  double scan_stock_latency;

  // enable exponential backoff
  bool run_backoff;

  // enable client affinity
  bool run_affinity;

  bool disable_insert;

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

  double commit_latency;
  LatSummary latency_summary;

  // Timer on
  TimerType timer_type;

  EpochType epoch_type;

  bool recover_checkpoint;

  bool replay_log;

  int recover_checkpoint_num = 1;
  
  int replay_log_num = 1;

  bool normal_txn_for_scan;

  int mock_sleep_millisec;
};

extern configuration state;

void Usage(FILE *out);

void ValidateScaleFactor(const configuration &state);

void ValidateDuration(const configuration &state);

void ValidateSnapshotDuration(const configuration &state);

void ValidateBackendCount(const configuration &state);

void ValidateWarehouseCount(const configuration &state);

void ValidateProtocol(const configuration &state);

void ValidateIndex(const configuration &state);

void ValidateEpoch(const configuration &state);

void ValidateOrderRange(const configuration &state);

void ValidateEpochType(configuration &state);

void ValidateLoggingType(configuration &state);

void ParseArguments(int argc, char *argv[], configuration &state);

void WriteOutput();

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton