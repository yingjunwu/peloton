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

namespace peloton {
namespace benchmark {
namespace smallbank {

#define PRELOAD 2000000  // 2000,000
#define CLUSTER_ANALYSIS_TXN 10000
#define CONFLICT_THRESHHOLD 0
#define LOGTABLE "logtable"

static const oid_t smallbank_database_oid = 100;

static const oid_t accounts_table_oid = 1001;
static const oid_t accounts_table_pkey_index_oid = 20010;  // CUSTID

static const oid_t savings_table_oid = 1002;
static const oid_t savings_table_pkey_index_oid = 20021;  // CUSTID

static const oid_t checking_table_oid = 1003;
static const oid_t checking_table_pkey_index_oid = 20030;  // CUSTID

extern const size_t BASIC_ACCOUNTS;
extern size_t NUM_ACCOUNTS;

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

  // number of query thread
  int generate_count;

  // number of txn / second
  int generate_speed;

  // For cluster analysis
  int min_pts;
  int analysis_txns;

  // running txns for reference counting
  int running_ref;

  std::vector<double> snapshot_throughput;

  std::vector<double> snapshot_abort_rate;

  std::vector<int> snapshot_memory;

  double steal;
  double steal_rate;

  double throughput1;
  double abort_rate1;

  double throughput2;
  double abort_rate2;

  double generate_rate;
  double delay_ave;
  double delay_max;
  double delay_min;

  double ama_throughput;
  double ama_abort_rate;
  double ama_delay;

  double bal_throughput;
  double bal_abort_rate;
  double bal_delay;

  double dep_throughput;
  double dep_abort_rate;
  double dep_delay;

  double tra_throughput;
  double tra_abort_rate;
  double tra_delay;

  double wri_throughput;
  double wri_abort_rate;
  double wri_delay;

  double exe_time;

  double new_order_throughput;

  double new_order_abort_rate;

  double stock_level_latency;

  double order_status_latency;

  double scan_stock_latency;

  // Theta in zipf distribution to control skewness
  double zipf_theta;

  // data skew
  int hot_spot;

  // enable exponential backoff
  bool run_backoff;

  // enable client affinity
  bool run_affinity;

  bool run_continue;

  bool online;
  bool offline;

  bool single_ref;

  // canonical means transform to underlying domain
  bool canonical;

  bool log_table;

  // update run table with lock-free
  bool lock_free;

  // use fraction to express conflict
  bool fraction;

  bool pure_balance;
  //
  SchedulerType scheduler;

  // protocol type
  ConcurrencyType protocol;

  // gc protocol type
  GCType gc_protocol;

  // index type
  IndexType index;

  // secondary index type
  SecondaryIndexType sindex;

  // number of threads used in GC,
  // Only available when gc type is n2o and va
  int gc_thread_count;
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

void ValidateOrderRange(const configuration &state);

void ParseArguments(int argc, char *argv[], configuration &state);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
