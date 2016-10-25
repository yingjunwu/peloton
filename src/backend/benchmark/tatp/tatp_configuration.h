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
namespace tatp {

#define PRELOAD 2000000  // 2000,000
#define CLUSTER_ANALYSIS_TXN 10000
#define CONFLICT_THRESHHOLD 0
#define LOGTABLE "logtable"

static const oid_t tatp_database_oid = 100;

static const oid_t subscriber_table_oid = 1001;
static const oid_t subscriber_table_pkey_index_oid = 20010;  // CUSTID

static const oid_t access_info_table_oid = 1002;
static const oid_t access_info_table_pkey_index_oid = 20021;  // sid,type

static const oid_t special_facility_table_oid = 1003;
static const oid_t special_facility_table_pkey_index_oid = 20030;  // sid,type
static const oid_t special_facility_table_skey_index_oid = 20031;  // id
static const oid_t special_facility_table_skey_index_oid2 =
    20032;  // id,type,active

static const oid_t call_forwarding_table_oid = 1004;
static const oid_t call_forwarding_table_pkey_index_oid =
    20040;  // id,type,time
static const oid_t call_forwarding_table_skey_index_oid = 20041;  // id
static const oid_t call_forwarding_table_skey_index_oid2 =
    20042;  // id,type,start_time,end_time

static const oid_t test_sub_table_oid = 1005;
static const oid_t test_sub_table_pkey_index_oid = 20050;  // CUSTID

extern const size_t BASIC_SUBSCRIBERS;
extern size_t NUM_SUBSCRIBERS;

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

  // For continous test
  oid_t random_assign;

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

  double throughput;

  double abort_rate;

  double generate_rate;
  double delay_ave;
  double delay_max;
  double delay_min;

  double del_throughput;
  double del_abort_rate;
  double del_delay;

  double acc_throughput;
  double acc_abort_rate;
  double acc_delay;

  double new_throughput;
  double new_abort_rate;
  double new_delay;

  double sub_throughput;
  double sub_abort_rate;
  double sub_delay;

  double ins_throughput;
  double ins_abort_rate;
  double ins_delay;

  double upl_throughput;
  double upl_abort_rate;
  double upl_delay;

  double ups_throughput;
  double ups_abort_rate;
  double ups_delay;

  double assign_delay;

  double exe_time;

  double payment_throughput;

  double payment_abort_rate;

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

  //
  BalanceType balancer;

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
