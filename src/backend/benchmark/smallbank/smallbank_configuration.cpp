//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: benchmark/tpcc/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iomanip>
#include <algorithm>
#include <string.h>

#include "backend/benchmark/smallbank/smallbank_configuration.h"
#include "backend/common/logger.h"

namespace peloton {
namespace benchmark {
namespace smallbank {

void Usage(FILE *out) {
  fprintf(
      out,
      "Command line options : tpcc <options> \n"
      "   -h --help              :  state.hot_spot \n"
      "   -a --affinity          :  state.run_continue \n"
      "   -i --index             :  index type could be btree or bwtree\n"
      "   -k --scale_factor      :  scale factor \n"
      "   -d --duration          :  execution duration \n"
      "   -s --snapshot_duration :  snapshot duration \n"
      "   -b --backend_count     :  # of backends \n"
      "   -y --scan              :  # of scan backends \n"
      "   -w --warehouse_count   :  # of warehouses \n"
      "   -r --order_range       :  order range \n"
      "   -e --exp_backoff       :  enable exponential backoff \n"
      "   -p --protocol          :  choose protocol, default OCC\n"
      "                             protocol could be occ, pcc, pccopt, ssi, "
      "sread, ewrite, occrb, occn2o, to, torb, tofullrb, and ton2o\n"
      "   -g --gc_protocol       :  choose gc protocol, default OFF\n"
      "                             gc protocol could be off, co, va, and n2o\n"
      "   -t --gc_thread         :  number of thread used in gc, only used for "
      "gc type n2o/va\n"
      "   -q --sindex_mode       :  secondary index mode: version or tuple\n"
      "   -z --scheduler         :  control, queue, detect, ml\n"
      "   -n --enqueue thread    :  number of enqueue threads\n"
      "   -v --enqueue speed     :  number of txns per second \n");
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    {"scale_factor", optional_argument, NULL, 'k'},
    {"index", optional_argument, NULL, 'i'},
    {"duration", optional_argument, NULL, 'd'},
    {"snapshot_duration", optional_argument, NULL, 's'},
    {"backend_count", optional_argument, NULL, 'b'},
    {"order_range", optional_argument, NULL, 'r'},
    {"exp_backoff", no_argument, NULL, 'e'},
    {"affinity", no_argument, NULL, 'a'},
    {"online", no_argument, NULL, 'o'},
    {"single_ref", no_argument, NULL, 'l'},
    {"canonical", no_argument, NULL, 'c'},
    {"zipf_theta", optional_argument, NULL, 'y'},
    {"log_table", no_argument, NULL, 'j'},
    {"lock_free", no_argument, NULL, 'f'},
    {"protocol", optional_argument, NULL, 'p'},
    {"scheduler", optional_argument, NULL, 'z'},
    {"gc_protocol", optional_argument, NULL, 'g'},
    {"gc_thread", optional_argument, NULL, 't'},
    {"sindex_mode", optional_argument, NULL, 'q'},
    {"generate_count", optional_argument, NULL, 'n'},
    {"generate_speed", optional_argument, NULL, 'v'},
    {"min_pts", optional_argument, NULL, 'm'},
    {"analysis_txns", optional_argument, NULL, 'x'},
    {"hot_spot", optional_argument, NULL, 'h'},
    {NULL, 0, NULL, 0}};

void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %lf", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "scale_factor", state.scale_factor);
}

void ValidateBackendCount(const configuration &state) {
  if (state.backend_count <= 0) {
    LOG_ERROR("Invalid backend_count :: %d", state.backend_count);
    exit(EXIT_FAILURE);
  }
  if (state.scan_backend_count > state.backend_count) {
    LOG_ERROR("Invalid backend_count :: %d", state.scan_backend_count);
    exit(EXIT_FAILURE);
  }
  LOG_TRACE("%s : %d", "backend_count", state.backend_count);
}

void ValidateDuration(const configuration &state) {
  if (state.duration <= 0) {
    LOG_ERROR("Invalid duration :: %lf", state.duration);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "execution duration", state.duration);
}

void ValidateSnapshotDuration(const configuration &state) {
  if (state.snapshot_duration <= 0) {
    LOG_ERROR("Invalid snapshot_duration :: %lf", state.snapshot_duration);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "snapshot_duration", state.snapshot_duration);
}

void ValidateWarehouseCount(const configuration &state) {
  if (state.warehouse_count <= 0) {
    LOG_ERROR("Invalid warehouse_count :: %d", state.warehouse_count);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "warehouse_count", state.warehouse_count);
}

void ValidateOrderRange(const configuration &state) {
  if (state.warehouse_count <= 0) {
    LOG_ERROR("Invalid order_range :: %d", state.order_range);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "order range", state.order_range);
}

void ValidateProtocol(const configuration &state) {
  if (state.protocol != CONCURRENCY_TYPE_TO_N2O &&
      state.protocol != CONCURRENCY_TYPE_OCC_N2O) {
    if (state.gc_protocol == GC_TYPE_N2O) {
      LOG_ERROR("Invalid protocol");
      exit(EXIT_FAILURE);
    }
  } else {
    if (state.gc_protocol != GC_TYPE_OFF && state.gc_protocol != GC_TYPE_N2O &&
        state.gc_protocol != GC_TYPE_N2O_TXN) {
      LOG_ERROR("Invalid protocol");
      exit(EXIT_FAILURE);
    }
  }
}

void ValidateIndex(const configuration &state) {
  // if (state.index != INDEX_TYPE_BTREE && state.index != INDEX_TYPE_BWTREE &&
  // state.index != INDEX_TYPE_HASH) {
  if (state.index != INDEX_TYPE_BWTREE && state.index != INDEX_TYPE_HASH) {
    LOG_ERROR("Invalid index");
    exit(EXIT_FAILURE);
  }
}

void ValidateGenerateCount(const configuration &state) {
  if (state.generate_count < 0) {
    LOG_ERROR("Invalid generate_count :: %d", state.generate_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "generate_count", state.generate_count);
}

void ValidateGenerateSpeed(const configuration &state) {
  if (state.generate_speed < 0) {
    LOG_ERROR("Invalid generate_speed :: %d", state.generate_speed);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "generate_speed", state.generate_speed);
}

void ParseArguments(int argc, char *argv[], configuration &state) {
  // Default Values
  state.steal = 0.0;
  state.steal_rate = 0.0;
  state.scale_factor = 1;
  state.zipf_theta = -1;
  state.duration = 10;
  state.snapshot_duration = 1;
  state.backend_count = 1;
  state.scan_backend_count = 0;
  state.generate_count = 0;  // 0 means no query thread. only prepared queries
  state.generate_speed = 0;
  state.delay_ave = 0.0;
  state.delay_max = 0.0;
  state.delay_min = 0.0;
  state.exe_time = 0.0;
  state.warehouse_count = 1;
  state.running_ref = 0;
  state.order_range = 20;
  state.run_affinity = false;
  state.run_continue = false;
  state.run_backoff = false;
  state.offline = false;
  state.online = false;
  state.single_ref = false;
  state.canonical = false;
  state.log_table = false;
  state.lock_free = false;
  state.fraction = false;
  state.pure_balance = false;
  state.scheduler = SCHEDULER_TYPE_NONE;
  state.protocol = CONCURRENCY_TYPE_OPTIMISTIC;
  state.gc_protocol = GC_TYPE_OFF;
  state.index = INDEX_TYPE_HASH;
  state.gc_thread_count = 1;
  state.sindex = SECONDARY_INDEX_TYPE_VERSION;
  state.min_pts = 1;
  state.analysis_txns = 10000;
  state.hot_spot = -1;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(
        argc, argv, "aeoflcjyur:m:x:k:w:n:h:v:d:s:b:p:z:g:i:t:q:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 't':
        state.gc_thread_count = atoi(optarg);
        break;
      case 'm':
        state.min_pts = atof(optarg);
        break;
      case 'x':
        state.analysis_txns = atof(optarg);
        break;
      case 'k':
        state.scale_factor = atof(optarg);
        break;
      case 'w':
        state.warehouse_count = atoi(optarg);
        break;
      case 'n':
        state.generate_count = atoi(optarg);
        break;
      case 'h':
        state.hot_spot = atoi(optarg);
        break;
      case 'v':
        state.generate_speed = atoi(optarg);
        break;
      case 'u':
        state.pure_balance = true;
        break;
      case 'r':
        state.order_range = atoi(optarg);
        break;
      case 'd':
        state.duration = atof(optarg);
        break;
      case 's':
        state.snapshot_duration = atof(optarg);
        break;
      case 'b':
        state.backend_count = atoi(optarg);
        break;
      case 'y':
        state.fraction = true;
        break;
      case 'a':
        state.run_continue = true;
        state.log_table = true;
        break;
      case 'e':
        state.run_backoff = true;
        break;
      case 'o':
        state.online = true;
        break;
      case 'f':
        state.lock_free = true;
        break;
      case 'l':
        state.single_ref = true;
        break;
      case 'c':
        state.canonical = true;
        break;
      case 'j':
        state.log_table = true;
        break;
      case 'z': {
        char *scheduler = optarg;
        if (strcmp(scheduler, "none") == 0) {
          state.scheduler = SCHEDULER_TYPE_NONE;
        } else if (strcmp(scheduler, "control") == 0) {
          state.scheduler = SCHEDULER_TYPE_CONTROL;
        } else if (strcmp(scheduler, "queue") == 0) {
          state.scheduler = SCHEDULER_TYPE_ABORT_QUEUE;
        } else if (strcmp(scheduler, "detect") == 0) {
          state.scheduler = SCHEDULER_TYPE_CONFLICT_DETECT;
        } else if (strcmp(scheduler, "hash") == 0) {
          state.scheduler = SCHEDULER_TYPE_HASH;
        } else if (strcmp(scheduler, "ml") == 0) {
          state.scheduler = SCHEDULER_TYPE_CONFLICT_LEANING;
        } else if (strcmp(scheduler, "cluster") == 0) {
          state.scheduler = SCHEDULER_TYPE_CLUSTER;
        } else if (strcmp(scheduler, "range") == 0) {
          state.scheduler = SCHEDULER_TYPE_CONFLICT_RANGE;
        } else {
          fprintf(stderr, "\nUnknown scheduler: %s\n", scheduler);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'p': {
        char *protocol = optarg;
        if (strcmp(protocol, "occ") == 0) {
          state.protocol = CONCURRENCY_TYPE_OPTIMISTIC;
        } else if (strcmp(protocol, "pcc") == 0) {
          state.protocol = CONCURRENCY_TYPE_PESSIMISTIC;
        } else if (strcmp(protocol, "ssi") == 0) {
          state.protocol = CONCURRENCY_TYPE_SSI;
        } else if (strcmp(protocol, "to") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO;
        } else if (strcmp(protocol, "ewrite") == 0) {
          state.protocol = CONCURRENCY_TYPE_EAGER_WRITE;
        } else if (strcmp(protocol, "occrb") == 0) {
          state.protocol = CONCURRENCY_TYPE_OCC_RB;
        } else if (strcmp(protocol, "sread") == 0) {
          state.protocol = CONCURRENCY_TYPE_SPECULATIVE_READ;
        } else if (strcmp(protocol, "occn2o") == 0) {
          state.protocol = CONCURRENCY_TYPE_OCC_N2O;
        } else if (strcmp(protocol, "pccopt") == 0) {
          state.protocol = CONCURRENCY_TYPE_PESSIMISTIC_OPT;
        } else if (strcmp(protocol, "torb") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_RB;
        } else if (strcmp(protocol, "tofullrb") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_FULL_RB;
        } else if (strcmp(protocol, "ton2o") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_N2O;
        } else if (strcmp(protocol, "occ_central_rb") == 0) {
          state.protocol = CONCURRENCY_TYPE_OCC_CENTRAL_RB;
        } else if (strcmp(protocol, "to_central_rb") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_CENTRAL_RB;
        } else if (strcmp(protocol, "to_full_central_rb") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_FULL_CENTRAL_RB;
        } else {
          fprintf(stderr, "\nUnknown protocol: %s\n", protocol);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'g': {
        char *gc_protocol = optarg;
        if (strcmp(gc_protocol, "off") == 0) {
          state.gc_protocol = GC_TYPE_OFF;
        } else if (strcmp(gc_protocol, "va") == 0) {
          state.gc_protocol = GC_TYPE_VACUUM;
        } else if (strcmp(gc_protocol, "co") == 0) {
          state.gc_protocol = GC_TYPE_CO;
        } else if (strcmp(gc_protocol, "n2o") == 0) {
          state.gc_protocol = GC_TYPE_N2O;
        } else if (strcmp(gc_protocol, "n2otxn") == 0) {
          state.gc_protocol = GC_TYPE_N2O_TXN;
        } else {
          fprintf(stderr, "\nUnknown gc protocol: %s\n", gc_protocol);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'i': {
        char *index = optarg;
        // if (strcmp(index, "btree") == 0) {
        //   state.index = INDEX_TYPE_BTREE;
        // } else
        if (strcmp(index, "bwtree") == 0) {
          state.index = INDEX_TYPE_BWTREE;
        } else if (strcmp(index, "hash") == 0) {
          state.index = INDEX_TYPE_HASH;
        } else {
          fprintf(stderr, "\nUnknown index: %s\n", index);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'q': {
        char *sindex = optarg;
        if (strcmp(sindex, "version") == 0) {
          state.sindex = SECONDARY_INDEX_TYPE_VERSION;
        } else if (strcmp(sindex, "tuple") == 0) {
          state.sindex = SECONDARY_INDEX_TYPE_TUPLE;
        } else {
          fprintf(stderr, "\n Unknown sindex: %s\n", sindex);
          exit(EXIT_FAILURE);
        }
        break;
      }
      //      case 'help':
      //        Usage(stderr);
      //        exit(EXIT_FAILURE);
      //        break;

      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        Usage(stderr);
        exit(EXIT_FAILURE);
    }
  }

  // Static parameters
  state.item_count = 100000 * state.scale_factor;
  state.districts_per_warehouse = 10;
  state.customers_per_district = 3000 * state.scale_factor;
  state.new_orders_per_district = 900 * state.scale_factor;

  NUM_ACCOUNTS = BASIC_ACCOUNTS * state.scale_factor;

  // Print configuration
  ValidateScaleFactor(state);
  ValidateDuration(state);
  ValidateSnapshotDuration(state);
  ValidateWarehouseCount(state);
  ValidateBackendCount(state);
  ValidateProtocol(state);
  ValidateIndex(state);
  ValidateOrderRange(state);
  ValidateGenerateCount(state);
  ValidateGenerateSpeed(state);

  LOG_TRACE("%s : %d", "hot_spot", state.hot_spot);
  LOG_TRACE("%s : %d", "Run client affinity", state.run_affinity);
  LOG_TRACE("%s : %d", "Run exponential backoff", state.run_backoff);
  LOG_TRACE("%s : %d", "Run online analysis", state.online);
  LOG_TRACE("%s : %d", "Run cluster min_pts", state.min_pts);
  LOG_TRACE("%s : %d", "Run cluster analysis txns", state.analysis_txns);

  LOG_INFO("ParseArguments over");
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
