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

#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/common/logger.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : tpcc <options> \n"
          "   -h --help              :  Print help message \n"
          "   -i --index             :  index type could be hash index or bwtree\n"
          "   -k --scale_factor      :  scale factor \n"
          "   -d --duration          :  execution duration \n"
          "   -s --snapshot_duration :  snapshot duration \n"
          "   -b --backend_count     :  # of backends \n"
          "   -y --scan              :  # of scan backends \n"
          "   -w --warehouse_count   :  # of warehouses \n"
          "   -r --order_range       :  order range \n"
          "   -e --exp_backoff       :  enable exponential backoff \n"
          "   -a --affinity          :  enable client affinity \n"
          "   -p --protocol          :  choose protocol, default OCC\n"
          "                             protocol could be occ, pcc, pccopt, ssi, sread, ewrite, occrb, occn2o, to, torb, tofullrb, occ_central_rb, to_central_rb, to_full_central_rb and ton2o\n"
          "   -g --gc_protocol       :  choose gc protocol, default OFF\n"
          "                             gc protocol could be off, co, va, n2o and n2otxn\n"
          "   -t --gc_thread         :  number of thread used in gc, only used for gc type n2o/va/n2otxn\n"
          "   -q --sindex_mode       :  secondary index mode: version or tuple\n"
          "   -f --epoch_length      :  epoch length\n "
  );
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
  { "scale_factor", optional_argument, NULL, 'k'},
  { "index", optional_argument, NULL, 'i'},
  { "duration", optional_argument, NULL, 'd' },
  { "snapshot_duration", optional_argument, NULL, 's'},
  { "backend_count", optional_argument, NULL, 'b'},
  { "scan_backend_count", optional_argument, NULL, 'y'},
  { "warehouse_count", optional_argument, NULL, 'w'},
  { "order_range", optional_argument, NULL, 'r'},
  { "exp_backoff", no_argument, NULL, 'e'},
  { "affinity", no_argument, NULL, 'a'},
  { "protocol", optional_argument, NULL, 'p'},
  { "gc_protocol", optional_argument, NULL, 'g'},
  { "gc_thread", optional_argument, NULL, 't'},
  { "sindex_mode", optional_argument, NULL, 'q'},
  {"epoch_length", optional_argument, NULL, 'f'},
  { NULL, 0, NULL, 0 }
};

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
  if (state.protocol == CONCURRENCY_TYPE_TO_SV ||
      state.protocol == CONCURRENCY_TYPE_OCC_SV ||
      state.protocol == CONCURRENCY_TYPE_OCC_SV_BEST) {
    if (state.gc_protocol != GC_TYPE_OFF && state.gc_protocol != GC_TYPE_SV) {
      LOG_ERROR("Invalid protocol");
      exit(EXIT_FAILURE);
    }
  }
  else if (state.protocol != CONCURRENCY_TYPE_TO_N2O && 
      state.protocol != CONCURRENCY_TYPE_OCC_N2O &&
      state.protocol != CONCURRENCY_TYPE_TO_OPT_N2O &&
      state.protocol != CONCURRENCY_TYPE_OCC_BEST_N2O) {
    if (state.gc_protocol == GC_TYPE_N2O) {
      LOG_ERROR("Invalid protocol");
      exit(EXIT_FAILURE);
    }
  } else {
    if (state.gc_protocol != GC_TYPE_OFF
    && state.gc_protocol != GC_TYPE_N2O
    && state.gc_protocol != GC_TYPE_N2O_TXN) {
      LOG_ERROR("Invalid protocol");
      exit(EXIT_FAILURE);
    }
  }
}

void ValidateIndex(const configuration &state) {
  // if (state.index != INDEX_TYPE_BTREE && state.index != INDEX_TYPE_BWTREE && state.index != INDEX_TYPE_HASH) {
  if (state.index != INDEX_TYPE_BWTREE && state.index != INDEX_TYPE_HASH) {
    LOG_ERROR("Invalid index");
    exit(EXIT_FAILURE);
  }
}

void ValidateEpoch(const configuration &state) {
  if (state.epoch_length <= 0) {
    LOG_ERROR("Invalid epoch length :: %d", state.epoch_length);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "epoch_length", state.epoch_length);
}

void ParseArguments(int argc, char *argv[], configuration &state) {
  // Default Values
  state.scale_factor = 1;
  state.duration = 10;
  state.snapshot_duration = 1;
  state.backend_count = 1;
  state.scan_backend_count = 0;
  state.warehouse_count = 1;
  state.order_range = 20;
  state.run_affinity = false;
  state.run_backoff = false;
  state.protocol = CONCURRENCY_TYPE_TO_N2O;
  state.gc_protocol = GC_TYPE_OFF;
  state.index = INDEX_TYPE_HASH;
  state.gc_thread_count = 1;
  state.sindex = SECONDARY_INDEX_TYPE_VERSION;
  state.epoch_length = 40;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "aeh:r:k:w:d:s:b:p:g:i:t:q:y:f:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 't':
        state.gc_thread_count = atoi(optarg);
      case 'k':
        state.scale_factor = atof(optarg);
        break;
      case 'w':
        state.warehouse_count = atoi(optarg);
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
        state.scan_backend_count = atoi(optarg);
        break;
      case 'a':
        state.run_affinity = true;
        break;
      case 'e':
        state.run_backoff = true;
        break;
      case 'f':
        state.epoch_length = atoi(optarg);
        break;
      case 'p': {
        char *protocol = optarg;
        bool valid_proto = false;
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
          valid_proto = true;
        } else if (strcmp(protocol, "occ_central_rb") == 0) {
          state.protocol = CONCURRENCY_TYPE_OCC_CENTRAL_RB;
        } else if (strcmp(protocol, "to_central_rb") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_CENTRAL_RB;
        } else if (strcmp(protocol, "to_full_central_rb") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_FULL_CENTRAL_RB;
        } else if (strcmp(protocol, "tooptn2o") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_OPT_N2O;
          valid_proto = true;
        } else if (strcmp(protocol, "tosv") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_SV;
        } else if (strcmp(protocol, "occbestn2o") == 0) {
          state.protocol = CONCURRENCY_TYPE_OCC_BEST_N2O;
        } else if (strcmp(protocol, "occsv") == 0) {
          state.protocol = CONCURRENCY_TYPE_OCC_SV;
        } else if (strcmp(protocol, "occsvbest") == 0) {
          state.protocol = CONCURRENCY_TYPE_OCC_SV_BEST;
        } else {
          fprintf(stderr, "\nUnknown protocol: %s\n", protocol);
          exit(EXIT_FAILURE);
        }

        if (valid_proto == false) {
          fprintf(stdout, "We no longer support %s, turn to default ton2o\n", protocol);
          state.protocol = CONCURRENCY_TYPE_TO_N2O;
        }
        break;
      }
      case 'g': {
        char *gc_protocol = optarg;
        bool valid_gc = false;
        if (strcmp(gc_protocol, "off") == 0) {
          state.gc_protocol = GC_TYPE_OFF;
          valid_gc = true;
        } else if (strcmp(gc_protocol, "va") == 0) {
          state.gc_protocol = GC_TYPE_VACUUM;
        } else if (strcmp(gc_protocol, "co") == 0) {
          state.gc_protocol = GC_TYPE_CO;
        } else if (strcmp(gc_protocol, "n2o") == 0) {
          state.gc_protocol = GC_TYPE_N2O;
        } else if (strcmp(gc_protocol, "n2otxn") == 0) {
          state.gc_protocol = GC_TYPE_N2O_TXN;
          valid_gc = true;
        } else if (strcmp(gc_protocol, "sv") == 0 ) {
          state.gc_protocol = GC_TYPE_SV;
        } else {
          fprintf(stderr, "\nUnknown gc protocol: %s\n", gc_protocol);
          exit(EXIT_FAILURE);
        }

        if (valid_gc == false) {
          fprintf(stdout, "We no longer support %s, turn to default gc-off\n", gc_protocol);
          state.gc_protocol = GC_TYPE_OFF;
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
      case 'h':
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;

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

  // Print configuration
  ValidateScaleFactor(state);
  ValidateDuration(state);
  ValidateSnapshotDuration(state);
  ValidateWarehouseCount(state);
  ValidateBackendCount(state);
  ValidateProtocol(state);
  ValidateIndex(state);
  ValidateOrderRange(state);
  ValidateEpoch(state);
  
  LOG_TRACE("%s : %d", "Run client affinity", state.run_affinity);
  LOG_TRACE("%s : %d", "Run exponential backoff", state.run_backoff);

}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton