//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: benchmark/ycsb/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#undef NDEBUG

#include <iomanip>
#include <algorithm>
#include <string.h>

#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/common/logger.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : ycsb <options> \n"
          "   -h --help              :  Print help message \n"
          "   -i --index             :  index type could be hash, btree, or bwtree\n"
          "   -k --scale_factor      :  # of tuples \n"
          "   -d --duration          :  execution duration \n"
          "   -s --snapshot_duration :  snapshot duration \n"
          "   -b --backend_count     :  # of backends \n"
          "   -c --column_count      :  # of columns \n"
          "   -l --update_col_count  :  # of updated columns \n"
          "   -r --read_col_count    :  # of read columns \n"
          "   -o --operation_count   :  # of operations \n"
          "   -y --scan              :  # of scan backends \n"
          "   -w --mock_duration     :  scan mock duration \n"
          "   -v --read-only         :  # of read-only backends \n"
          "   -a --declared          :  declared read-only \n"
          "   -n --sindex_count      :  # of secondary index \n"
          "   -u --write_ratio       :  Fraction of updates \n"
          "   -z --zipf_theta        :  theta to control skewness \n"
          "   -e --exp_backoff       :  enable exponential backoff \n"
          "   -x --blind_write       :  enable blind write \n"
          "   -p --protocol          :  choose protocol, default OCC\n"
          "                             protocol could be occ, pcc, pccopt, ssi, sread, ewrite, occrb, occn2o, to, torb, tofullrb, occ_central_rb, to_central_rb, to_full_central_rb, ton2o, tooptn2o, and tosv\n"
          "   -g --gc_protocol       :  choose gc protocol, default OFF\n"
          "                             gc protocol could be off, co, va, n2o and n2otxn\n"
          "   -t --gc_thread         :  number of thread used in gc, only used for gc type n2o/n2otxn/va\n"
          "   -q --sindex_mode       :  mode of secondary index: version or tuple\n"
          "   -j --sindex_scan       :  use secondary index to scan\n"
          "   -f --epoch_length      :  epoch length\n "
  );
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    {"scale_factor", optional_argument, NULL, 'k'},
    {"index", optional_argument, NULL, 'i'},
    {"duration", optional_argument, NULL, 'd'},
    {"snapshot_duration", optional_argument, NULL, 's'},
    {"column_count", optional_argument, NULL, 'c'},
    {"update_col_count", optional_argument, NULL, 'l'},
    {"read_col_count", optional_argument, NULL, 'r'},
    {"operation_count", optional_argument, NULL, 'o'},
    {"scan_mock_duration", optional_argument, NULL, 'w'},
    {"scan_backend_count", optional_argument, NULL, 'y'},
    {"ro_backend_count", optional_argument, NULL, 'v'},
    {"update_ratio", optional_argument, NULL, 'u'},
    {"backend_count", optional_argument, NULL, 'b'},
    {"zipf_theta", optional_argument, NULL, 'z'},
    {"exp_backoff", no_argument, NULL, 'e'},
    {"blind_write", no_argument, NULL, 'x'},
    {"declared", no_argument, NULL, 'a'},
    {"protocol", optional_argument, NULL, 'p'},
    {"gc_protocol", optional_argument, NULL, 'g'},
    {"gc_thread", optional_argument, NULL, 't'},
    {"sindex_count", optional_argument, NULL, 'n'},
    {"sindex_mode", optional_argument, NULL, 'q'},
    {"sindex_scan", optional_argument, NULL, 'j'},
    {"epoch_length", optional_argument, NULL, 'f'},
    {NULL, 0, NULL, 0}};

void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %d", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "scale_factor", state.scale_factor);
}

void ValidateColumnCount(const configuration &state) {
  if (state.column_count <= 0) {
    LOG_ERROR("Invalid column_count :: %d", state.column_count);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "column_count", state.column_count);
}

void ValidateUpdateColumnCount(const configuration &state) {
  if (state.update_column_count < 0 || state.update_column_count > state.column_count) {
    LOG_ERROR("Invalid update_column_count :: %d", state.update_column_count);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "update_column_count", state.update_column_count);
}

void ValidateReadColumnCount(const configuration &state) {
  if (state.read_column_count <= 0 || state.read_column_count > state.column_count) {
    LOG_ERROR("Invalid read_column_count :: %d", state.read_column_count);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "read_column_count", state.read_column_count);
}

void ValidateOperationCount(const configuration &state) {
  if (state.operation_count <= 0) {
    LOG_ERROR("Invalid operation_count :: %d", state.operation_count);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "operation_count", state.operation_count);
}

void ValidateUpdateRatio(const configuration &state) {
  if (state.update_ratio < 0 || state.update_ratio > 1) {
    LOG_ERROR("Invalid update_ratio :: %lf", state.update_ratio);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "update_ratio", state.update_ratio);
}

void ValidateBackendCount(const configuration &state) {
  if (state.backend_count <= 0) {
    LOG_ERROR("Invalid backend_count :: %d", state.backend_count);
    exit(EXIT_FAILURE);
  }
  if (state.scan_backend_count + state.ro_backend_count > state.backend_count) {
    LOG_ERROR("Invalid backend_count :: %d, %d", state.ro_backend_count, state.scan_backend_count);
    exit(EXIT_FAILURE);
  }
  LOG_TRACE("%s : %d", "backend_count", state.backend_count);
}

void ValidateScanMockDuration(const configuration &state) {
  if (state.scan_mock_duration < 0) {
    LOG_ERROR("Invalid duration :: %d", state.scan_mock_duration);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "scan mock duration", state.scan_mock_duration);
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

void ValidateZipfTheta(const configuration &state) {
  if (state.zipf_theta < 0 || state.zipf_theta > 1.0) {
    LOG_ERROR("Invalid zipf_theta :: %lf", state.zipf_theta);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "zipf_theta", state.zipf_theta);
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

void ValidateSecondaryIndexScan(const configuration &state) {
  if (state.sindex_scan == true && (state.sindex_count < 1 || state.column_count < 2)) {
    LOG_ERROR("Invalid scan type");
    exit(EXIT_FAILURE);
  }
}

void ValidateIndex(const configuration &state) {
  if (state.index != INDEX_TYPE_BTREE && state.index != INDEX_TYPE_BWTREE && state.index != INDEX_TYPE_HASH) {
    LOG_ERROR("Invalid index");
    exit(EXIT_FAILURE);
  }
}

void ValidateSecondaryIndex(const configuration &state) {
  if (state.sindex_count < 0) {
    LOG_ERROR("Secondary index number should >= 0");
    exit(EXIT_FAILURE);
  } else if (state.sindex_count > state.column_count) {
    // Fixme (Runshen Zhu): <= column count - 1 ?
    // const oid_t col_count = state.column_count + 1; in constructing table
    LOG_ERROR("Secondary index number should <= column count");
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
  state.column_count = 10;
  state.update_column_count = 1;
  state.read_column_count = 1;
  state.operation_count = 10;
  state.scan_backend_count = 0;
  state.scan_mock_duration = 0;
  state.ro_backend_count = 0;
  state.update_ratio = 0.5;
  state.backend_count = 2;
  state.zipf_theta = 0.0;
  state.declared = false;
  state.run_backoff = false;
  state.blind_write = false;
  state.protocol = CONCURRENCY_TYPE_TO_N2O;
  state.gc_protocol = GC_TYPE_OFF;
  state.index = INDEX_TYPE_HASH;
  state.gc_thread_count = 1;
  state.sindex_count = 0;
  state.sindex = SECONDARY_INDEX_TYPE_VERSION;
  state.sindex_scan = false;
  state.epoch_length = 40;
  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "hamexjk:d:s:c:l:r:o:u:b:z:p:g:i:t:y:v:n:q:w:f:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'a':
        state.declared = true;
        break;
      case 'n':
        state.sindex_count = atoi(optarg);
        break;
      case 't':
        state.gc_thread_count = atoi(optarg);
        break;
      case 'k':
        state.scale_factor = atoi(optarg);
        break;
      case 'd':
        state.duration = atof(optarg);
        break;
      case 's':
        state.snapshot_duration = atof(optarg);
        break;
      case 'o':
        state.operation_count = atoi(optarg);
        break;
      case 'c':
        state.column_count = atoi(optarg);
        break;
      case 'l':
        state.update_column_count = atoi(optarg);
        break;
      case 'r':
        state.read_column_count = atoi(optarg);
        break;
      case 'y':
        state.scan_backend_count = atoi(optarg);
        break;
      case 'w':
        state.scan_mock_duration = atoi(optarg);
        break;
      case 'v':
        state.ro_backend_count = atoi(optarg);
        break;
      case 'u':
        state.update_ratio = atof(optarg);
        break;
      case 'b':
        state.backend_count = atoi(optarg);
        break;
      case 'z':
        state.zipf_theta = atof(optarg);
        break;
      case 'e':
        state.run_backoff = true;
        break;
      case 'x':
        state.blind_write = true;
        break;
      case 'j':
        state.sindex_scan = true;
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
        } else if (strcmp(protocol, "occ_central_rb") == 0) {
          state.protocol = CONCURRENCY_TYPE_OCC_CENTRAL_RB;
        } else if (strcmp(protocol, "to_central_rb") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_CENTRAL_RB;
        } else if (strcmp(protocol, "sread") == 0) {
          state.protocol = CONCURRENCY_TYPE_SPECULATIVE_READ;
        } else if (strcmp(protocol, "occn2o") == 0) {
          state.protocol = CONCURRENCY_TYPE_OCC_N2O;
        } else if (strcmp(protocol, "pccopt") == 0) {
          state.protocol = CONCURRENCY_TYPE_PESSIMISTIC_OPT;
        } else if (strcmp(protocol, "torb") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_RB;
        } else if (strcmp(protocol, "ton2o") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_N2O;
          valid_proto = true;
        } else if (strcmp(protocol, "tofullrb") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO_FULL_RB;
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
        if (strcmp(index, "btree") == 0) {
          state.index = INDEX_TYPE_BTREE;
        } else if (strcmp(index, "bwtree") == 0) {
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

  // Print configuration
  ValidateScaleFactor(state);
  ValidateColumnCount(state);
  ValidateUpdateColumnCount(state);
  ValidateReadColumnCount(state);
  ValidateOperationCount(state);
  ValidateUpdateRatio(state);
  ValidateBackendCount(state);
  ValidateScanMockDuration(state);
  ValidateDuration(state);
  ValidateSnapshotDuration(state);
  ValidateZipfTheta(state);
  ValidateProtocol(state);
  ValidateIndex(state);
  ValidateSecondaryIndex(state);
  ValidateEpoch(state);
  ValidateSecondaryIndexScan(state);

  LOG_TRACE("%s : %d", "Run exponential backoff", state.run_backoff);
  LOG_TRACE("%s : %d", "Run blind write", state.blind_write);
  LOG_TRACE("%s : %d", "Run declared read-only", state.declared);
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
