//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger_configuration.cpp
//
// Identification: src/main/logger/logger_configuration.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iomanip>
#include <algorithm>
#include <sys/stat.h>

#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/benchmark/logger/logger_configuration.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"

extern CheckpointType peloton_checkpoint_mode;

namespace peloton {
namespace benchmark {
namespace logger {

void Usage(FILE* out) {
  fprintf(out,
          "Command line options :  logger <options> \n"
          "   -h --help              :  Print help message \n"
          "   -F --data-file-size    :  Data file size (MB) \n"
          "   -L --logging-type      :  Logging type \n"
          "   -Y --benchmark-type    :  Benchmark type \n");
}

static struct option opts[] = {
    {"data-file-size", optional_argument, NULL, 'F'},
    {"logging-type", optional_argument, NULL, 'L'},
    {"benchmark-type", optional_argument, NULL, 'Y'},
    {NULL, 0, NULL, 0}};

static void ValidateLoggingType(const configuration& state) {
  std::cout<<"logging type="<<state.logging_type<<std::endl;
  if (state.logging_type != LOGGING_TYPE_INVALID && state.logging_type != LOGGING_TYPE_ON) {
    LOG_ERROR("Invalid logging_type :: %d", state.logging_type);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("Logging_type :: %s",
           LoggingTypeToString(state.logging_type).c_str());
}

static void ValidateDataFileSize(const configuration& state) {
  if (state.data_file_size <= 0) {
    LOG_ERROR("Invalid data_file_size :: %lu", state.data_file_size);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("data_file_size :: %lu", state.data_file_size);
}

// static void ValidateLogFileDir(configuration& state) {
//   struct stat data_stat;

//   // Assign log file dir based on logging type
//   switch (state.logging_type) {

//     // Log file on SSD
//     case LOGGING_TYPE_SSD_WAL: {
//       int status = stat(SSD_DIR, &data_stat);
//       if (status == 0 && S_ISDIR(data_stat.st_mode)) {
//         state.log_file_dir = SSD_DIR;
//       }
//     } break;

//     // Log file on HDD
//     case LOGGING_TYPE_HDD_WAL: {
//       int status = stat(HDD_DIR, &data_stat);
//       if (status == 0 && S_ISDIR(data_stat.st_mode)) {
//         state.log_file_dir = HDD_DIR;
//       }
//     } break;

//     case LOGGING_TYPE_INVALID:
//     default: {
//       int status = stat(TMP_DIR, &data_stat);
//       if (status == 0 && S_ISDIR(data_stat.st_mode)) {
//         state.log_file_dir = TMP_DIR;
//       } else {
//         throw Exception("Could not find temp directory : " +
//                         std::string(TMP_DIR));
//       }
//     } break;
//   }

//   LOG_INFO("log_file_dir :: %s", state.log_file_dir.c_str());
// }

void ParseArguments(int argc, char* argv[], configuration& state) {
  // Default Logger Values
  state.logging_type = LOGGING_TYPE_ON;
  state.log_file_dir = TMP_DIR;
  state.data_file_size = 512;

  state.benchmark_type = BENCHMARK_TYPE_YCSB;
  state.checkpoint_type = CHECKPOINT_TYPE_INVALID;

  // YCSB Default Values
  ycsb::state.scale_factor = 1;
  ycsb::state.duration = 10;
  ycsb::state.snapshot_duration = 1;
  ycsb::state.column_count = 10;
  ycsb::state.update_column_count = 1;
  ycsb::state.read_column_count = 1;
  ycsb::state.operation_count = 10;
  ycsb::state.scan_backend_count = 0;
  ycsb::state.scan_mock_duration = 0;
  ycsb::state.ro_backend_count = 0;
  ycsb::state.update_ratio = 0.5;
  ycsb::state.backend_count = 2;
  ycsb::state.zipf_theta = 0.0;
  ycsb::state.declared = false;
  ycsb::state.run_backoff = false;
  ycsb::state.blind_write = false;
  ycsb::state.protocol = CONCURRENCY_TYPE_TO_N2O;
  ycsb::state.gc_protocol = GC_TYPE_OFF;
  ycsb::state.index = INDEX_TYPE_HASH;
  ycsb::state.gc_thread_count = 1;
  ycsb::state.sindex_count = 0;
  ycsb::state.sindex = SECONDARY_INDEX_TYPE_VERSION;
  ycsb::state.sindex_scan = false;
  ycsb::state.epoch_length = 40;

  // TPC-C Default Values
  tpcc::state.scale_factor = 1;
  tpcc::state.duration = 10;
  tpcc::state.snapshot_duration = 1;
  tpcc::state.backend_count = 1;
  tpcc::state.scan_backend_count = 0;
  tpcc::state.warehouse_count = 1;
  tpcc::state.order_range = 20;
  tpcc::state.run_affinity = false;
  tpcc::state.run_backoff = false;
  tpcc::state.protocol = CONCURRENCY_TYPE_TO_N2O;
  tpcc::state.gc_protocol = GC_TYPE_OFF;
  tpcc::state.index = INDEX_TYPE_HASH;
  tpcc::state.gc_thread_count = 1;
  tpcc::state.sindex = SECONDARY_INDEX_TYPE_VERSION;
  tpcc::state.epoch_length = 40;


  // Parse args
  while (1) {
    int idx = 0;
    // logger - hW:F:L:
    // ycsb   - hamexjk:d:s:c:l:r:o:u:b:z:p:g:i:t:y:v:n:q:w:f:
    // tpcc   - hae:r:k:w:d:s:b:p:g:i:t:q:y:f:
    int c = getopt_long(argc, argv, "F:L:Y:hamexjk:d:s:c:l:r:o:u:b:z:p:g:i:t:y:v:n:q:w:f:",
                        opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'F':
        state.data_file_size = atoi(optarg);
        break;
      case 'L':
        state.logging_type = (LoggingType)atoi(optarg);
        break;
      case 'Y': {
        char *benchmark_str = optarg;
        if (strcmp(optarg, "ycsb") == 0) {
          state.benchmark_type = BENCHMARK_TYPE_YCSB;
        } else if (strcmp(optarg, "tpcc") == 0) {
          state.benchmark_type = BENCHMARK_TYPE_TPCC;
        } else {
          fprintf(stderr, "\nUnknown benchmark: %s\n", benchmark_str);
          exit(EXIT_FAILURE);
        }
        break;
      }
      
      case 'a':
        ycsb::state.declared = true;
        tpcc::state.run_affinity = true;
        break;
      case 'b':
        ycsb::state.backend_count = atoi(optarg);
        tpcc::state.backend_count = atoi(optarg);
        break;
      case 'c':
        ycsb::state.column_count = atoi(optarg);
        break;
      case 'd':
        ycsb::state.duration = atof(optarg);
        tpcc::state.duration = atof(optarg);
        break;
      case 'e':
        ycsb::state.run_backoff = true;
        tpcc::state.run_backoff = true;
        break;
      case 'f':
        ycsb::state.epoch_length = atoi(optarg);
        tpcc::state.epoch_length = atoi(optarg);
        break; 
      case 'g': {
        char *gc_protocol = optarg;
        bool valid_gc = false;
        if (strcmp(gc_protocol, "off") == 0) {
          ycsb::state.gc_protocol = GC_TYPE_OFF;
          tpcc::state.gc_protocol = GC_TYPE_OFF;
          valid_gc = true;
        } else if (strcmp(gc_protocol, "va") == 0) {
          ycsb::state.gc_protocol = GC_TYPE_VACUUM;
          tpcc::state.gc_protocol = GC_TYPE_VACUUM;
        } else if (strcmp(gc_protocol, "co") == 0) {
          ycsb::state.gc_protocol = GC_TYPE_CO;
          tpcc::state.gc_protocol = GC_TYPE_CO;
        } else if (strcmp(gc_protocol, "n2o") == 0) {
          ycsb::state.gc_protocol = GC_TYPE_N2O;
          tpcc::state.gc_protocol = GC_TYPE_N2O;
        } else if (strcmp(gc_protocol, "n2otxn") == 0) {
          ycsb::state.gc_protocol = GC_TYPE_N2O_TXN;
          tpcc::state.gc_protocol = GC_TYPE_N2O_TXN;
          valid_gc = true;
        } else if (strcmp(gc_protocol, "sv") == 0 ) {
          ycsb::state.gc_protocol = GC_TYPE_SV;
          tpcc::state.gc_protocol = GC_TYPE_SV;
        } else {
          fprintf(stderr, "\nUnknown gc protocol: %s\n", gc_protocol);
          exit(EXIT_FAILURE);
        }

        if (valid_gc == false) {
          fprintf(stdout, "We no longer support %s, turn to default gc-off\n", gc_protocol);
          ycsb::state.gc_protocol = GC_TYPE_OFF;
          tpcc::state.gc_protocol = GC_TYPE_OFF;
        }
        break;
      }
      case 'i': {
        char *index = optarg;
        if (strcmp(index, "bwtree") == 0) {
          ycsb::state.index = INDEX_TYPE_BWTREE;
          tpcc::state.index = INDEX_TYPE_BWTREE;
        } else if (strcmp(index, "hash") == 0) {
          ycsb::state.index = INDEX_TYPE_HASH;
          tpcc::state.index = INDEX_TYPE_HASH;
        } else {
          fprintf(stderr, "\nUnknown index: %s\n", index);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'j':
        ycsb::state.sindex_scan = true;
        break;
      case 'k':
        ycsb::state.scale_factor = atoi(optarg);
        tpcc::state.scale_factor = atof(optarg);
        break;
      case 'l':
        ycsb::state.update_column_count = atoi(optarg);
        break;
      case 'n':
        ycsb::state.sindex_count = atoi(optarg);
        break;
      case 'o':
        ycsb::state.operation_count = atoi(optarg);
        break;
      case 'p': {
        char *protocol = optarg;
        bool valid_proto = false;
        if (strcmp(protocol, "occ") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_OPTIMISTIC;
          tpcc::state.protocol = CONCURRENCY_TYPE_OPTIMISTIC;
        } else if (strcmp(protocol, "pcc") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_PESSIMISTIC;
          tpcc::state.protocol = CONCURRENCY_TYPE_PESSIMISTIC;
        } else if (strcmp(protocol, "ssi") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_SSI;
          tpcc::state.protocol = CONCURRENCY_TYPE_SSI;
        } else if (strcmp(protocol, "to") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_TO;
          tpcc::state.protocol = CONCURRENCY_TYPE_TO;
        } else if (strcmp(protocol, "ewrite") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_EAGER_WRITE;
          tpcc::state.protocol = CONCURRENCY_TYPE_EAGER_WRITE;
        } else if (strcmp(protocol, "occrb") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_OCC_RB;
          tpcc::state.protocol = CONCURRENCY_TYPE_OCC_RB;
        } else if (strcmp(protocol, "sread") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_SPECULATIVE_READ;
          tpcc::state.protocol = CONCURRENCY_TYPE_SPECULATIVE_READ;
        } else if (strcmp(protocol, "occn2o") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_OCC_N2O;
          tpcc::state.protocol = CONCURRENCY_TYPE_OCC_N2O;
        } else if (strcmp(protocol, "pccopt") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_PESSIMISTIC_OPT;
          tpcc::state.protocol = CONCURRENCY_TYPE_PESSIMISTIC_OPT;
        } else if (strcmp(protocol, "torb") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_TO_RB;
          tpcc::state.protocol = CONCURRENCY_TYPE_TO_RB;
        } else if (strcmp(protocol, "tofullrb") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_TO_FULL_RB;
          tpcc::state.protocol = CONCURRENCY_TYPE_TO_FULL_RB;
        } else if (strcmp(protocol, "ton2o") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_TO_N2O;
          tpcc::state.protocol = CONCURRENCY_TYPE_TO_N2O;
          valid_proto = true;
        } else if (strcmp(protocol, "occ_central_rb") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_OCC_CENTRAL_RB;
          tpcc::state.protocol = CONCURRENCY_TYPE_OCC_CENTRAL_RB;
        } else if (strcmp(protocol, "to_central_rb") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_TO_CENTRAL_RB;
          tpcc::state.protocol = CONCURRENCY_TYPE_TO_CENTRAL_RB;
        } else if (strcmp(protocol, "to_full_central_rb") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_TO_FULL_CENTRAL_RB;
          tpcc::state.protocol = CONCURRENCY_TYPE_TO_FULL_CENTRAL_RB;
        } else if (strcmp(protocol, "tooptn2o") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_TO_OPT_N2O;
          tpcc::state.protocol = CONCURRENCY_TYPE_TO_OPT_N2O;
          valid_proto = true;
        } else if (strcmp(protocol, "tosv") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_TO_SV;
          tpcc::state.protocol = CONCURRENCY_TYPE_TO_SV;
        } else if (strcmp(protocol, "occbestn2o") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_OCC_BEST_N2O;
          tpcc::state.protocol = CONCURRENCY_TYPE_OCC_BEST_N2O;
        } else if (strcmp(protocol, "occsv") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_OCC_SV;
          tpcc::state.protocol = CONCURRENCY_TYPE_OCC_SV;
        } else if (strcmp(protocol, "occsvbest") == 0) {
          ycsb::state.protocol = CONCURRENCY_TYPE_OCC_SV_BEST;
          tpcc::state.protocol = CONCURRENCY_TYPE_OCC_SV_BEST;
        } else {
          fprintf(stderr, "\nUnknown protocol: %s\n", protocol);
          exit(EXIT_FAILURE);
        }

        if (valid_proto == false) {
          fprintf(stdout, "We no longer support %s, turn to default ton2o\n", protocol);
          ycsb::state.protocol = CONCURRENCY_TYPE_TO_N2O;
          tpcc::state.protocol = CONCURRENCY_TYPE_TO_N2O;
        }
        break;
      }
      case 'q': {
        char *sindex = optarg;
        if (strcmp(sindex, "version") == 0) {
          ycsb::state.sindex = SECONDARY_INDEX_TYPE_VERSION;
          tpcc::state.sindex = SECONDARY_INDEX_TYPE_VERSION;
        } else if (strcmp(sindex, "tuple") == 0) {
          ycsb::state.sindex = SECONDARY_INDEX_TYPE_TUPLE;
          tpcc::state.sindex = SECONDARY_INDEX_TYPE_TUPLE;
        } else {
          fprintf(stderr, "\n Unknown sindex: %s\n", sindex);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'r':
        ycsb::state.read_column_count = atoi(optarg);
        tpcc::state.order_range = atoi(optarg);
        break;
      case 's':
        ycsb::state.snapshot_duration = atof(optarg);
        tpcc::state.snapshot_duration = atof(optarg);
        break;
      case 't':
        ycsb::state.gc_thread_count = atoi(optarg);
        tpcc::state.gc_thread_count = atoi(optarg);
        break;
      case 'u':
        ycsb::state.update_ratio = atof(optarg);
        break;
      case 'v':
        ycsb::state.ro_backend_count = atoi(optarg);
        break;
      case 'w':
        tpcc::state.warehouse_count = atoi(optarg);
        break;
      case 'x':
        ycsb::state.blind_write = true;
        break;
      case 'y':
        tpcc::state.scan_backend_count = atoi(optarg);
        break;
      case 'z':
        ycsb::state.zipf_theta = atof(optarg);
        break;

      case 'h':
        Usage(stderr);
        ycsb::Usage(stderr);
        tpcc::Usage(stderr);
        exit(EXIT_FAILURE);
        break;

      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        Usage(stderr);
        ycsb::Usage(stderr);
        tpcc::Usage(stderr);
        exit(EXIT_FAILURE);
        break;
    }
  }

  if (state.checkpoint_type == CHECKPOINT_TYPE_NORMAL &&
      (state.logging_type == LOGGING_TYPE_ON)) {
    peloton_checkpoint_mode = CHECKPOINT_TYPE_NORMAL;
  }

  // Print Logger configuration
  ValidateLoggingType(state);
  ValidateDataFileSize(state);
  // ValidateLogFileDir(state);

  // Print YCSB configuration
  if (state.benchmark_type == BENCHMARK_TYPE_YCSB) {
    ycsb::ValidateScaleFactor(ycsb::state);
    ycsb::ValidateColumnCount(ycsb::state);
    ycsb::ValidateUpdateColumnCount(ycsb::state);
    ycsb::ValidateReadColumnCount(ycsb::state);
    ycsb::ValidateOperationCount(ycsb::state);
    ycsb::ValidateUpdateRatio(ycsb::state);
    ycsb::ValidateBackendCount(ycsb::state);
    ycsb::ValidateScanMockDuration(ycsb::state);
    ycsb::ValidateDuration(ycsb::state);
    ycsb::ValidateSnapshotDuration(ycsb::state);
    ycsb::ValidateZipfTheta(ycsb::state);
    ycsb::ValidateProtocol(ycsb::state);
    ycsb::ValidateIndex(ycsb::state);
    ycsb::ValidateSecondaryIndex(ycsb::state);
    ycsb::ValidateEpoch(ycsb::state);
    ycsb::ValidateSecondaryIndexScan(ycsb::state);
  }
  // Print TPCC configuration
  else if (state.benchmark_type == BENCHMARK_TYPE_TPCC) {
    tpcc::ValidateScaleFactor(tpcc::state);
    tpcc::ValidateDuration(tpcc::state);
    tpcc::ValidateSnapshotDuration(tpcc::state);
    tpcc::ValidateWarehouseCount(tpcc::state);
    tpcc::ValidateBackendCount(tpcc::state);
    tpcc::ValidateProtocol(tpcc::state);
    tpcc::ValidateIndex(tpcc::state);
    tpcc::ValidateOrderRange(tpcc::state);
    tpcc::ValidateEpoch(tpcc::state);

    // Static TPCC parameters
    tpcc::state.item_count = 100000 * tpcc::state.scale_factor;
    tpcc::state.districts_per_warehouse = 10;
    tpcc::state.customers_per_district = 3000 * tpcc::state.scale_factor;
    tpcc::state.new_orders_per_district = 900 * tpcc::state.scale_factor;
  }
}

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
