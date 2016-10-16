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

#include <fstream>
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
          "                             gc protocol could be off, n2otxn, n2oepoch, n2oss\n"
          "   -t --gc_thread         :  number of thread used in gc, only used for gc type n2o/n2otxn/va\n"
          "   -q --sindex_mode       :  mode of secondary index: version or tuple\n"
          "   -j --sindex_scan       :  use secondary index to scan\n"
          "   -f --epoch_length      :  epoch length\n"
          "   -L --log_type          :  log type could be phylog, epoch, off\n"
          "   -D --log_directories   :  multiple log directories, e.g., /data1/,/data2/,/data3/,...\n"
          "   -C --checkpoint_type   :  checkpoint type could be phylog, off\n"
          "   -F --ckpt_directories  :  multiple checkpoint directories, e.g., /data1/,/data2/,/data3/,...\n"
          "   -T --timer_on          :  timer type could be off, sum, dist. Default is off\n"
          "   -S --sleep_between_ro  :  sleep between ro txn in millisecond (default 0)\n"
          "   -E --epoch_type        :  can be queue (default), local\n"
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
    {"log_type", optional_argument, NULL, 'L'},
    {"log_directories", optional_argument, NULL, 'D'},
    {"checkpoint_type", optional_argument, NULL, 'C'},
    {"ckpt_directories", optional_argument, NULL, 'F'},
    {"timer_on", optional_argument, NULL, 'T'},
    {"sleep_between_ro", optional_argument, NULL, 'S'},
    {"epoch_type", optional_argument, NULL, 'E'},
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
    && state.gc_protocol != GC_TYPE_N2O_TXN
    && state.gc_protocol != GC_TYPE_N2O_EPOCH
    && state.gc_protocol != GC_TYPE_N2O_SNAPSHOT) {
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
    LOG_ERROR("Invalid epoch length :: %lf", state.epoch_length);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "epoch_length", state.epoch_length);
}

void ValidateRoSleepInterval(const configuration &state) {
  if (state.ro_sleep_between_txn > state.duration * 1000) {
    LOG_ERROR("Sleep interval between 2 readonly txn can not be larger than the exp's duration");
    exit(EXIT_FAILURE);
  }
  LOG_TRACE("%s : %d", "sleep between ro txn", state.ro_sleep_between_txn);
}

void ValidateEpochType(configuration &state) {
  if (state.gc_protocol == GC_TYPE_N2O_SNAPSHOT) {
    LOG_INFO("Use snapshot gc protocol");
    if (state.epoch_type == EPOCH_LOCALIZED) {
      LOG_INFO("Use localized snapshot epoch manager");
      state.epoch_type = EPOCH_LOCALIZED_SNAPSHOT;
    } else {
      state.epoch_type = EPOCH_SNAPSHOT;
    }
  }
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
  state.epoch_length = 10;
  state.logging_type = LOGGING_TYPE_INVALID;
  state.log_directories = {TMP_DIR};
  state.checkpoint_type = CHECKPOINT_TYPE_INVALID;
  state.checkpoint_directories = {TMP_DIR};
  state.timer_type = TIMER_OFF;
  state.ro_sleep_between_txn = 0;
  state.epoch_type = EPOCH_SINGLE_QUEUE;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "haexjk:d:s:c:l:r:o:u:b:z:p:g:i:t:y:v:n:q:w:f:L:D:T:S:E:C:F:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'S':
        state.ro_sleep_between_txn = atoi(optarg);
        break;
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
        state.epoch_length = atof(optarg);
        break;
      case 'E' : {
        char *epoch_type = optarg;
        if (strcmp(epoch_type, "queue") == 0) {
          state.epoch_type = EPOCH_SINGLE_QUEUE;
        } else if (strcmp(epoch_type, "local") == 0) {
          state.epoch_type = EPOCH_LOCALIZED;
        } else {
          fprintf(stderr, "\nUnknown epoch protocol: %s\n", epoch_type);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'T' : {
        char *timer_type = optarg;
        if (strcmp(timer_type, "off") == 0) {
          state.timer_type = TIMER_OFF;
        } else if (strcmp(timer_type, "sum") == 0) {
          state.timer_type = TIMER_SUMMARY;
        } else if (strcmp(timer_type, "dist") == 0) {
          state.timer_type = TIMER_DISTRIBUTION;
        } else {
          fprintf(stderr, "\nUnknown timer protocol: %s\n", timer_type);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'L': {
        char *logging_proto = optarg;
        if (strcmp(logging_proto, "off") == 0) {
          state.logging_type = LOGGING_TYPE_INVALID;
        } else if (strcmp(logging_proto, "phylog") == 0) {
          state.logging_type = LOGGING_TYPE_PHYLOG;
        } else {
          fprintf(stderr, "\nUnknown logging protocol: %s\n", logging_proto);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'D': {
        state.log_directories.clear();
        std::string log_dir_param(optarg);
        SplitString(log_dir_param, ',', state.log_directories);
        break;
      }
      case 'C': {
        char *checkpoint_proto = optarg;
        if (strcmp(checkpoint_proto, "off") == 0) {
          state.checkpoint_type = CHECKPOINT_TYPE_INVALID;
        } else if (strcmp(checkpoint_proto, "phylog") == 0) {
          state.checkpoint_type = CHECKPOINT_TYPE_PHYLOG;
        } else {
          fprintf(stderr, "\nUnknown checkpoint protocol: %s\n", checkpoint_proto);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'F': {
        state.checkpoint_directories.clear();
        std::string checkpoint_dir_param(optarg);
        SplitString(checkpoint_dir_param, ',', state.checkpoint_directories);
        break;
      }
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
        if (strcmp(gc_protocol, "off") == 0) {
          state.gc_protocol = GC_TYPE_OFF;
        } else if (strcmp(gc_protocol, "n2otxn") == 0) {
          state.gc_protocol = GC_TYPE_N2O_TXN;
        } else if (strcmp(gc_protocol, "n2oepoch") == 0) {
          state.gc_protocol = GC_TYPE_N2O_EPOCH;
        } else if (strcmp(gc_protocol, "n2oss") == 0) {
          state.gc_protocol = GC_TYPE_N2O_SNAPSHOT;
        }
        else {
          fprintf(stderr, "\nUnknown gc protocol: %s\n", gc_protocol);
          exit(EXIT_FAILURE);
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
  ValidateRoSleepInterval(state);
  ValidateEpoch(state);
  ValidateEpochType(state);

  LOG_TRACE("%s : %d", "Run exponential backoff", state.run_backoff);
  LOG_TRACE("%s : %d", "Run blind write", state.blind_write);
  LOG_TRACE("%s : %d", "Run declared read-only", state.declared);
}


void WriteOutput() {

  std::ofstream out("outputfile.summary", std::ofstream::out);

  oid_t total_snapshot_memory = 0;
  for (auto &entry : state.snapshot_memory) {
    total_snapshot_memory += entry;
  }

  LOG_INFO("%lf tps, %lf; %lf tps, %lf; %lf ms; %d",
             state.throughput, state.abort_rate, state.ro_throughput, state.ro_abort_rate, state.scan_latency, total_snapshot_memory);

  LOG_INFO("average commit latency: %lf ms", state.commit_latency);
  LOG_INFO("min commit latency: %lf ms", state.latency_summary.min_lat);
  LOG_INFO("max commit latency: %lf ms", state.latency_summary.max_lat);
  LOG_INFO("p50 commit latency: %lf ms", state.latency_summary.percentile_50);
  LOG_INFO("p90 commit latency: %lf ms", state.latency_summary.percentile_90);
  LOG_INFO("p99 commit latency: %lf ms", state.latency_summary.percentile_99);


  for (size_t round_id = 0; round_id < state.snapshot_throughput.size();
       ++round_id) {
    out << "[" << std::setw(3) << std::left
        << state.snapshot_duration * round_id << " - " << std::setw(3)
        << std::left << state.snapshot_duration * (round_id + 1)
        << " s]: " << state.snapshot_throughput[round_id] << " "
        << state.snapshot_abort_rate[round_id] << " " 
        << state.snapshot_memory[round_id] << "\n";
  }

  out << "scalefactor=" << state.scale_factor << " ";
  out << "skew=" << state.zipf_theta << " ";
  out << "update=" << state.update_ratio << " ";
  out << "opt=" << state.operation_count << " ";
  if (state.protocol == CONCURRENCY_TYPE_OPTIMISTIC) {
    out << "proto=occ ";
  } else if (state.protocol == CONCURRENCY_TYPE_PESSIMISTIC) {
    out << "proto=pcc ";
  } else if (state.protocol == CONCURRENCY_TYPE_SSI) {
    out << "proto=ssi ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO) {
    out << "proto=to ";
  } else if (state.protocol == CONCURRENCY_TYPE_EAGER_WRITE) {
    out << "proto=ewrite ";
  } else if (state.protocol == CONCURRENCY_TYPE_OCC_RB) {
    out << "proto=occrb ";
  } else if (state.protocol == CONCURRENCY_TYPE_OCC_CENTRAL_RB) {
    out << "proto=occ_central_rb ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO_CENTRAL_RB) {
    out << "proto=to_central_rb ";
  } else if (state.protocol == CONCURRENCY_TYPE_SPECULATIVE_READ) {
    out << "proto=sread ";
  } else if (state.protocol == CONCURRENCY_TYPE_OCC_N2O) {
    out << "proto=occn2o ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO_RB) {
    out << "proto=torb ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO_N2O) {
    out << "proto=ton2o ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO_FULL_RB) {
    out << "proto=tofullrb ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO_FULL_CENTRAL_RB) {
    out << "proto=to_full_central_rb ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO_OPT_N2O) {
    out << "proto=tooptn2o ";
  } else if (state.protocol == CONCURRENCY_TYPE_TO_SV) {
    out << "proto=tosv ";
  } else if (state.protocol == CONCURRENCY_TYPE_OCC_SV) {
    out << "proto=occsv ";
  } else if (state.protocol == CONCURRENCY_TYPE_OCC_SV_BEST) {
    out << "proto=occsvbest ";
  } 
  if (state.gc_protocol == GC_TYPE_OFF) {
    out << "gc=off ";
  }else if (state.gc_protocol == GC_TYPE_VACUUM) {
    out << "gc=va ";
  }else if (state.gc_protocol == GC_TYPE_CO) {
    out << "gc=co ";
  }else if (state.gc_protocol == GC_TYPE_N2O) {
    out << "gc=n2o ";
  } else if (state.gc_protocol == GC_TYPE_N2O_TXN) {
    out << "gc=n2otxn ";
  } else if (state.gc_protocol == GC_TYPE_SV) {
    out << "gc=sv ";
  }
  out << "column=" << state.column_count << " ";
  out << "read_column=" << state.read_column_count << " ";
  out << "update_column=" << state.update_column_count << " ";
  out << "core_cnt=" << state.backend_count << " ";
  out << "ro_core_cnt=" << state.ro_backend_count << " ";
  out << "scan_core_cnt=" << state.scan_backend_count << " ";
  out << "scan_mock_duration=" << state.scan_mock_duration << " ";
  out << "sindex_count=" << state.sindex_count << " ";
  if (state.sindex == SECONDARY_INDEX_TYPE_VERSION) {
    out << "sindex=version ";
  } else {
    out << "sindex=tuple ";
  }
  out << "\n";

  out << state.throughput << " ";
  out << state.abort_rate << " ";

  out << state.ro_throughput << " ";
  out << state.ro_abort_rate << " ";

  out << state.scan_latency << " ";

  out << total_snapshot_memory <<"\n";

  out << "average commit latency = " << state.commit_latency << "\n";
 out << "min commit latency = " <<  state.latency_summary.min_lat << "\n";
 out << "max commit latency = " <<  state.latency_summary.max_lat << "\n";
 out << "p50 commit latency = " <<  state.latency_summary.percentile_50 << "\n";
 out << "p90 commit latency = " <<  state.latency_summary.percentile_90 << "\n";
 out << "p99 commit latency = " <<  state.latency_summary.percentile_99 << "\n";
  out.flush();
  out.close();
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
