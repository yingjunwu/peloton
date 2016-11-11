//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: benchmark/smallbank/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <fstream>
#include <iomanip>
#include <algorithm>
#include <string.h>

#include "backend/benchmark/smallbank/smallbank_configuration.h"
#include "backend/common/logger.h"

namespace peloton {
namespace benchmark {
namespace smallbank {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : smallbank <options> \n"
          "   -h --help              :  Print help message \n"
          "   -z --zipf_theta        :  theta to control skewness \n"
          "   -i --index             :  index type could be hash index or bwtree\n"
          "   -k --scale_factor      :  scale factor \n"
          "   -d --duration          :  execution duration \n"
          "   -s --snapshot_duration :  snapshot duration \n"
          "   -b --backend_count     :  # of backends \n"
          "   -e --exp_backoff       :  enable exponential backoff \n"
          "   -p --protocol          :  choose protocol, default OCC\n"
          "                             protocol could be occ, pcc, pccopt, ssi, sread, ewrite, occrb, occn2o, to, torb, tofullrb, occ_central_rb, to_central_rb, to_full_central_rb and ton2o\n"
          "   -g --gc_protocol       :  choose gc protocol, default OFF\n"
          "                             gc protocol could be off, n2otxn, n2oepoch, n2oss\n"
          "   -t --gc_thread         :  number of thread used in gc, only used for gc type n2o/va/n2otxn\n"
          "   -q --sindex_mode       :  secondary index mode: version or tuple\n"
          "   -f --epoch_length      :  epoch length\n"
          "   -L --log_type          :  log type could be phylog, physical, command, dep, off\n"
          "   -D --log_directories   :  multiple log directories, e.g., /data1/,/data2/,/data3/,...\n"
          "   -C --checkpoint_type   :  checkpoint type could be phylog, physical, off\n"
          "   -F --ckpt_directories  :  multiple checkpoint directories, e.g., /data1/,/data2/,/data3/,...\n"
          "   -I --ckpt_interval     :  checkpoint interval (s)\n"
          "   -T --timer_type        :  timer type could be off, sum, dist. Default is off\n"
          "   -E --epoch_type        :  can be queue (default), local\n"
          "   -R --recover_ckpt      :  recover checkpoint\n"
          "   -P --replay_log        :  replay log\n"
          "   -M --recover_ckpt_num  :  # threads for recovering checkpoints\n"
          "   -N --replay_log_num    :  # threads for replaying logs\n"
          "   -A --adhoc_ratio       :  percentage of ad-hoc transactions\n"
  );
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
  { "scale_factor", optional_argument, NULL, 'k'},
  { "index", optional_argument, NULL, 'i'},
  { "duration", optional_argument, NULL, 'd' },
  { "snapshot_duration", optional_argument, NULL, 's'},
  { "backend_count", optional_argument, NULL, 'b'},
  { "exp_backoff", no_argument, NULL, 'e'},
  { "protocol", optional_argument, NULL, 'p'},
  { "gc_protocol", optional_argument, NULL, 'g'},
  { "gc_thread", optional_argument, NULL, 't'},
  { "sindex_mode", optional_argument, NULL, 'q'},
  { "epoch_length", optional_argument, NULL, 'f'},
  { "zipf_theta", optional_argument, NULL, 'z'},
  { "log_type", optional_argument, NULL, 'L'},
  { "log_directories", optional_argument, NULL, 'D'},
  { "checkpoint_type", optional_argument, NULL, 'C'},
  { "ckpt_directories", optional_argument, NULL, 'F'},
  { "ckpt_interval", optional_argument, NULL, 'I'},
  { "timer_type", optional_argument, NULL, 'T'},
  { "epoch_type", optional_argument, NULL, 'E'},
  { "recover_ckpt", no_argument, NULL, 'R'},
  { "replay_log", no_argument, NULL, 'P'},
  { "recover_ckpt_num", optional_argument, NULL, 'M'},
  { "replay_log_num", optional_argument, NULL, 'N'},
  { "adhoc_ratio", optional_argument, NULL, 'A'},
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

void ValidateEpochType(configuration &state) {
  if (state.logging_type == LOGGING_TYPE_DEPENDENCY) {
    if (state.epoch_type != EPOCH_LOCALIZED && state.epoch_type != EPOCH_LOCALIZED_SNAPSHOT) {
      LOG_ERROR("Dependency logging should use localized epoch manager");
      exit(EXIT_FAILURE);
    } else {
      LOG_INFO("Use dependency logging");
    }
  }

  if (state.gc_protocol == GC_TYPE_N2O_SNAPSHOT) {
    LOG_INFO("Use snapshot GC manager");
    if (state.epoch_type == EPOCH_SINGLE_QUEUE) {
      state.epoch_type = EPOCH_SNAPSHOT;
    } else {
      state.epoch_type = EPOCH_LOCALIZED_SNAPSHOT;
    }
  }
}

void ValidateLoggingType(configuration &state) {
  if (state.logging_type == LOGGING_TYPE_PHYLOG) {
    if (state.checkpoint_type == CHECKPOINT_TYPE_PHYSICAL) {
      LOG_ERROR("logging and checkpointing types inconsistent!");
      exit(EXIT_FAILURE);
    }
  }
  else if (state.logging_type == LOGGING_TYPE_PHYSICAL) {
    if (state.checkpoint_type == CHECKPOINT_TYPE_PHYLOG) {
      LOG_ERROR("logging and checkpointing types inconsistent!");
      exit(EXIT_FAILURE);
    }
  }
  if (state.logging_type == LOGGING_TYPE_COMMAND) {
    if (state.checkpoint_type == CHECKPOINT_TYPE_PHYSICAL) {
      LOG_ERROR("logging and checkpointing types inconsistent!");
      exit(EXIT_FAILURE);
    }
  }

  if (state.recover_checkpoint == true && state.checkpoint_type == CHECKPOINT_TYPE_INVALID) {
    LOG_ERROR("must set checkpoint type when performing checkpoint recovery!");
    exit(EXIT_FAILURE);
  }
  if (state.replay_log == true && state.logging_type == LOGGING_TYPE_INVALID) {
    LOG_ERROR("must set logging type when performing log replay!");
    exit(EXIT_FAILURE);
  }
}

void ValidateAdhocRatio(const configuration &state) {
  if (state.adhoc_ratio < 0 || state.adhoc_ratio > 1) {
    LOG_ERROR("Invalid adhoc_ratio :: %lf", state.adhoc_ratio);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "adhoc_ratio", state.adhoc_ratio);
}

void ParseArguments(int argc, char *argv[], configuration &state) {
  // Default Values
  state.scale_factor = 1;
  state.zipf_theta = 0.0;
  state.duration = 10;
  state.snapshot_duration = 1;
  state.backend_count = 1;
  state.run_backoff = false;
  state.protocol = CONCURRENCY_TYPE_TO_N2O;
  state.gc_protocol = GC_TYPE_N2O_TXN;
  state.index = INDEX_TYPE_HASH;
  state.gc_thread_count = 1;
  state.sindex = SECONDARY_INDEX_TYPE_TUPLE;
  state.epoch_length = 10;
  state.logging_type = LOGGING_TYPE_INVALID;
  state.log_directories = {TMP_DIR};
  state.checkpoint_type = CHECKPOINT_TYPE_INVALID;
  state.checkpoint_directories = {TMP_DIR};
  state.checkpoint_interval = 30;
  state.timer_type = TIMER_OFF;
  state.epoch_type = EPOCH_LOCALIZED;
  state.recover_checkpoint = false;
  state.replay_log = false;
  state.recover_checkpoint_num = 1;
  state.replay_log_num = 1;
  state.adhoc_ratio = 0;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "RPeh:z:k:d:s:b:p:g:i:t:q:f:L:D:T:E:C:F:I:M:N:A:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'A':
        state.adhoc_ratio = atof(optarg);
        break;
      case 'M':
        state.recover_checkpoint_num = atoi(optarg);
        break;
      case 'N':
        state.replay_log_num = atoi(optarg);
        break;
      case 'R':
        state.recover_checkpoint = true;
        break;
      case 'P':
        state.replay_log = true;
        break;
      case 't':
        state.gc_thread_count = atoi(optarg);
        break;
      case 'k':
        state.scale_factor = atof(optarg);
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
      case 'e':
        state.run_backoff = true;
        break;
      case 'f':
        state.epoch_length = atoi(optarg);
        break;
      case 'z':
        state.zipf_theta = atof(optarg);
        break;
      case 'E' : {
        char *epoch_type = optarg;
        if (strcmp(epoch_type, "queue") == 0) {
          state.epoch_type = EPOCH_SINGLE_QUEUE;
        } else if (strcmp(epoch_type, "local") == 0) {
          state.epoch_type = EPOCH_LOCALIZED;
        } else if (strcmp(epoch_type, "localsnapshot") == 0) {
          state.epoch_type = EPOCH_LOCALIZED_SNAPSHOT;
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
        }
        break;
      }
      case 'L': {
        char *logging_proto = optarg;
        if (strcmp(logging_proto, "off") == 0) {
          state.logging_type = LOGGING_TYPE_INVALID;
        } else if (strcmp(logging_proto, "phylog") == 0) {
          state.logging_type = LOGGING_TYPE_PHYLOG;
        } else if (strcmp(logging_proto, "physical") == 0) {
          state.logging_type = LOGGING_TYPE_PHYSICAL;
        } else if (strcmp(logging_proto, "command") == 0) {
          state.logging_type = LOGGING_TYPE_COMMAND;
        } else if (strcmp(logging_proto, "dep") == 0) {
          state.logging_type = LOGGING_TYPE_DEPENDENCY;
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
        } else if (strcmp(checkpoint_proto, "physical") == 0) {
          state.checkpoint_type = CHECKPOINT_TYPE_PHYSICAL;
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
      case 'I': {
        state.checkpoint_interval = atoi(optarg);
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
        if (strcmp(gc_protocol, "off") == 0) {
          state.gc_protocol = GC_TYPE_OFF;
        } else if (strcmp(gc_protocol, "n2otxn") == 0) {
          state.gc_protocol = GC_TYPE_N2O_TXN;
        } else if (strcmp(gc_protocol, "n2oepoch") == 0) {
          state.gc_protocol = GC_TYPE_N2O_EPOCH;
        } else if (strcmp(gc_protocol, "n2oss") == 0) {
          state.gc_protocol = GC_TYPE_N2O_SNAPSHOT;
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
  state.account_count = 1000000 * state.scale_factor;

  // Print configuration
  ValidateScaleFactor(state);
  ValidateDuration(state);
  ValidateSnapshotDuration(state);
  ValidateBackendCount(state);
  ValidateProtocol(state);
  ValidateIndex(state);
  ValidateEpoch(state);
  ValidateEpochType(state);
  ValidateAdhocRatio(state);
  ValidateZipfTheta(state);
  
  LOG_TRACE("%s : %d", "Run exponential backoff", state.run_backoff);

}


void WriteOutput() {
  
  std::ofstream out("outputfile.summary", std::ofstream::out);
  
  oid_t total_snapshot_memory = 0;
  for (auto &entry : state.snapshot_memory) {
    total_snapshot_memory += entry;
  }

  LOG_INFO("%lf tps, %lf, %d", 
    state.throughput, state.abort_rate, 
    total_snapshot_memory);
  LOG_INFO("average commit latency: %lf ms", state.commit_latency);
  // LOG_INFO("min commit latency: %lf ms", state.latency_summary.min_lat);
  // LOG_INFO("max commit latency: %lf ms", state.latency_summary.max_lat);
  // LOG_INFO("p50 commit latency: %lf ms", state.latency_summary.percentile_50);
  // LOG_INFO("p90 commit latency: %lf ms", state.latency_summary.percentile_90);
  // LOG_INFO("p99 commit latency: %lf ms", state.latency_summary.percentile_99);


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
  } else if (state.protocol == CONCURRENCY_TYPE_OCC_BEST_N2O) {
    out << "proto=occbestn2o ";
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
  out << "core_cnt=" << state.backend_count << " ";
  if (state.sindex == SECONDARY_INDEX_TYPE_VERSION) {
    out << "sindex=version ";
  } else {
    out << "sindex=tuple ";
  }
  if (state.logging_type == LOGGING_TYPE_INVALID) {
    out << "log=off ";
  } else if (state.logging_type == LOGGING_TYPE_COMMAND) {
    out << "log=command ";
  } else if (state.logging_type == LOGGING_TYPE_PHYSICAL) {
    out << "log=physical ";
  } else if (state.logging_type == LOGGING_TYPE_PHYLOG) {
    out << "log=phylog ";
  } else if (state.logging_type == LOGGING_TYPE_DEPENDENCY) {
    out << "log=dep ";
  }
  out << "log_count=" << state.log_directories.size() << " ";
  if (state.checkpoint_type == CHECKPOINT_TYPE_INVALID) {
    out << "ckpt=off ";
  } else if (state.checkpoint_type == CHECKPOINT_TYPE_PHYSICAL) {
    out << "ckpt=physical ";
  } else if (state.checkpoint_type == CHECKPOINT_TYPE_PHYLOG) {
    out << "ckpt=phylog ";
  }
  out << "ckpt_count=" << state.checkpoint_directories.size() << " ";
  out << "\n";

  out << state.throughput << " ";
  out << state.abort_rate << " ";

  out << total_snapshot_memory <<" ";

  out << state.commit_latency << " ";
  out << state.latency_summary.min_lat << " ";
  out << state.latency_summary.max_lat << " ";
  out << state.latency_summary.percentile_50 << " ";
  out << state.latency_summary.percentile_90 << " ";
  out << state.latency_summary.percentile_99 << "\n";

  out.flush();
  out.close();
}

}  // namespace smallbank
}  // namespace benchmark
}  // namespace peloton