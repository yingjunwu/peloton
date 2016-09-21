//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger_configuration.h
//
// Identification: src/include/benchmark/logger/logger_configuration.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <string>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "backend/common/types.h"
#include "backend/common/pool.h"

namespace peloton {
namespace benchmark {
namespace logger {


enum BenchmarkType {
  BENCHMARK_TYPE_INVALID = 0,

  BENCHMARK_TYPE_YCSB = 1,
  BENCHMARK_TYPE_TPCC = 2
};


class configuration {
 public:

  // logging type
  LoggingType logging_type;

  // checkpoint type
  CheckpointType checkpoint_type;

  // log file dir
  std::string log_file_dir;

  // size of the pmem file (in MB)
  size_t data_file_size;

  // Benchmark type
  BenchmarkType benchmark_type;

};

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
