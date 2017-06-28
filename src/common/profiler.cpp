//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction.h
//
// Identification: src/common/profiler.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/profiler.h"

namespace peloton {

  std::atomic<int> Profiler::total_count_(0);
  struct timeval Profiler::begin_time_;
  std::vector<std::pair<struct timeval, std::string>> Profiler::time_points_;
  bool Profiler::is_profiling_ = false;

}

