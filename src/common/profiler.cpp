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
  steady_clock::time_point Profiler::begin_time_;
  std::vector<std::pair<steady_clock::time_point, std::string>> Profiler::time_points_;
  bool Profiler::is_profiling_ = true;

}

