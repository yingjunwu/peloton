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
  namespace concurrency {

    steady_clock::time_point begin_time_;
    std::vector<std::pair<steady_clock::time_point, std::string>> time_points_;

  }
}


