//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction.h
//
// Identification: src/include/common/profiler.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>

namespace peloton {
namespace concurrency {

class Profiler {

  using std::chrono;

  Profiler() {}

  static void BeginProfiling() {
    begin_time = steady_clock::now();
  }

  static void EndProfiling(bool report) {
    if (report == true) {

    } else {
    
      double diff = duration_cast<microseconds>(end_time - start_time).count();

    }
  }

  static void InsertTimePoint(const std::string point_name) {
    steady_clock::time_point now_time;
    time_points_.push_back(std::pair(now_time, point_name));
  }

private:
  steady_clock::time_point begin_time;
  steady_clock::time_point end_time;
  std::vector<std::pair<steady_clock::time_point, std::string>> time_points_;
}

}
}
