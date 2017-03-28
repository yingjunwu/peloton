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
#include <iostream>

namespace peloton {
namespace concurrency {

class Profiler {

  using std::chrono;

  Profiler() {}

  static void BeginProfiling() {
    time_points_.clear();
    begin_time_ = steady_clock::now();
  }

  static void EndProfiling(const bool report) {
    if (report == true) {
      steady_clock::time_point end_time = steady_clock::now();

      std::cout << "=================================" << std::endl;
      for (auto &time_point : time_points_) {
        double diff = duration_cast<microseconds>(time_point.first - begin_time_).count();
        std::cout << "point: " << time_point.second << ", time: " << diff << " us." << std::endl;
      }

      double diff = duration_cast<microseconds>(end_time - begin_time_).count();
      std::cout << "point: END, time: " << diff << " us." << std::endl;

    }
  }

  static void InsertTimePoint(const std::string point_name) {
    steady_clock::time_point now_time;
    time_points_.push_back(std::pair(now_time, point_name));
  }

private:
  static steady_clock::time_point begin_time_;
  static std::vector<std::pair<steady_clock::time_point, std::string>> time_points_;
}

}
}
