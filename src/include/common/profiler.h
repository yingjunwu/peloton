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

#include <atomic>
#include <chrono>
#include <vector>
#include <iostream>

using namespace std::chrono;

namespace peloton {

class Profiler {

public:
  Profiler() {}

  static void BeginProfiling() {
    ++total_count_;
    time_points_.clear();
    begin_time_ = steady_clock::now();
    is_profiling_ = true;
  }

  static void EndProfiling() {
    if (total_count_ % 2000 == 0) {
      steady_clock::time_point end_time = steady_clock::now();

      std::cout << "=================================" << std::endl;
      for (auto &time_point : time_points_) {
        double diff = duration_cast<microseconds>(time_point.first - begin_time_).count();
        std::cout << "point: " << time_point.second << ", time: " << diff << " us." << std::endl;
      }

      double diff = duration_cast<microseconds>(end_time - begin_time_).count();
      std::cout << "point: END, time: " << diff << " us." << std::endl;

    }
    is_profiling_ = false;
  }

  static bool IsProfiling() {
    return is_profiling_;
  }

  static void InsertTimePoint(const std::string point_name) {
    steady_clock::time_point now_time = steady_clock::now();
    time_points_.push_back(std::make_pair(now_time, point_name));
  }

private:
  static std::atomic<int> total_count_;
  static steady_clock::time_point begin_time_;
  static std::vector<std::pair<steady_clock::time_point, std::string>> time_points_;
  static bool is_profiling_;
};

}
