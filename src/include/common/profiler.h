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

#include <sys/time.h>
#include <atomic>
#include <vector>
#include <iostream>

namespace peloton {

class Profiler {

public:
  Profiler() {}

  static void BeginProfiling() {
    ++total_count_;

    struct timeval begin_time_;
    gettimeofday(&begin_time_, NULL);

    time_points_.clear();
    is_profiling_ = true;
  }

  static void EndProfiling() {
    if (total_count_ % 2000 == 0) {
      
      struct timeval end_time;
      gettimeofday(&end_time, NULL);

      std::cout << "=================================" << std::endl;
      
      for (auto &time_point : time_points_) {
        double diff = (time_point.first.tv_sec - begin_time_.tv_sec) * 1000.0 * 1000.0;
        diff + time_point.first.tv_usec - begin_time_.tv_usec;

        std::cout << "point: " << time_point.second << ", time: " << diff << " us." << std::endl;
      }

      double diff = end_time.tv_sec - begin_time_.tv_sec * 1000.0 * 1000.0;
      diff + end_time.tv_usec - begin_time_.tv_usec;

      std::cout << "point: END, time: " << diff << " us." << std::endl;

    }
    
    is_profiling_ = false;
  }

  static bool IsProfiling() {
    return is_profiling_;
  }

  static void InsertTimePoint(const std::string point_name) {
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    time_points_.push_back(std::make_pair(now_time, point_name));
  }

private:
  static std::atomic<int> total_count_;
  static struct timeval begin_time_;
  static std::vector<std::pair<struct timeval, std::string>> time_points_;
  static bool is_profiling_;
};

}
