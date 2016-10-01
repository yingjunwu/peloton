//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.h
//
// Identification: src/backend/benchmark/ycsb_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/benchmark/benchmark_common.h"

#include <sstream>

namespace peloton {
  namespace benchmark {
    // Helper function to pin current thread to a specific core
    void PinToCore(size_t core) {
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(core, &cpuset);
      pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    }


	void SplitString(const std::string &src_str, char delim, std::vector<std::string> &ret_strs) {
	  std::stringstream ss;
	  ss.str(src_str);
	  std::string ret_str;
	  while (std::getline(ss, ret_str, delim)) {
	    ret_strs.push_back(ret_str);
	  }
	}

  }
}