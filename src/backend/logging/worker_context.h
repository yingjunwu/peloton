//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_context.h
//
// Identification: src/backend/logging/worker_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <thread>
#include <list>
#include <stack>
#include <map>

#include "libcuckoo/cuckoohash_map.hh"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/epoch_manager.h"
#include "backend/logging/log_buffer.h"
#include "backend/logging/log_record.h"
#include "backend/logging/log_buffer_pool.h"
#include "backend/logging/log_manager.h"
#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"

namespace peloton {
namespace logging {

  class TxnSummary {
  private:
    std::vector<uint64_t> per_10k_txn_lat;
    uint64_t last_count;
    uint64_t last_total_usec;

    const static uint64_t batch_size = 10000;
  public:
    TxnSummary() : per_10k_txn_lat(), last_count(0), last_total_usec(0) {};
    ~TxnSummary() {}

    void AddTxnLatReport(uint64_t lat) {
        last_total_usec += lat;
        if ((++last_count) == batch_size) {
          per_10k_txn_lat.push_back(last_total_usec);
          last_count = 0;
          last_total_usec = 0;
        }
    }

    double GetAverageLatencyInMs() {
      double avg_sum = 0.0;
      for (uint64_t lat_10k : per_10k_txn_lat) {
        avg_sum += (lat_10k) * 1.0 / batch_size;
      }

      double last_avg = 0.0;
      if (last_count != 0) last_avg = last_total_usec * 1.0 / last_count;

      return ((avg_sum + last_avg) / (per_10k_txn_lat.size() + last_count * 1.0 / batch_size)) / 1000;
    }
  };

}
}