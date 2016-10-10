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
    std::unique_ptr<std::list<uint64_t>> per_txn_lat;

    uint64_t last_count;
    uint64_t last_total_usec;

    const static uint64_t batch_size = 10000;
  public:
    TxnSummary() : per_10k_txn_lat(), per_txn_lat(new std::list<uint64_t>()), last_count(0), last_total_usec(0) {};
    ~TxnSummary() {}

    void AddTxnLatReport(uint64_t lat, bool distribution = false) {
        last_total_usec += lat;
        if ((++last_count) == batch_size) {
          per_10k_txn_lat.push_back(last_total_usec);
          last_count = 0;
          last_total_usec = 0;
        }

        if (distribution) {
          per_txn_lat->push_back(lat);
        }
    }

    const std::list<uint64_t>* GetLatencyList() {
      return per_txn_lat.get();
    }

    LatSummary GetLatSummary() {
      LatSummary res = LatSummary();
      if (per_txn_lat->empty() == true) return res;

      size_t len = per_txn_lat->size();
      size_t len_at_p50 = len / 2;
      size_t len_at_p90 = len * 9 / 10;
      size_t len_at_p99 = len * 99 / 100;

      // Start the computation... may be time consuming
      res.average_lat = GetAverageLatencyInMs();
      res.txn_count = len;

      per_txn_lat->sort();
      auto itr = per_txn_lat->begin();
      for (size_t idx = 0; idx < len; ++idx, ++itr) {
        if (idx == len_at_p50) {
          res.percentile_50 = *itr / 1000.0;
        } else if (idx == len_at_p90) {
          res.percentile_90 = *itr / 1000.0;
        } else if (idx == len_at_p99) {
          res.percentile_99 = *itr / 1000.0;
        };
      }

      res.min_lat = *(per_txn_lat->begin()) / 1000.0;
      res.max_lat = *(per_txn_lat->rbegin()) / 1000.0;
      return res;
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