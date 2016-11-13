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
  namespace concurrency {
    extern thread_local size_t tl_txn_worker_id;
  }

namespace logging {

  class TxnSummary {
  private:
    std::list<uint64_t> per_batch_txn_lat;
    std::unique_ptr<std::list<uint64_t>> per_txn_lat;

    uint64_t last_count;
    uint64_t last_total_usec;

    const static uint64_t batch_size = 100;
  public:
    TxnSummary() : per_batch_txn_lat(), per_txn_lat(new std::list<uint64_t>()), last_count(0), last_total_usec(0) {};
    ~TxnSummary() {}

    void AddTxnLatReport(uint64_t lat, bool distribution = false) {
        last_total_usec += lat;
        if ((++last_count) == batch_size) {
          per_batch_txn_lat.push_back(last_total_usec);
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

    double GetRecentAvgLatency() {
      if (per_batch_txn_lat.empty()) {
        return 0;
      }
      auto last_10k = per_batch_txn_lat.rbegin();
      return *last_10k / 1000.0 / batch_size;
    }

    double GetAverageLatencyInMs() {
      double avg_sum = 0.0;
      for (uint64_t lat_10k : per_batch_txn_lat) {
        avg_sum += (lat_10k) * 1.0 / batch_size;
      }

      double last_avg = 0.0;
      if (last_count != 0) last_avg = last_total_usec * 1.0 / last_count;

      return ((avg_sum + last_avg) / (per_batch_txn_lat.size() + last_count * 1.0 / batch_size)) / 1000;
    }
  };

  
  // the worker context is constructed when registering the worker to the logger.
  struct WorkerContext {

    WorkerContext(oid_t id)
      : per_epoch_buffer_ptrs(concurrency::EpochManager::GetEpochQueueCapacity()),
        buffer_pool(id), 
        output_buffer(),
        current_commit_eid(MAX_EPOCH_ID),
        persist_eid(0),
        reported_eid(INVALID_EPOCH_ID),
        current_cid(INVALID_CID), 
        worker_id(id),
        transaction_worker_id(INVALID_TXN_WORKER_ID),
        cur_txn_start_time(0),
        pending_txn_timers(),
        txn_summary(),
        per_epoch_dependencies(concurrency::EpochManager::GetEpochQueueCapacity()) {
      LOG_TRACE("Create worker %d", (int) worker_id);
      PL_ASSERT(concurrency::tl_txn_worker_id != INVALID_TXN_WORKER_ID);
      transaction_worker_id = concurrency::tl_txn_worker_id;
    }

    ~WorkerContext() {
      LOG_TRACE("Destroy worker %d", (int) worker_id);
    }


    // Every epoch has a buffer stack
    // TODO: Remove this, workers should push the buffer to the logger. -- Jiexi
    std::vector<std::stack<std::unique_ptr<LogBuffer>>> per_epoch_buffer_ptrs;

    // each worker thread has a buffer pool. each buffer pool contains 16 log buffers.
    LogBufferPool buffer_pool;
    // serialize each tuple to string.
    CopySerializeOutput output_buffer;

    // current epoch id
    size_t current_commit_eid;
    // persisted epoch id
    // TODO: Move this to logger -- Jiexi
    size_t persist_eid;
    // reported epoch id
    size_t reported_eid;

    // current transaction id
    cid_t current_cid;

    // worker thread id
    oid_t worker_id;
    // transaction worker id from the epoch manager's point of view
    size_t transaction_worker_id;

    /* Statistics */

    // XXX: Simulation of early lock release
    uint64_t cur_txn_start_time;
    std::map<size_t, std::vector<uint64_t>> pending_txn_timers;
    TxnSummary txn_summary;

    // Note: Only used by dep log manager
    // Per epoch dependency graph
    // TODO: Remove this, workers should push the dependencies along with the buffer to the logger. -- Jiexi
    std::vector<std::unordered_set<size_t>> per_epoch_dependencies;
  };

}
}