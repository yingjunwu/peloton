//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_log_context.h
//
// Identification: src/backend/logging/loggers/worker_log_context.h
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

  // the worker context is constructed when registering the worker to the logger.
  struct WorkerLogContext {

    WorkerLogContext(oid_t id)
      : per_epoch_buffer_ptrs(concurrency::EpochManager::GetEpochQueueCapacity()),
        buffer_pool(id), 
        output_buffer(),
        current_eid(START_EPOCH_ID), 
        persist_eid(INVALID_EPOCH_ID),
        current_cid(INVALID_CID), 
        worker_id(id),
        cur_txn_start_time(0),
        pending_txn_timers(),
        txn_summary() {
      LOG_TRACE("Create worker %d", (int) worker_id);
    }

    ~WorkerLogContext() {
      LOG_TRACE("Destroy worker %d", (int) worker_id);
    }


    // Every epoch has a buffer stack
    std::vector<std::stack<std::unique_ptr<LogBuffer>>> per_epoch_buffer_ptrs;
    // each worker thread has a buffer pool. each buffer pool contains 16 log buffers.
    LogBufferPool buffer_pool;
    // serialize each tuple to string.
    CopySerializeOutput output_buffer;

    // current epoch id
    size_t current_eid;
    // persisted epoch id
    size_t persist_eid;

    // current transaction id
    cid_t current_cid;

    // worker thread id
    oid_t worker_id;

    /* Statistics */

    // XXX: Simulation of early lock release
    uint64_t cur_txn_start_time;
    std::map<size_t, std::vector<uint64_t>> pending_txn_timers;
    TxnSummary txn_summary;
  };

}
}