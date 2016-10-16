//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dep_worker_context.h
//
// Identification: src/backend/logging/dep_worker_context.h
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
#include "backend/logging/worker_context.h"
#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"

namespace peloton {
  namespace concurrency {
    extern thread_local size_t lt_txn_worker_id;
  }
  namespace logging {

    // the worker context is constructed when registering the worker to the logger.
    struct DepWorkerContext {

      DepWorkerContext(oid_t id)
        : per_epoch_buffer_ptrs(concurrency::EpochManager::GetEpochQueueCapacity()),
          per_epoch_dependencies(concurrency::EpochManager::GetEpochQueueCapacity()),
          buffer_pool(id),
          output_buffer(),
          current_eid(START_EPOCH_ID),
          last_seen_eid(INVALID_EPOCH_ID),
          txn_worker_id(INVALID_TXN_WORKER_ID),
          current_cid(INVALID_CID),
          worker_id(id),
          cur_txn_start_time(0),
          pending_txn_timers(),
          txn_summary() {
        LOG_TRACE("Create worker %d", (int) worker_id);
        PL_ASSERT(concurrency::lt_txn_worker_id != INVALID_TXN_WORKER_ID);
        txn_worker_id = concurrency::lt_txn_worker_id;
      }

      ~DepWorkerContext() {
        LOG_TRACE("Destroy worker %d", (int) worker_id);
      }


      // Every epoch has a buffer stack
      std::vector<std::stack<std::unique_ptr<LogBuffer>>> per_epoch_buffer_ptrs;
      // Per epoch dependency graph
      std::vector<std::unordered_set<size_t>> per_epoch_dependencies;

      // each worker thread has a buffer pool. each buffer pool contains 16 log buffers.
      LogBufferPool buffer_pool;
      // serialize each tuple to string.
      CopySerializeOutput output_buffer;

      // current epoch id
      size_t current_eid;
      // persisted epoch id
      size_t last_seen_eid;

      // worker's id in the epoch manager
      size_t txn_worker_id;

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