//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_worker_context.h
//
// Identification: src/backend/logging/epoch_worker_context.h
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
namespace logging {

  // the worker context is constructed when registering the worker to the logger.
  struct EpochWorkerContext {

    EpochWorkerContext(oid_t id)
      : current_eid(START_EPOCH_ID), 
        worker_id(id),
        cur_txn_start_time(0),
        pending_txn_timers(),
        txn_summary() {
      LOG_TRACE("Create worker %d", (int) worker_id);
    }

    ~EpochWorkerContext() {
      LOG_TRACE("Destroy worker %d", (int) worker_id);
    }

    
    std::unordered_map<oid_t, std::unordered_map<oid_t, ItemPointer>> delta_snapshot_;

    // current epoch id
    size_t current_eid;
    
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