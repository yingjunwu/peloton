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
#include <unordered_map>

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


namespace peloton {
namespace logging {

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
        terminated(false) {
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

    // When a worker terminates, we cannot destruct it immediately.
    // What we do is first set this flag and the logger will check if it can destruct a terminated worker
    // TODO: Find out some where to call termination to a worker
    bool terminated;

  };

}
}