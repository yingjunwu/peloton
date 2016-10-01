//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_context.h
//
// Identification: src/backend/logging/loggers/log_context.h
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
#include <backend/common/logger.h>

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
  struct LogWorkerContext {
    // Every epoch has a buffer stack
    std::vector<std::stack<std::unique_ptr<LogBuffer>>> per_epoch_buffer_ptrs;
    // each worker thread has a buffer pool. each buffer pool contains 16 log buffers.
    LogBufferPool buffer_pool;
    // serialize each tuple to string.
    CopySerializeOutput output_buffer;

    size_t current_eid;
    cid_t current_cid;
    oid_t worker_id;

    // When a worker terminates, we cannot destruct it immediately.
    // What we do is first set this flag and the logger will check if it can destruct a terminated worker
    // TODO: Find out some where to call termination to a worker
    bool terminated;

    LogWorkerContext(oid_t id)
      : per_epoch_buffer_ptrs(concurrency::EpochManager::GetEpochQueueCapacity()),
        buffer_pool(id), output_buffer(),
        current_eid(INVALID_EPOCH_ID), current_cid(INVALID_CID), worker_id(id), terminated(false)
    {
      LOG_TRACE("Create worker %d", (int) id);
    }

    ~LogWorkerContext() {
      LOG_TRACE("Destroy worker %d", (int) worker_id);
    }
  };


  struct LoggerContext {
    // logger id
    size_t lid;
    // logger thread
    std::unique_ptr<std::thread> logger_thread;

    /* File system related */
    std::string log_dir;
    size_t next_file_id;
    CopySerializeOutput logger_output_buffer;
    FileHandle cur_file_handle;

    /* Log buffers */
    size_t max_committed_eid;

    // The spin lock to protect the worker map. We only update this map when creating/terminating a new worker
    Spinlock worker_map_lock_;
    // map from worker id to the worker's context.
    std::unordered_map<oid_t, std::shared_ptr<LogWorkerContext>> worker_map_;
    
    std::vector<std::stack<std::unique_ptr<LogBuffer>>> local_buffer_map;

    LoggerContext() :
      lid(INVALID_LOGGERID), logger_thread(nullptr), log_dir(), next_file_id(0), logger_output_buffer(),
      cur_file_handle(), max_committed_eid(INVALID_EPOCH_ID), worker_map_lock_(), worker_map_(),
      local_buffer_map(concurrency::EpochManager::GetEpochQueueCapacity())
    {}

    ~LoggerContext() {}
  };


}
}