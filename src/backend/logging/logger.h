//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger.h
//
// Identification: src/backend/logging/loggers/logger.h
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

  class Logger {

  public:
    Logger() :
      logger_thread_(nullptr), 
      log_dir(), 
      next_file_id(0), 
      logger_output_buffer(),
      cur_file_handle(), 
      max_committed_eid(INVALID_EPOCH_ID), 
      worker_map_lock_(), 
      worker_map_(),
      local_buffer_map(concurrency::EpochManager::GetEpochQueueCapacity())
    {}

    ~Logger() {}

    void RegisterWorker(WorkerLogContext *log_worker_ctx) {
      worker_map_lock_.Lock();
      worker_map_[log_worker_ctx->worker_id].reset(log_worker_ctx);
      worker_map_lock_.Unlock();
    }

    // logger thread
    std::unique_ptr<std::thread> logger_thread_;

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
    std::unordered_map<oid_t, std::shared_ptr<WorkerLogContext>> worker_map_;
    
    std::vector<std::stack<std::unique_ptr<LogBuffer>>> local_buffer_map;

  };


}
}