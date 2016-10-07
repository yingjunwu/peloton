//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_logger.h
//
// Identification: src/backend/logging/epoch_logger.h
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
#include "backend/common/pool.h"


namespace peloton {
  
namespace storage {
  class TileGroupHeader;
}

namespace logging {

  class EpochLogger {

  public:
    EpochLogger(const size_t &logger_id, const std::string &log_dir) :
      logger_id_(logger_id),
      log_dir_(log_dir),
      logger_thread_(nullptr),
      is_running_(false),
      logger_output_buffer_(), 
      file_handle_(),
      next_file_id_(0),
      persist_epoch_id_(INVALID_EPOCH_ID),
      worker_map_lock_(), 
      worker_map_()
    {}

    ~EpochLogger() {}

    void StartLogging() {
      is_running_ = true;
      logger_thread_.reset(new std::thread(&EpochLogger::Run, this));
    }

    void StopLogging() {
      is_running_ = false;
      logger_thread_->join();
    }

    void RegisterWorker(EpochWorkerContext *phylog_worker_ctx);
    void DeregisterWorker(EpochWorkerContext *phylog_worker_ctx);

    size_t GetPersistEpochId() const {
      return persist_epoch_id_;
    }


private:
  void Run();

  void PersistEpochBegin(const size_t epoch_id);
  void PersistEpochEnd(const size_t epoch_id);

  std::string GetLogFileFullPath(size_t epoch_id) {
    return log_dir_ + "/" + logging_filename_prefix_ + "_" + std::to_string(logger_id_) + "_" + std::to_string(epoch_id);
  }

  private:
    size_t logger_id_;
    std::string log_dir_;
    
    // logger thread
    std::unique_ptr<std::thread> logger_thread_;
    volatile bool is_running_;

    /* File system related */
    CopySerializeOutput logger_output_buffer_;
    FileHandle file_handle_;
    size_t next_file_id_;

    /* Log buffers */
    size_t persist_epoch_id_;

    // The spin lock to protect the worker map. We only update this map when creating/terminating a new worker
    Spinlock worker_map_lock_;
    // map from worker id to the worker's context.
    std::unordered_map<oid_t, std::shared_ptr<EpochWorkerContext>> worker_map_;

    const std::string logging_filename_prefix_ = "log";

    const size_t sleep_period_us_ = 40000;
  };


}
}