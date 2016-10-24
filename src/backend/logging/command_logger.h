//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// command_logger.h
//
// Identification: src/backend/logging/command_logger.h
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
#include "backend/logging/worker_context.h"
#include "backend/logging/command_logging_util.h"
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

  class CommandLogger {

  public:
    CommandLogger(const size_t &logger_id, const std::string &log_dir) :
      logger_id_(logger_id),
      log_dir_(log_dir),
      logger_thread_(nullptr),
      is_running_(false),
      logger_output_buffer_(), 
      persist_epoch_id_(INVALID_EPOCH_ID),
      worker_map_lock_(), 
      worker_map_() {}

    ~CommandLogger() {}

    void StartRecovery(const size_t checkpoint_eid, const size_t persist_eid, const size_t recovery_thread_count);
    void WaitForRecovery();

    void StartLogging() {
      is_running_ = true;
      logger_thread_.reset(new std::thread(&CommandLogger::Run, this));
    }

    void StopLogging() {
      is_running_ = false;
      logger_thread_->join();
    }

    void RegisterWorker(WorkerContext *command_worker_ctx);
    void DeregisterWorker(WorkerContext *command_worker_ctx);

    size_t GetPersistEpochId() const {
      return persist_epoch_id_;
    }

private:
  void Run();

  void PersistEpochBegin(FileHandle &file_handle, const size_t epoch_id);
  void PersistEpochEnd(FileHandle &file_handle, const size_t epoch_id);
  void PersistLogBuffer(FileHandle &file_handle, std::unique_ptr<LogBuffer> log_buffer);

  std::string GetLogFileFullPath(size_t epoch_id) {
    return log_dir_ + "/" + logging_filename_prefix_ + "_" + std::to_string(logger_id_) + "_" + std::to_string(epoch_id);
  }

  void GetSortedLogFileIdList(const size_t checkpoint_eid, const size_t persist_eid);
  
  void RunRecoveryThread(const size_t thread_id, const size_t checkpoint_eid, const size_t persist_eid);

  bool ReplayLogFile(const size_t thread_id, FileHandle &file_handle, size_t checkpoint_eid, size_t pepoch_eid);

  virtual TransactionParameter* DeserializeParameter(UNUSED_ATTRIBUTE const int transaction_type, UNUSED_ATTRIBUTE CopySerializeInputBE &input) { return nullptr; }

  private:
    size_t logger_id_;
    std::string log_dir_;
    
    // recovery threads
    std::vector<std::unique_ptr<std::thread>> recovery_threads_;
    std::vector<size_t> file_eids_;
    std::atomic<int> max_replay_file_id_;
    
    /* Recovery */
    // TODO: Check if we can discard the recovery pool after the recovery is done. Since every thing is copied to the
    // tile group and tile group related pool
    std::vector<std::unique_ptr<VarlenPool>> recovery_pools_;
    
    // logger thread
    std::unique_ptr<std::thread> logger_thread_;
    volatile bool is_running_;

    /* File system related */
    CopySerializeOutput logger_output_buffer_;

    /* Log buffers */
    size_t persist_epoch_id_;

    // The spin lock to protect the worker map. We only update this map when creating/terminating a new worker
    Spinlock worker_map_lock_;
    // map from worker id to the worker's context.
    std::unordered_map<oid_t, std::shared_ptr<WorkerContext>> worker_map_;
  
    const std::string logging_filename_prefix_ = "log";

    const size_t sleep_period_us_ = 40000;

    const int new_file_interval_ = 2000; // 2000 milliseconds.

    std::vector<ParamWrapper> param_wrappers_[40];

    std::vector<ParamWrapper> ordered_param_wrappers_;

  };

}
}