//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// command_log_manager.h
//
// Identification: src/backend/logging/command_log_manager.h
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
#include "backend/logging/logging_util.h"
#include "backend/logging/worker_context.h"
#include "backend/logging/command_logger.h"
#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"


namespace peloton {
namespace logging {

/**
 * logging file name layout :
 * 
 * dir_name + "/" + prefix + "_" + epoch_id
 *
 *
 * logging file layout :
 *
 *  -----------------------------------------------------------------------------
 *  | txn_cid | txn_type
 *  -----------------------------------------------------------------------------
 *
 * NOTE: this layout is designed for physiological delta logging.
 *
 * NOTE: tuple length can be obtained from the table schema.
 *
 */

/* Per worker thread local context */
extern thread_local WorkerContext* tl_command_worker_ctx;

class CommandLogManager : public LogManager {
  CommandLogManager(const CommandLogManager &) = delete;
  CommandLogManager &operator=(const CommandLogManager &) = delete;
  CommandLogManager(CommandLogManager &&) = delete;
  CommandLogManager &operator=(CommandLogManager &&) = delete;

protected:

  CommandLogManager()
    : worker_count_(0),
      is_running_(false) {}

public:
  static CommandLogManager &GetInstance() {
    static CommandLogManager log_manager;
    return log_manager;
  }
  virtual ~CommandLogManager() {}

  virtual void SetDirectories(const std::vector<std::string> &logging_dirs) override {
    if (logging_dirs.size() > 0) {
      pepoch_dir_ = logging_dirs.at(0);
    }
    // check the existence of logging directories.
    // if not exists, then create the directory.
    for (auto logging_dir : logging_dirs) {
      if (LoggingUtil::CheckDirectoryExistence(logging_dir.c_str()) == false) {
        LOG_INFO("Logging directory %s is not accessible or does not exist", logging_dir.c_str());
        bool res = LoggingUtil::CreateDirectory(logging_dir.c_str(), 0700);
        if (res == false) {
          LOG_ERROR("Cannot create directory: %s", logging_dir.c_str());
        }
      }
    }

    logger_count_ = logging_dirs.size();
    for (size_t i = 0; i < logger_count_; ++i) {
      loggers_.emplace_back(new CommandLogger(i, logging_dirs.at(i)));
    }
  }

  // Worker side logic
  virtual void RegisterWorker() override;
  virtual void DeregisterWorker() override;
  
  void PersistTxn(concurrency::Transaction *txn, const int transaction_type);
  void FinishPendingTxn();

  // Logger side logic
  virtual void DoRecovery(const size_t &begin_eid) override;
  virtual void StartLoggers() override;
  virtual void StopLoggers() override;


  void RunPepochLogger();

private:
  size_t RecoverPepoch();

  // Don't delete the returned pointer
  inline LogBuffer * RegisterNewBufferToEpoch(std::unique_ptr<LogBuffer> log_buffer_ptr) {
    LOG_TRACE("Worker %d Register buffer to epoch %d", (int) tl_command_worker_ctx->worker_id, (int) tl_command_worker_ctx->current_eid);
    PL_ASSERT(log_buffer_ptr && log_buffer_ptr->Empty());
    PL_ASSERT(tl_command_worker_ctx);
    size_t eid_idx = tl_command_worker_ctx->current_eid % concurrency::EpochManager::GetEpochQueueCapacity();
    tl_command_worker_ctx->per_epoch_buffer_ptrs[eid_idx].push(std::move(log_buffer_ptr));
    return tl_command_worker_ctx->per_epoch_buffer_ptrs[eid_idx].top().get();
  }


  inline size_t HashToLogger(oid_t worker_id) {
    return ((size_t) worker_id) % logger_count_;
  }

  void WriteRecordToBuffer(const int transaction_type);

private:
  std::atomic<oid_t> worker_count_;

  std::vector<std::shared_ptr<CommandLogger>> loggers_;

  std::unique_ptr<std::thread> pepoch_thread_;
  volatile bool is_running_;

  std::string pepoch_dir_;

  const std::string pepoch_filename_ = "pepoch";
};

}
}