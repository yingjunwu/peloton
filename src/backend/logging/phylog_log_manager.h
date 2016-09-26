//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// phylog_log_manager.cpp
//
// Identification: src/backend/logging/loggers/phylog_log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <thread>

#include "libcuckoo/cuckoohash_map.hh"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/epoch_manager.h"
#include "backend/logging/log_buffer.h"
#include "backend/logging/log_record.h"
#include "backend/logging/backend_buffer_pool.h"
#include "backend/logging/log_manager.h"
#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/common/lockfree_queue.h"

namespace peloton {
namespace logging {

class PhyLogLogManager : public LogManager {
  PhyLogLogManager(const PhyLogLogManager &) = delete;
  PhyLogLogManager &operator=(const PhyLogLogManager &) = delete;
  PhyLogLogManager(PhyLogLogManager &&) = delete;
  PhyLogLogManager &operator=(PhyLogLogManager &&) = delete;

  const static size_t sleep_period_us = 40000;
  const static std::string logger_dir_prefix = "phylog_logdir";
  const static std::string log_file_prefix = "phylog_log";

private:
  struct LoggerContext {
    size_t lid;
    std::unique_ptr<std::thread> logger_thread;

    /* File system related */
    std::string log_dir;
    size_t last_file_id;
    FileHandle cur_file_handle;

    /* Log buffers */
    size_t max_committed_eid;
    peloton::LockfreeQueue<std::unique_ptr<peloton::logging::LogBuffer>> buffer_queue;
  };

  struct LogWorkerContext {
    std::vector<std::unique_ptr<LogBuffer>> per_epoch_buffer_ptrs;
    BackendBufferPool buffer_pool;
    CopySerializeOutput output_buffer;

    size_t current_eid;
    cid_t current_cid;
    oid_t worker_id;

    bool terminated;

    LogWorkerContext(oid_t id)
      : per_epoch_buffer_ptrs(concurrency::EpochManager::GetEpochQueueCapacity()),
        buffer_pool(id), output_buffer(),
        current_eid(INVALID_EPOCH_ID), current_cid(INVALID_CID), worker_id(id), terminated(false)
    {}
    ~LogWorkerContext() {}
  };

  thread_local LogWorkerContext* log_worker_ctx = nullptr;

public:
  PhyLogLogManager(std::string &log_dir, int thread_count)
    : LogManager(log_dir), logger_thread_count_(thread_count), log_worker_id_generator_(0) {}
  virtual ~PhyLogLogManager() {}

  // Worker side logic
  virtual void CreateLogWorker() override ;
  virtual void TerminateLogWorker() override ;

  virtual void LogInsert(const ItemPointer &tuple_pos) override ;
  virtual void LogUpdate(const ItemPointer &tuple_pos) override ;
  virtual void LogDelete(const ItemPointer &tuple_pos_deleted) override ;
  virtual void StartTxn(concurrency::Transaction *txn) override ;
  virtual void CommitCurrentTxn() override ;

  // Logger side logic
  virtual void StartLogger() override ;
  virtual void StopLogger() override ;

private:

  inline void RegisterNewBufferToEpoch(std::unique_ptr<LogBuffer> log_buffer_ptr) {
    PL_ASSERT(log_buffer_ptr);
    PL_ASSERT(log_worker_ctx);
    log_worker_ctx->per_epoch_buffer_ptrs[log_worker_ctx->current_eid] = std::move(log_buffer_ptr);
  }

  void UpdateGlobalCommittedEid(size_t committed_eid);

  size_t HashToLogger(oid_t worker_id) {
    return ((size_t) worker_id) % logger_thread_count_;
  }

  void WriteRecord(LogRecord &record);

  void PassBufferToFrontend(LogWorkerContext *ctx);

  // Run logger thread
  void Run(size_t logger_id);

  void InitLoggerContext(size_t lid);

private:
  const int logger_thread_count_;
  std::atomic<oid_t> log_worker_id_generator_;

  volatile bool is_running_;
  size_t global_committed_eid_;

  // TODO: use unique ptr
  cuckoohash_map<oid_t, std::shared_ptr<LogWorkerContext>> worker_map_;

  std::vector<std::unique_ptr<LoggerContext>> logger_ctxs_;
};

}
}