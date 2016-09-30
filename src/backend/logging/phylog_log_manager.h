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
#include <list>
#include <stack>

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

namespace peloton {
namespace logging {


class PhyLogLogManager : public LogManager {
  PhyLogLogManager(const PhyLogLogManager &) = delete;
  PhyLogLogManager &operator=(const PhyLogLogManager &) = delete;
  PhyLogLogManager(PhyLogLogManager &&) = delete;
  PhyLogLogManager &operator=(PhyLogLogManager &&) = delete;

  // TODO: See if we can move some of this to the base class
  const static size_t sleep_period_us = 40000;
  const static std::string logger_dir_prefix = "phylog_logdir";
  const static std::string log_file_prefix = "phylog_log";
  const static std::string log_file_surfix = ".log";

  const static uint64_t uint64_place_holder = 0;

protected:
  struct LoggerContext {
    size_t lid;
    std::unique_ptr<std::thread> logger_thread;

    /* File system related */
    std::string log_dir;
    size_t next_file_id;
    CopySerializeOutput output_buffer;
    FileHandle cur_file_handle;

    /* Log buffers */
    size_t max_committed_eid;

    // The spin lock to protect the worker map. We only update this map when creating/terminating a new worker
    Spinlock worker_map_lock_;
    std::unordered_map<oid_t, std::shared_ptr<LogWorkerContext>> worker_map_;
    std::vector<std::stack<std::unique_ptr<peloton::logging::LogBuffer>>> local_buffer_map;

    LoggerContext() :
      lid(INVALID_LOGGERID), logger_thread(nullptr), log_dir(), next_file_id(0), output_buffer(),
      cur_file_handle(), max_committed_eid(INVALID_EPOCH_ID), worker_map_lock_(), worker_map_(),
      local_buffer_map(concurrency::EpochManager::GetEpochQueueCapacity())
    {}

    ~LoggerContext() {}
  };

  struct LogWorkerContext {
    // Every epoch has a buffer stack
    std::vector<std::stack<std::unique_ptr<LogBuffer>>> per_epoch_buffer_ptrs;
    LogBufferPool buffer_pool;
    CopySerializeOutput output_buffer;

    size_t current_eid;
    cid_t current_cid;
    oid_t worker_id;

    // When a worker terminate, we can not destruct it immediately.
    // What we do is first set this flag and the logger will check if it can destruct a terminated worker
    // TODO: Find out some where to call termination to a worker
    bool terminated;
    bool cleaned;

    LogWorkerContext(oid_t id)
      : per_epoch_buffer_ptrs(concurrency::EpochManager::GetEpochQueueCapacity()),
        buffer_pool(id), output_buffer(),
        current_eid(INVALID_EPOCH_ID), current_cid(INVALID_CID), worker_id(id), terminated(false), cleaned(false)
    {}
    ~LogWorkerContext() {
      PL_ASSERT(cleaned == true);
    }
  };

  /* Per worker thread local context */
  thread_local LogWorkerContext* log_worker_ctx = nullptr;

  PhyLogLogManager(const std::string &log_dir, int thread_count)
    : LogManager(log_dir), logger_thread_count_(thread_count), log_worker_id_generator_(0),
      is_running_(false), global_committed_eid_(INVALID_EPOCH_ID),
      logger_ctxs_(thread_count) {}

public:
  static PhyLogLogManager &GetInstance(const std::string &log_dir, int thread_count) {
    static PhyLogLogManager logManager(log_dir, thread_count);
    return logManager;
  }
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

   /*
    *   Worker utils
    */

  // Don't delete the returned pointer
  inline LogBuffer * RegisterNewBufferToEpoch(std::unique_ptr<LogBuffer> log_buffer_ptr) {
    PL_ASSERT(log_buffer_ptr && log_buffer_ptr->Empty());
    PL_ASSERT(log_worker_ctx);
    log_worker_ctx->per_epoch_buffer_ptrs[log_worker_ctx->current_eid].push(std::move(log_buffer_ptr));
    return log_worker_ctx->per_epoch_buffer_ptrs[log_worker_ctx->current_eid].top().get();
  }


  inline size_t HashToLogger(oid_t worker_id) {
    return ((size_t) worker_id) % logger_thread_count_;
  }

  void WriteRecordToBuffer(LogRecord &record);

  /*
   *    Logger utils
   */
  void Run(size_t logger_id);
  void UpdateGlobalCommittedEid(size_t committed_eid);
  void SyncEpochToFile(LoggerContext *logger_ctx, size_t eid);
  void InitLoggerContext(size_t lid);
  inline std::string GetNextLogFileName(LoggerContext *logger_ctx) {
    // Example: /tmp/phylog_log_0.log
    return logger_ctx->log_dir + "/" +
           log_file_prefix + "_" + ((logger_ctx->next_file_id)++) + log_file_surfix;
  }
  /*
   * Log file layout:
   *  Header: 8 bytes, for integrity validation
   *  Body:  actual log records
   *  Tail:   8 bytes, for integrity validation
   */
  void CreateAndInitLogFile(LoggerContext *logger_ctx_ptr);
  void CloseCurrentLogFile(LoggerContext *logger_ctx_ptr);

private:
  const int logger_thread_count_;
  std::atomic<oid_t> log_worker_id_generator_;

  volatile bool is_running_;
  size_t global_committed_eid_;

  std::vector<std::shared_ptr<LoggerContext>> logger_ctxs_;
};

}
}