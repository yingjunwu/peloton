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

  struct LogWorkerContext {
    // Every epoch has a buffer stack
    std::vector<std::stack<std::unique_ptr<LogBuffer>>> per_epoch_buffer_ptrs;
    LogBufferPool buffer_pool;
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
    size_t lid;
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
    std::unordered_map<oid_t, std::shared_ptr<LogWorkerContext>> worker_map_;
    std::vector<std::stack<std::unique_ptr<peloton::logging::LogBuffer>>> local_buffer_map;

    LoggerContext() :
      lid(INVALID_LOGGERID), logger_thread(nullptr), log_dir(), next_file_id(0), logger_output_buffer(),
      cur_file_handle(), max_committed_eid(INVALID_EPOCH_ID), worker_map_lock_(), worker_map_(),
      local_buffer_map(concurrency::EpochManager::GetEpochQueueCapacity())
    {}

    ~LoggerContext() {}
  };

  /* Per worker thread local context */
  extern thread_local LogWorkerContext* thread_local_log_worker_ctx;



class PhyLogLogManager : public LogManager {
  PhyLogLogManager(const PhyLogLogManager &) = delete;
  PhyLogLogManager &operator=(const PhyLogLogManager &) = delete;
  PhyLogLogManager(PhyLogLogManager &&) = delete;
  PhyLogLogManager &operator=(PhyLogLogManager &&) = delete;

protected:

  PhyLogLogManager(int thread_count)
    : LogManager(thread_count), log_worker_id_generator_(0),
      global_committed_eid_(INVALID_EPOCH_ID),
      logger_ctxs_() {
    for (int i = 0; i < thread_count; ++i) {
      logger_ctxs_.emplace_back(new LoggerContext());
    }
  }

public:
  static PhyLogLogManager &GetInstance(int thread_count) {
    static PhyLogLogManager log_manager(thread_count);
    return log_manager;
  }
  virtual ~PhyLogLogManager() {}

  // Worker side logic
  virtual void RegisterWorkerToLogger() override ;
  virtual void DeregisterWorkerFromLogger() override ;

  virtual void LogInsert(const ItemPointer &tuple_pos) override ;
  virtual void LogUpdate(const ItemPointer &tuple_pos) override ;
  virtual void LogDelete(const ItemPointer &tuple_pos_deleted) override ;
  virtual void StartTxn(concurrency::Transaction *txn) override ;
  virtual void CommitCurrentTxn() override ;

  // Logger side logic
  virtual void StartLogger() override ;
  virtual void StopLogger() override ;

  // TODO: See if we can move some of this to the base class
  static const size_t sleep_period_us;
  static const uint64_t uint64_place_holder;

private:

   /*
    *   Worker utils
    */

  // Don't delete the returned pointer
  inline LogBuffer * RegisterNewBufferToEpoch(std::unique_ptr<LogBuffer> log_buffer_ptr) {
    LOG_TRACE("Worker %d Register buffer to epoch %d", (int) thread_local_log_worker_ctx->worker_id, (int) thread_local_log_worker_ctx->current_eid);
    PL_ASSERT(log_buffer_ptr && log_buffer_ptr->Empty());
    PL_ASSERT(thread_local_log_worker_ctx);
    thread_local_log_worker_ctx->per_epoch_buffer_ptrs[thread_local_log_worker_ctx->current_eid].push(std::move(log_buffer_ptr));
    return thread_local_log_worker_ctx->per_epoch_buffer_ptrs[thread_local_log_worker_ctx->current_eid].top().get();
  }


  inline size_t HashToLogger(oid_t worker_id) {
    return ((size_t) worker_id) % logging_thread_count_;
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
    size_t file_id = logger_ctx->next_file_id;
    logger_ctx->next_file_id++;
    return GetLogFileFullPath(logger_ctx->lid, file_id);
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
  std::atomic<oid_t> log_worker_id_generator_;

  size_t global_committed_eid_;

  std::vector<std::shared_ptr<LoggerContext>> logger_ctxs_;
};

}
}