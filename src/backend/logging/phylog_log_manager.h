//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// phylog_log_manager.h
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

#include "libcuckoo/cuckoohash_map.hh"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/epoch_manager.h"
#include "backend/logging/log_buffer.h"
#include "backend/logging/log_record.h"
#include "backend/logging/log_buffer_pool.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/worker_log_context.h"
#include "backend/logging/logger.h"
#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"


namespace peloton {
namespace logging {

/* Per worker thread local context */
extern thread_local WorkerLogContext* thread_local_log_worker_ctx;


/**
 * logging file name layout :
 * 
 * dir_name + "/" + prefix + "_" + epoch_id
 *
 *
 * logging file layout :
 *
 *  -----------------------------------------------------------------------------
 *  | txn_cid | database_id | table_id | operation_type | data | ... | txn_end_flag
 *  -----------------------------------------------------------------------------
 *
 * NOTE: this layout is designed for physiological logging.
 *
 * NOTE: tuple length can be obtained from the table schema.
 *
 */

class PhyLogLogManager : public LogManager {
  PhyLogLogManager(const PhyLogLogManager &) = delete;
  PhyLogLogManager &operator=(const PhyLogLogManager &) = delete;
  PhyLogLogManager(PhyLogLogManager &&) = delete;
  PhyLogLogManager &operator=(PhyLogLogManager &&) = delete;

protected:

  PhyLogLogManager(int thread_count)
    : LogManager(thread_count), log_worker_id_generator_(0),
      global_committed_eid_(INVALID_EPOCH_ID),
      loggers_() {
    for (int i = 0; i < thread_count; ++i) {
      loggers_.emplace_back(new Logger());
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
  virtual void StartLoggers() override ;
  virtual void StopLoggers() override ;

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
  // void UpdateGlobalCommittedEid(size_t committed_eid);
  void SyncEpochToFile(Logger *logger_ctx, size_t eid);

  /*
   * Log file layout:
   *  Header: 8 bytes, for integrity validation
   *  Body:  actual log records
   *  Tail:   8 bytes, for integrity validation
   */

private:
  std::atomic<oid_t> log_worker_id_generator_;

  size_t global_committed_eid_;

  std::vector<std::shared_ptr<Logger>> loggers_;
};

}
}