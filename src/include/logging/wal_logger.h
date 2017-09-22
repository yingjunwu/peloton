//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// reordered_phylog_logger.h
//
// Identification: src/backend/logging/reordered_phylog_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <thread>
#include <list>
#include <stack>

#include "concurrency/transaction.h"
#include "concurrency/epoch_manager.h"
#include "logging/log_buffer.h"
#include "logging/log_record.h"
#include "logging/log_buffer_pool.h"
#include "logging/log_manager.h"
#include "type/types.h"
#include "type/serializer.h"
#include "container/lock_free_queue.h"
#include "common/logger.h"
#include "type/abstract_pool.h"


namespace peloton {

namespace storage {
class TileGroupHeader;
}

namespace logging {

class WalLogger {

public:
  WalLogger(const size_t &logger_id, const std::string &log_dir) :
    logger_id_(logger_id),
    log_dir_(log_dir),
    logger_thread_(nullptr),
    is_running_(false),
    logger_output_buffer_() {}

  ~WalLogger() {}

  void StartRecovery();
  void StartIndexRebulding();

  void WaitForRecovery();
  void WaitForIndexRebuilding();

  void StartLogging() {
    is_running_ = true;
   }

  void StopLogging() {
    is_running_ = false;
  }

  std::unique_ptr<LogBuffer> PersistLogBuffer(std::unique_ptr<LogBuffer> log_buffer);

private:
  void Run();

  std::string GetLogFileFullPath(size_t epoch_id) {
    return log_dir_ + "/" + logging_filename_prefix_ + "_" + std::to_string(logger_id_) + "_" + std::to_string(epoch_id);
  }

  void GetSortedLogFileIdList();

  void RunRecoveryThread();

  void RunSecIndexRebuildThread();

  void RebuildSecIndexForTable(storage::DataTable *table);

  bool ReplayLogFile(FileHandle &file_handle);

  bool InstallTupleRecord(LogRecordType type, storage::Tuple *tuple, storage::DataTable *table, cid_t cur_cid, ItemPointer location);

  // Return value is the swapped txn id, either INVALID_TXNID or INITIAL_TXNID
  txn_id_t LockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset);
  void UnlockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset, txn_id_t new_txn_id);

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
  std::vector<std::unique_ptr<type::AbstractPool>> recovery_pools_;

  // logger thread
  std::unique_ptr<std::thread> logger_thread_;
  volatile bool is_running_;

  /* File system related */
  CopySerializeOutput logger_output_buffer_;

  /* Log buffers */
  LogBuffer* log_buffer_;
  size_t persist_epoch_id_;

  // The spin lock to protect the worker map. We only update this map when creating/terminating a new worker
  //Spinlock worker_map_lock_;
  // map from worker id to the worker's context.
  //std::unordered_map<oid_t, std::shared_ptr<WorkerContext>> worker_map_;

  const std::string logging_filename_prefix_ = "log";

  const size_t sleep_period_us_ = 40000;

  const int new_file_interval_ = 500; // 500 milliseconds.

};


}
}