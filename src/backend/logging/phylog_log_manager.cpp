//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// phylog_log_manager.cpp
//
// Identification: src/backend/logging/loggers/phylog_log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <backend/concurrency/epoch_manager_factory.h>

#include "backend/logging/phylog_log_manager.h"
#include "backend/catalog/manager.h"
#include "backend/expression/container_tuple.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace logging {


void PhyLogLogManager::UpdateGlobalCommittedEid(size_t committed_eid) {
  while(true) {
    auto old = global_committed_eid_;
    if(old > committed_eid) {
      return;
    }else if ( __sync_bool_compare_and_swap(&global_committed_eid_, old, committed_eid) ) {
      return;
    }
  }
}

void PhyLogLogManager::CreateLogWorker() {
  PL_ASSERT(log_worker_ctx == nullptr);
  log_worker_ctx = new LogWorkerContext(log_worker_id_generator_++);
  size_t logger_id = HashToLogger(log_worker_ctx->worker_id);

  auto logger_ctx_ptr = logger_ctxs_[logger_id].get();
  PL_ASSERT(logger_ctx_ptr != nullptr);

  {
    logger_ctx_ptr->worker_map_lock_.Lock();
    logger_ctx_ptr->worker_map_.emplace(log_worker_ctx->worker_id, log_worker_ctx);
    logger_ctx_ptr->worker_map_lock_.Unlock();
  }
}

void PhyLogLogManager::TerminateLogWorker() {
  PL_ASSERT(log_worker_ctx != nullptr);
  log_worker_ctx->terminated = true;
}

void PhyLogLogManager::WriteRecord(LogRecord &record) {
  LogWorkerContext *ctx = log_worker_ctx;
  PL_ASSERT(ctx);

  // First serialize the epoch to current output buffer
  // TODO: Eliminate this extra copy
  auto &output = ctx->output_buffer;

  // Reset the output buffer
  output.Reset();

  LogRecordType type = record.GetType();
  output.WriteEnumInSingleByte(type);

  switch (type) {
    case LOGRECORD_TYPE_TUPLE_INSERT:
    case LOGRECORD_TYPE_TUPLE_DELETE:
    case LOGRECORD_TYPE_TUPLE_UPDATE:
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();

      // Write down the database it and the table id
      output.WriteLong(tg->GetDatabaseId());
      output.WriteLong(tg->GetTableId());

      // Write the full tuple into the buffer
      expression::ContainerTuple<storage::TileGroup> container_tuple(
        tg, tuple_pos.offset
      );
      container_tuple.SerializeTo(output);
      break;
    case LOGRECORD_TYPE_TRANSACTION_BEGIN:
    case LOGRECORD_TYPE_TRANSACTION_COMMIT:
      output.WriteLong(ctx->current_cid);
      break;
    case LOGRECORD_TYPE_EPOCH_BEGIN:
    case LOGRECORD_TYPE_EPOCH_END:
      output.WriteLong((uint64_t) ctx->current_eid);
      break;
    default:
      LOG_ERROR("Unsupported log record type");
      PL_ASSERT(false);
  }

  // Copy the output buffer into current buffer
  // TODO: figure out how to reference a unique pointer to reduce lookup
  PL_ASSERT(ctx->per_epoch_buffer_ptrs[ctx->current_eid].empty() == false
            &&  ctx->per_epoch_buffer_ptrs[ctx->current_eid].top());

  if ( ctx->per_epoch_buffer_ptrs[ctx->current_eid].top()->WriteData(output.Data(), output.Size())) {
    // A buffer is full, pass it to the front end logger
    // Get a new buffer and register it to current epoch
    RegisterNewBufferToEpoch(std::move((ctx->buffer_pool.GetBuffer())));
    // Write it again
    bool res = ctx->per_epoch_buffer_ptrs[ctx->current_eid].top()->WriteData(output.Data(), output.Size());
    PL_ASSERT(res);
  }
}

void PhyLogLogManager::StartTxn(concurrency::Transaction *txn) {
  PL_ASSERT(log_worker_ctx);
  size_t txn_eid = txn->GetEpochId();

  // Handle the epoch id
  if (log_worker_ctx->current_eid == INVALID_EPOCH_ID || log_worker_ctx->current_eid != txn_eid) {
    // Get a new buffer
    PL_ASSERT(log_worker_ctx->current_eid == INVALID_EPOCH_ID || log_worker_ctx->current_eid < txn_eid);
    log_worker_ctx->current_eid = txn_eid;
    RegisterNewBufferToEpoch(std::move(log_worker_ctx->buffer_pool.GetBuffer()));
  }

  // Handle the commit id
  cid_t txn_cid = txn->GetBeginCommitId();
  log_worker_ctx->current_cid = txn_cid;

  // Log down the begin of txn
  LogRecord record = LogRecordFactory::CreateTxnRecord(LOGRECORD_TYPE_EPOCH_BEGIN, txn_cid);
  WriteRecord(record);
}

void PhyLogLogManager::CommitCurrentTxn() {
  PL_ASSERT(log_worker_ctx);
  LogRecord record = LogRecordFactory::CreateTxnRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT, log_worker_ctx->current_cid);
  WriteRecord(record);
}


void PhyLogLogManager::LogInsert(const ItemPointer &tuple_pos) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LOGRECORD_TYPE_TUPLE_INSERT, tuple_pos);
  WriteRecord(record);
}

void PhyLogLogManager::LogUpdate(const ItemPointer &tuple_pos) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE, tuple_pos);
  WriteRecord(record);
}

void PhyLogLogManager::LogDelete(const ItemPointer &tuple_pos_deleted) {
  // Need the tuple value for the deleted tuple
  LogRecord record = LogRecordFactory::CreateTupleRecord(LOGRECORD_TYPE_TUPLE_DELETE, tuple_pos_deleted);
  WriteRecord(record);
}

void PhyLogLogManager::StartLogger() {
  is_running_ = true;

  if (LoggingUtil::CheckDirectoryExistence(GetLogDirectoryName().c_str()) == false) {
    LOG_ERROR("Logging directory %s is not accessible or does not exist\n", GetLogDirectoryName().c_str());
  }

  for (size_t lid = 0; lid < logger_thread_count_; ++lid) {
    logger_ctxs_[lid]->logger_thread.reset(new std::thread(&PhyLogLogManager::Run, this, lid));
  }
}

void PhyLogLogManager::StopLogger() {
  is_running_ = false;
  for (size_t lid = 0; lid < logger_thread_count_; ++lid) {
    logger_ctxs_[lid]->logger_thread->join();
  }
}

void PhyLogLogManager::CreateAndInitLogFile(PhyLogLogManager::LoggerContext *logger_ctx_ptr) {
  // Get the file name
  // TODO: we just use the last file id. May be we can use some epoch id?
  std::string filename =
    logger_ctx_ptr->log_dir + "/" + log_file_prefix + "_" + ((logger_ctx_ptr->next_file_id)++) + log_file_surfix;

  // Create a new file
  if (LoggingUtil::CreateFile(filename.c_str(), "wb", logger_ctx_ptr->cur_file_handle) == false) {
    LOG_ERROR("Unable to create log file %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }

  // Init the header of the log file
  fwrite((void *)(&uint64_place_holder), sizeof(uint64_place_holder), 1, logger_ctx_ptr->cur_file_handle.file);

  // Update the logger context
  logger_ctx_ptr->cur_file_handle.size = 0;
}

void PhyLogLogManager::CloseCurrentLogFile(LoggerContext *logger_ctx_ptr) {
  // Seek and write the integrity information inthe header

  // Safely close the file

  // Reset the logger context
}

void PhyLogLogManager::InitLoggerContext(size_t logger_id) {
  // Init log directory
  auto logger_ctx_ptr = logger_ctxs_[logger_id].get();
  logger_ctx_ptr->lid = logger_id;
  logger_ctx_ptr->log_dir = GetLogDirectoryName() + "/" + logger_dir_prefix + "_" + logger_id;

  bool res = LoggingUtil::CreateDirectory(logger_ctx_ptr->log_dir.c_str(), 0700);
  if (res == false) {
    LOG_ERROR("Failed to create logging directory %s", logger_ctx_ptr->log_dir.c_str());
    exit(EXIT_FAILURE);
  }

  // Init file list for recovery and figure out the next log file id of this logger
  // TODO: Figure out how to assign logger directory before recovery

  // Create a new log file
  CreateAndInitLogFile(logger_ctx_ptr);
}

void PhyLogLogManager::Run(size_t logger_id) {
  /**
   * Init the logger
   */
  InitLoggerContext(logger_id);
  auto logger_ctx_ptr = logger_ctxs_[logger_id].get();

  /**
   *  Main loop
   */
  // TODO: Once we have recovery, we should be able to set the begin epoch id for the epoch manager. Then the start epoch
  // id is not neccessary the START_EPOCH_ID. We should load it from the epoch manager.

  // TODO: Another option is, instead of the logger checking the epoch id, the epoch manager can push the epoch id of
  // dead epochs to the logger
  size_t last_epoch_id = START_EPOCH_ID;

  while (true) {
    if (is_running_ == false) {
      return;
    }

    size_t current_epoch_id = concurrency::EpochManagerFactory::GetInstance().GetMaxDeadEid();
    // Pull log records from workers per epoch buffer
    {
      logger_ctx_ptr->worker_map_lock_.Lock();

      for (size_t eid = last_epoch_id + 1; eid <= current_epoch_id; ++eid) {
          size_t epoch_idx = eid % concurrency::EpochManager::GetEpochQueueCapacity();

          for (auto wp : logger_ctx_ptr->worker_map_) {
            auto worker_ctx_ptr = wp.second.get();
            auto &buffers = worker_ctx_ptr->per_epoch_buffer_ptrs[epoch_idx];

            while (buffers.empty() == false) {
              // Check if the buffer is empty
              if (buffers.top()->Empty()) {
                // Return the buffer to the worker immediately
                worker_ctx_ptr->buffer_pool.PutBuffer(std::move(buffers.top()));
                // Insert an empty place holder into the local buffer queue, for maintance of the max committed epoch id
                logger_ctx_ptr->local_buffer_queue.emplace_back(eid, nullptr);
              } else {
                // Move the buffer into the local buffer queue
                logger_ctx_ptr->local_buffer_queue.emplace_back(
                  eid, std::move(buffers.top()));
              }
              buffers.pop();
            }
          }
      }

      logger_ctx_ptr->worker_map_lock_.Unlock();
    }

    // Log down all possible epochs

    // Fsync and post the max committed epoch id

    // Wait for next round
    last_epoch_id = current_epoch_id;
    // TODO: calibrate the timer like siloR
    std::this_thread::sleep_for(std::chrono::microseconds(sleep_period_us));
  }

  /**
   *  Clean the logger before termination
   */

  // Clean the log record in current queue

  // Close the log file
  CloseCurrentLogFile(logger_ctx_ptr);
}

}
}