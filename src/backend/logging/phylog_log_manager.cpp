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

#include "backend/logging/phylog_log_manager.h"
#include "backend/catalog/manager.h"
#include "backend/expression/container_tuple.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace logging {

void PhyLogLogManager::PassBufferToFrontend(LogWorkerContext *ctx) {
  size_t logger_id = HashToLogger(ctx->worker_id);
  logger_ctxs_[logger_id]->buffer_queue.Enqueue(std::move(ctx->per_epoch_buffer_ptrs[ctx->current_eid]));
}

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
  worker_map_.insert(log_worker_ctx->worker_id, log_worker_ctx);
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
  LogBuffer *current_buffer_ptr = ctx->per_epoch_buffer_ptrs[ctx->current_eid].get();
  PL_ASSERT(current_buffer_ptr);

  if (current_buffer_ptr->WriteData(output.Data(), output.Size())) {
    // A buffer is full, pass it to the front end logger
    PassBufferToFrontend(ctx);
    // Get a new buffer and register it to current epoch
    RegisterNewBufferToEpoch(std::move((ctx->buffer_pool.GetBuffer())));
    // Write it again
    bool res = ctx->per_epoch_buffer_ptrs[ctx->current_eid].get()->WriteData(output.Data(), output.Size());
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

void PhyLogLogManager::InitLoggerContext(size_t lid) {
  auto logger_ctx = logger_ctxs_[lid];

  // Init log directory
  logger_ctx->lid = lid;
  logger_ctx->log_dir = GetLogDirectoryName() + "/" + logger_dir_prefix + lid;

  bool res = LoggingUtil::CreateDirectory(logger_ctx->log_dir.c_str(), 0700);
  if (res == false) {
    LOG_ERROR("Failed to create logging directory %s", logger_ctx->log_dir.c_str());
    exit(EXIT_FAILURE);
  }

  // Init file list

}

void PhyLogLogManager::Run(size_t logger_id) {
  /* Init */


  /* Main loop  */
  while (true) {
    if (is_running_ == false) {
      return;
    }

    // TODO: calibrate the timer like siloR
    std::this_thread::sleep_for(std::chrono::microseconds(sleep_period_us));
  }

  /* Clean */

}

}
}