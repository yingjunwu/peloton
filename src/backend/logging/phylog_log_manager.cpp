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

thread_local WorkerLogContext* thread_local_worker_log_ctx = nullptr;

// const uint64_t PhyLogLogManager::uint64_place_holder = 0;

// NOTE: this function is never used.
// void PhyLogLogManager::UpdateGlobalCommittedEid(size_t committed_eid) {
//   while(true) {
//     auto old = global_committed_eid_;
//     if(old > committed_eid) {
//       return;
//     }else if ( __sync_bool_compare_and_swap(&global_committed_eid_, old, committed_eid) ) {
//       return;
//     }
//   }
// }

// register worker threads to the log manager before execution.
// note that we always construct logger prior to worker.
// this function is called by each worker thread.
void PhyLogLogManager::RegisterWorkerToLogger() {
  PL_ASSERT(thread_local_worker_log_ctx == nullptr);
  // shuffle worker to logger
  thread_local_worker_log_ctx = new WorkerLogContext(worker_count_++);
  size_t logger_id = HashToLogger(thread_local_worker_log_ctx->worker_id);

  loggers_[logger_id]->RegisterWorker(thread_local_worker_log_ctx);
}

// deregister worker threads.
void PhyLogLogManager::DeregisterWorkerFromLogger() {
  PL_ASSERT(thread_local_worker_log_ctx != nullptr);
  thread_local_worker_log_ctx->terminated = true;
}

void PhyLogLogManager::WriteRecordToBuffer(LogRecord &record) {
  WorkerLogContext *ctx = thread_local_worker_log_ctx;
  LOG_TRACE("Worker %d write a record", ctx->worker_id);

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
    case LOGRECORD_TYPE_TUPLE_UPDATE: {
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
    }
    case LOGRECORD_TYPE_TRANSACTION_BEGIN:
    case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
      output.WriteLong(ctx->current_cid);
      break;
    }
    case LOGRECORD_TYPE_EPOCH_BEGIN:
    case LOGRECORD_TYPE_EPOCH_END: {
      output.WriteLong((uint64_t) ctx->current_eid);
      break;
    }
    default: {
      LOG_ERROR("Unsupported log record type");
      PL_ASSERT(false);
    }
  }

  // Copy the output buffer into current buffer
  PL_ASSERT(ctx->per_epoch_buffer_ptrs[ctx->current_eid].empty() == false);

  LogBuffer* buffer_ptr = ctx->per_epoch_buffer_ptrs[ctx->current_eid].top().get();
  PL_ASSERT(buffer_ptr);

  bool is_success = buffer_ptr->WriteData(output.Data(), output.Size());
  if (is_success == false) {
    // A buffer is full, pass it to the front end logger
    // Get a new buffer and register it to current epoch
    buffer_ptr = RegisterNewBufferToEpoch(std::move((ctx->buffer_pool.GetBuffer())));
    // Write it again
    is_success = buffer_ptr->WriteData(output.Data(), output.Size());
    PL_ASSERT(is_success);
  }
}

void PhyLogLogManager::StartTxn(concurrency::Transaction *txn) {
  PL_ASSERT(thread_local_worker_log_ctx);
  size_t txn_eid = txn->GetEpochId();

  // Handle the epoch id
  // TODO: what if there's no read-write transaction within a certain epoch?
  if (thread_local_worker_log_ctx->current_eid == INVALID_EPOCH_ID || thread_local_worker_log_ctx->current_eid != txn_eid) {
    // Get a new buffer
    PL_ASSERT(thread_local_worker_log_ctx->current_eid == INVALID_EPOCH_ID || thread_local_worker_log_ctx->current_eid < txn_eid);
    thread_local_worker_log_ctx->current_eid = txn_eid;
    RegisterNewBufferToEpoch(std::move(thread_local_worker_log_ctx->buffer_pool.GetBuffer()));
  }

  // Handle the commit id
  cid_t txn_cid = txn->GetBeginCommitId();
  thread_local_worker_log_ctx->current_cid = txn_cid;

  // Log down the begin of txn
  LogRecord record = LogRecordFactory::CreateTxnRecord(LOGRECORD_TYPE_TRANSACTION_BEGIN, txn_cid);
  WriteRecordToBuffer(record);
}

void PhyLogLogManager::CommitCurrentTxn() {
  PL_ASSERT(thread_local_worker_log_ctx);
  LogRecord record = LogRecordFactory::CreateTxnRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT, thread_local_worker_log_ctx->current_cid);
  WriteRecordToBuffer(record);
}


void PhyLogLogManager::LogInsert(const ItemPointer &tuple_pos) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LOGRECORD_TYPE_TUPLE_INSERT, tuple_pos);
  WriteRecordToBuffer(record);
}

void PhyLogLogManager::LogUpdate(const ItemPointer &tuple_pos) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE, tuple_pos);
  WriteRecordToBuffer(record);
}

void PhyLogLogManager::LogDelete(const ItemPointer &tuple_pos_deleted) {
  // Need the tuple value for the deleted tuple
  LogRecord record = LogRecordFactory::CreateTupleRecord(LOGRECORD_TYPE_TUPLE_DELETE, tuple_pos_deleted);
  WriteRecordToBuffer(record);
}

void PhyLogLogManager::StartLoggers() {
  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    LOG_TRACE("Start logger %d", (int) logger_id);
    loggers_[logger_id]->Start();
  }
}

void PhyLogLogManager::StopLoggers() {
  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    loggers_[logger_id]->Stop();
  }
}

}
}