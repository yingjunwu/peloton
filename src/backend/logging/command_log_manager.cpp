//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// command_log_manager.cpp
//
// Identification: src/backend/logging/command_log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "backend/concurrency/epoch_manager_factory.h"
#include "backend/logging/command_log_manager.h"
#include "backend/logging/durability_factory.h"
#include "backend/catalog/manager.h"
#include "backend/expression/container_tuple.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace logging {

// register worker threads to the log manager before execution.
// note that we always construct logger prior to worker.
// this function is called by each worker thread.
void CommandLogManager::RegisterWorker() {
  PL_ASSERT(tl_worker_ctx == nullptr);
  // shuffle worker to logger
  tl_worker_ctx = new WorkerContext(worker_count_++);
  size_t logger_id = HashToLogger(tl_worker_ctx->worker_id);

  loggers_[logger_id]->RegisterWorker(tl_worker_ctx);
}

// deregister worker threads.
void CommandLogManager::DeregisterWorker() {
  PL_ASSERT(tl_worker_ctx != nullptr);

  size_t logger_id = HashToLogger(tl_worker_ctx->worker_id);

  loggers_[logger_id]->DeregisterWorker(tl_worker_ctx);
}

void CommandLogManager::WriteRecordToBuffer(const int transaction_type) {
  WorkerContext *ctx = tl_worker_ctx;
  LOG_TRACE("Worker %d write a record", ctx->worker_id);

  PL_ASSERT(ctx);

  size_t epoch_idx = ctx->current_eid % concurrency::EpochManager::GetEpochQueueCapacity();

  size_t buffer_size = sizeof(ctx->current_cid) + sizeof(transaction_type);
  
  char *data_buffer = new char[buffer_size];
  
  memcpy(data_buffer, &(ctx->current_cid), sizeof(ctx->current_cid));
  memcpy(data_buffer + sizeof(ctx->current_cid), &(transaction_type), sizeof(transaction_type));

  PL_ASSERT(ctx->per_epoch_buffer_ptrs[epoch_idx].empty() == false);
  LogBuffer* buffer_ptr = ctx->per_epoch_buffer_ptrs[epoch_idx].top().get();
  PL_ASSERT(buffer_ptr);

  // Copy the output buffer into current buffer
  bool is_success = buffer_ptr->WriteData(data_buffer, buffer_size);
  if (is_success == false) {
    // A buffer is full, pass it to the front end logger
    // Get a new buffer and register it to current epoch
    buffer_ptr = RegisterNewBufferToEpoch(std::move((ctx->buffer_pool.GetBuffer())));
    // Write it again
    is_success = buffer_ptr->WriteData(data_buffer, buffer_size);
    PL_ASSERT(is_success);
  }

  delete[] data_buffer;
  data_buffer;
}

void CommandLogManager::WriteRecordToBuffer(LogRecord &record) {

}

void CommandLogManager::StartTxn(concurrency::Transaction *txn, const int transaction_type) {
  PL_ASSERT(tl_worker_ctx);
  size_t txn_eid = txn->GetEpochId();

  // Record the txn timer
  DurabilityFactory::StartTxnTimer(txn_eid, tl_worker_ctx);

  PL_ASSERT(tl_worker_ctx->current_eid == INVALID_EPOCH_ID || 
    tl_worker_ctx->current_eid <= txn_eid);

  // Handle the epoch id
  if (tl_worker_ctx->current_eid == INVALID_EPOCH_ID 
    || tl_worker_ctx->current_eid != txn_eid) {
    // if this is a new epoch, then write to a new buffer
    tl_worker_ctx->current_eid = txn_eid;
    RegisterNewBufferToEpoch(std::move(tl_worker_ctx->buffer_pool.GetBuffer()));
  }

  // Handle the commit id
  cid_t txn_cid = txn->GetEndCommitId();
  tl_worker_ctx->current_cid = txn_cid;

  WriteRecordToBuffer(transaction_type);
}

void CommandLogManager::LogInsert(const ItemPointer &tuple_pos) {}
void CommandLogManager::LogUpdate(const ItemPointer &tuple_pos) {}
void CommandLogManager::LogDelete(const ItemPointer &tuple_pos_deleted) {}
void CommandLogManager::CommitCurrentTxn() {}

void CommandLogManager::FinishPendingTxn() {
  PL_ASSERT(tl_worker_ctx);
  size_t glob_peid = global_persist_epoch_id_.load();
  DurabilityFactory::StopTimersByPepoch(glob_peid, tl_worker_ctx);
}


void CommandLogManager::DoRecovery(const size_t &begin_eid UNUSED_ATTRIBUTE){}

void CommandLogManager::StartLoggers() {
  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    LOG_TRACE("Start logger %d", (int) logger_id);
    loggers_[logger_id]->StartLogging();
  }
  is_running_ = true;
  pepoch_thread_.reset(new std::thread(&CommandLogManager::RunPepochLogger, this));
}

void CommandLogManager::StopLoggers() {
  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    loggers_[logger_id]->StopLogging();
  }
  is_running_ = false;
  pepoch_thread_->join();
}

void CommandLogManager::RunPepochLogger() {
  
  FileHandle file_handle;
  std::string filename = pepoch_dir_ + "/" + pepoch_filename_;
  // Create a new file
  if (LoggingUtil::OpenFile(filename.c_str(), "wb", file_handle) == false) {
    LOG_ERROR("Unable to create pepoch file %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }


  while (true) {
    if (is_running_ == false) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::microseconds
      (concurrency::EpochManagerFactory::GetInstance().GetEpochLengthInMicroSecQuarter())
    );
    
    size_t curr_persist_epoch_id = MAX_EPOCH_ID;
    for (auto &logger : loggers_) {
      size_t logger_pepoch_id = logger->GetPersistEpochId();
      if (logger_pepoch_id < curr_persist_epoch_id) {
        curr_persist_epoch_id = logger_pepoch_id;
      }
    }

    PL_ASSERT(curr_persist_epoch_id < MAX_EPOCH_ID);
    size_t glob_peid = global_persist_epoch_id_.load();
    if (curr_persist_epoch_id > glob_peid) {
      // we should post the pepoch id after the fsync -- Jiexi
      fwrite((const void *) (&curr_persist_epoch_id), sizeof(curr_persist_epoch_id), 1, file_handle.file);
      global_persist_epoch_id_ = curr_persist_epoch_id;
      // printf("global persist epoch id = %d\n", (int)global_persist_epoch_id_);
      // Call fsync
      LoggingUtil::FFlushFsync(file_handle);
    }
  }

  // Safely close the file
  bool res = LoggingUtil::CloseFile(file_handle);
  if (res == false) {
    LOG_ERROR("Cannot close pepoch file");
    exit(EXIT_FAILURE);
  }

}

size_t CommandLogManager::RecoverPepoch() {
  FileHandle file_handle;
  std::string filename = pepoch_dir_ + "/" + pepoch_filename_;
  // Create a new file
  if (LoggingUtil::OpenFile(filename.c_str(), "rb", file_handle) == false) {
    LOG_ERROR("Unable to open pepoch file %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }
  
  size_t persist_epoch_id = 0;

  while (true) {
    if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) &persist_epoch_id, sizeof(persist_epoch_id)) == false) {
      LOG_TRACE("Reach the end of the log file");
      break;
    }
    printf("persist_epoch_id = %d\n", (int)persist_epoch_id);
  }

  // Safely close the file
  bool res = LoggingUtil::CloseFile(file_handle);
  if (res == false) {
    LOG_ERROR("Cannot close pepoch file");
    exit(EXIT_FAILURE);
  }

  return persist_epoch_id;
}

}
}