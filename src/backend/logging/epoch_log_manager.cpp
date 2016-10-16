//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_log_manager.cpp
//
// Identification: src/backend/logging/epoch_log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include "backend/concurrency/epoch_manager_factory.h"
#include "backend/logging/epoch_log_manager.h"
#include "backend/logging/durability_factory.h"
#include "backend/catalog/manager.h"
#include "backend/expression/container_tuple.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace logging {

thread_local EpochWorkerContext* tl_epoch_worker_ctx = nullptr;

// register worker threads to the log manager before execution.
// note that we always construct logger prior to worker.
// this function is called by each worker thread.
void EpochLogManager::RegisterWorker() {
  PL_ASSERT(tl_epoch_worker_ctx == nullptr);
  // shuffle worker to logger
  tl_epoch_worker_ctx = new EpochWorkerContext(worker_count_++);
  size_t logger_id = HashToLogger(tl_epoch_worker_ctx->worker_id);

  loggers_[logger_id]->RegisterWorker(tl_epoch_worker_ctx);
}

// deregister worker threads.
void EpochLogManager::DeregisterWorker() {
  PL_ASSERT(tl_epoch_worker_ctx != nullptr);

  size_t logger_id = HashToLogger(tl_epoch_worker_ctx->worker_id);

  loggers_[logger_id]->DeregisterWorker(tl_epoch_worker_ctx);
}

void EpochLogManager::StartTxn(concurrency::Transaction *txn) {
  PL_ASSERT(tl_epoch_worker_ctx);
  size_t txn_eid = txn->GetEpochId();

  // Record the txn timer
  DurabilityFactory::StartTxnTimer(txn_eid, tl_epoch_worker_ctx);

  PL_ASSERT(tl_epoch_worker_ctx->current_eid == INVALID_EPOCH_ID || tl_epoch_worker_ctx->current_eid <= txn_eid);

  // Handle the epoch id
  if (tl_epoch_worker_ctx->current_eid == INVALID_EPOCH_ID 
    || tl_epoch_worker_ctx->current_eid != txn_eid) {
    // if this is a new epoch, then write to a new buffer

    if (tl_epoch_worker_ctx->current_eid != INVALID_EPOCH_ID) {
      
      RegisterNewBufferToEpoch(std::move(tl_epoch_worker_ctx->buffer_pool.GetBuffer(txn_eid)));
      
      
      auto &manager = catalog::Manager::GetInstance();    
      for (auto &entry : tl_epoch_worker_ctx->delta_snapshot) {
        ItemPointer persist_pos = entry.second.first;
        if (persist_pos.IsNull() == true) {
          // currently, we do not handle delete.
          continue;
        } else {

          auto tile_group = manager.GetTileGroup(persist_pos.block);
          
          expression::ContainerTuple<storage::TileGroup> container_tuple(tile_group.get(), persist_pos.offset);
    
          auto &output = tl_epoch_worker_ctx->output_buffer;

          output.Reset();

          container_tuple.SerializeTo(output);


          PL_ASSERT(tl_epoch_worker_ctx->per_epoch_buffer_ptrs[tl_epoch_worker_ctx->current_eid].empty() == false);
          LogBuffer* buffer_ptr = tl_epoch_worker_ctx->per_epoch_buffer_ptrs[tl_epoch_worker_ctx->current_eid].top().get();
          PL_ASSERT(buffer_ptr);

          // Copy the output buffer into current buffer
          bool is_success = buffer_ptr->WriteData(output.Data(), output.Size());
          if (is_success == false) {
            // A buffer is full, pass it to the front end logger
            // Get a new buffer and register it to current epoch
            buffer_ptr = RegisterNewBufferToEpoch(std::move((tl_epoch_worker_ctx->buffer_pool.GetBuffer(tl_epoch_worker_ctx->current_eid))));
            // Write it again
            is_success = buffer_ptr->WriteData(output.Data(), output.Size());
            PL_ASSERT(is_success);
          }
        }
      }

    }

    tl_epoch_worker_ctx->delta_snapshot.clear();
    tl_epoch_worker_ctx->current_eid = txn_eid;
    
  }

  // Handle the commit id
  cid_t txn_cid = txn->GetEndCommitId();
  tl_epoch_worker_ctx->current_cid = txn_cid;
}

void EpochLogManager::FinishPendingTxn() {
  PL_ASSERT(tl_epoch_worker_ctx);
  size_t glob_peid = global_persist_epoch_id_.load();
  DurabilityFactory::StopTimersByPepoch(glob_peid, tl_epoch_worker_ctx);
}

void EpochLogManager::LogInsert(ItemPointer *master_ptr, const ItemPointer &tuple_pos) {
  
  tl_epoch_worker_ctx->delta_snapshot[master_ptr] = std::make_pair(tuple_pos, tl_epoch_worker_ctx->current_cid);
}

void EpochLogManager::LogUpdate(ItemPointer *master_ptr, const ItemPointer &tuple_pos) {
  tl_epoch_worker_ctx->delta_snapshot[master_ptr] = std::make_pair(tuple_pos, tl_epoch_worker_ctx->current_cid);
}

void EpochLogManager::LogDelete(ItemPointer *master_ptr) {
  tl_epoch_worker_ctx->delta_snapshot[master_ptr] = std::make_pair(INVALID_ITEMPOINTER, tl_epoch_worker_ctx->current_cid);
}

void EpochLogManager::StartLoggers() {
  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    LOG_TRACE("Start logger %d", (int) logger_id);
    loggers_[logger_id]->StartLogging();
  }
  is_running_ = true;
  pepoch_thread_.reset(new std::thread(&EpochLogManager::RunPepochLogger, this));
}

void EpochLogManager::StopLoggers() {
  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    loggers_[logger_id]->StopLogging();
  }
  is_running_ = false;
  pepoch_thread_->join();
}


void EpochLogManager::RunPepochLogger() {
  
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
    
    size_t curr_persist_epoch_id = INVALID_EPOCH_ID;
    for (auto &logger : loggers_) {
      size_t logger_pepoch_id = logger->GetPersistEpochId();
      if (curr_persist_epoch_id == INVALID_EPOCH_ID || curr_persist_epoch_id > logger_pepoch_id) {
        curr_persist_epoch_id = logger_pepoch_id;
      }
    }
    size_t glob_peid = global_persist_epoch_id_.load();
    if (curr_persist_epoch_id > glob_peid) {
      // we should post the pepoch id after the fsync -- Jiexi
      fwrite((const void *) (&curr_persist_epoch_id), sizeof(curr_persist_epoch_id), 1, file_handle.file);
      global_persist_epoch_id_ = curr_persist_epoch_id;
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

}
}