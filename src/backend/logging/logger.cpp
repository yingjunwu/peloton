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

void Logger::RegisterWorker(WorkerLogContext *log_worker_ctx) {
  worker_map_lock_.Lock();
  worker_map_[log_worker_ctx->worker_id].reset(log_worker_ctx);
  worker_map_lock_.Unlock();
}

void Logger::Run() {
  
  // Get the file name
  // TODO: we just use the last file id. May be we can use some epoch id?
  // for now, let's assume that each logger uses a single file to record logs. --YINGJUN
  // SILO uses multiple files only to simplify the process of log truncation.
  // size_t file_id = next_file_id;
  // next_file_id++;
  std::string filename = GetLogFileFullPath(0);

  // Create a new file
  if (LoggingUtil::CreateFile(filename.c_str(), "wb", file_handle_) == false) {
    LOG_ERROR("Unable to create log file %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }

  // Init the header of the log file
  // for now, we do not need this... --YINGJUN
  // fwrite((void *)(&Logger::uint64_place_holder), sizeof(Logger::uint64_place_holder), 1, file_handle_.file);

  // Update the logger context
  file_handle_.size = 0;


  /**
   *  Main loop
   */ 
  // TODO: Once we have recovery, we should be able to set the begin epoch id for the epoch manager. Then the start epoch
  // id is not necessarily the START_EPOCH_ID. We should load it from the epoch manager.

  // TODO: Another option is, instead of the logger checking the epoch id, the epoch manager can push the epoch id of
  // dead epochs to the logger
  size_t last_epoch_id = START_EPOCH_ID;

  while (true) {
    if (is_running_ == false && worker_map_.empty()) {
      // TODO: Wait for all registered worker to terminate
      break;
    }

    size_t current_epoch_id = concurrency::EpochManagerFactory::GetInstance().GetMaxDeadEid();
    // Pull log records from workers per epoch buffer
    {
      worker_map_lock_.Lock();

      if (is_running_ == false && worker_map_.empty()) {
        // TODO: Wait for all registered worker to terminate
        worker_map_lock_.Unlock();
        break;
      }

      for (size_t epoch_id = last_epoch_id + 1; epoch_id <= current_epoch_id; ++epoch_id) {
        LOG_TRACE("Logger %d collecting buffers for epoch %d", (int) logger_id_, (int) epoch_id);
        // For every dead epoch, check the local buffer of all workers
        size_t epoch_idx = epoch_id % concurrency::EpochManager::GetEpochQueueCapacity();

        auto worker_itr = worker_map_.begin();
        while (worker_itr != worker_map_.end()) {
          // For every alive worker, move its buffer to the logger's local buffer
          auto worker_ctx_ptr = worker_itr->second.get();
          auto &buffers = worker_ctx_ptr->per_epoch_buffer_ptrs[epoch_idx];

          // NOTE: load the terminated flag before we start checking log buffers
          bool terminated = worker_ctx_ptr->terminated;

          COMPILER_MEMORY_FENCE;

          // Move to local queue
          while (buffers.empty() == false) {
            // Check the worker's local buffer for the entire epoch
            // Check if the buffer is empty
            if (buffers.top()->Empty()) {
              // Return the buffer to the worker immediately
              worker_ctx_ptr->buffer_pool.PutBuffer(std::move(buffers.top()));
            } else {
              // Move the buffer into the local buffer queue
              local_buffer_map_[epoch_idx].emplace(std::move(buffers.top()));
            }
            PL_ASSERT(buffers.top() == nullptr);
            buffers.pop();
          }

          COMPILER_MEMORY_FENCE;

          if (terminated) {
            // Remove terminated workers
            worker_itr = worker_map_.erase(worker_itr);
          } else {
            worker_itr++;
          }
        } // end while
      } // end for

      worker_map_lock_.Unlock();
    }

    // Log down all possible epochs
    // TODO: We just log down all buffers without any throttling
    for (size_t epoch_id = last_epoch_id + 1; epoch_id <= current_epoch_id; ++epoch_id) {
      SyncEpochToFile(epoch_id);
      PL_ASSERT(max_committed_epoch_id_ < epoch_id);
      max_committed_epoch_id_ = epoch_id;

      // TODO: Update the global committed epoch_id

      // TODO: Notify pending transactions to commit
    }

    // Wait for next round
    last_epoch_id = current_epoch_id;
    // TODO: calibrate the timer like siloR
    std::this_thread::sleep_for(std::chrono::microseconds(sleep_period_us_));
  }

  /**
   *  Clean the logger before termination
   */

  // Close the log file
  // TODO: Seek and write the integrity information in the header

  // Safely close the file
  bool res = LoggingUtil::CloseFile(file_handle_);
  if (res == false) {
    LOG_ERROR("Can not close log file under directory %s", log_dir_.c_str());
    exit(EXIT_FAILURE);
  }
}



void Logger::SyncEpochToFile(size_t epoch_id) {
  // TODO: Check the return value of FS operations
  size_t epoch_idx = epoch_id % concurrency::EpochManager::GetEpochQueueCapacity();

  // Write nothing for empty epochs
  auto &buffers = local_buffer_map_[epoch_idx];

  if (buffers.empty() == false) {
    // Write down the epoch begin record
    LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_BEGIN, epoch_id);
    record.Serialize(logger_output_buffer_);
    fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1,
           file_handle_.file);
    logger_output_buffer_.Reset();

    // Write every log buffer
    while (buffers.empty() == false) {
      // Write down the buffer
      LogBuffer *buffer_ptr = buffers.top().get();
      LOG_TRACE("Logger %d flush log buffer of epoch %d from worker %d", (int) logger_id_, (int) epoch_id, (int) buffer_ptr->GetWorkerId());

      fwrite((const void *) (buffer_ptr->GetData()), buffer_ptr->GetSize(), 1, file_handle_.file);

      // Return the buffer to the worker
      buffer_ptr->Reset();
      {
        worker_map_lock_.Lock();
        auto itr = worker_map_.find(buffer_ptr->GetWorkerId());
        if (itr != worker_map_.end()) {
          // In this case, the worker is already terminated and removed
          itr->second->buffer_pool.PutBuffer(std::move(buffers.top()));
        } else {
          // Release the buffer
          buffers.top().reset(nullptr);
        }
        worker_map_lock_.Unlock();
      }
      PL_ASSERT(buffers.top() == nullptr);
      buffers.pop();
    }

    // Write down the epoch end record
    record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_END, epoch_id);
    record.Serialize(logger_output_buffer_);
    fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1,
           file_handle_.file);
    logger_output_buffer_.Reset();

    // Call fsync
    LoggingUtil::FFlushFsync(file_handle_);
  }
}


}
}