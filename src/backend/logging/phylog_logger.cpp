//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// phylog_logger.cpp
//
// Identification: src/backend/logging/phylog_logger.cpp
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

void PhyLogLogger::RegisterWorker(WorkerLogContext *worker_log_ctx) {
  worker_map_lock_.Lock();
  worker_map_[worker_log_ctx->worker_id].reset(worker_log_ctx);
  worker_map_lock_.Unlock();
}

void PhyLogLogger::Run() {
  
  // Get the file name
  // TODO: we just use the last file id. May be we can use some epoch id?
  // for now, let's assume that each logger uses a single file to record logs. --YINGJUN
  // SILO uses multiple files only to simplify the process of log truncation.
  std::string filename = GetLogFileFullPath(0);

  // Create a new file
  if (LoggingUtil::CreateFile(filename.c_str(), "wb", file_handle_) == false) {
    LOG_ERROR("Unable to create log file %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }

  // Init the header of the log file
  // for now, we do not need this... --YINGJUN
  // fwrite((void *)(&PhyLogLogger::uint64_place_holder), sizeof(PhyLogLogger::uint64_place_holder), 1, file_handle_.file);

  // Update the logger context
  file_handle_.size = 0;


  /**
   *  Main loop
   */ 
  // TODO: Once we have recovery, we should be able to set the begin epoch id for the epoch manager. Then the start epoch
  // id is not necessarily the START_EPOCH_ID. We should load it from the epoch manager.

  while (true) {
    if (is_running_ == false && worker_map_.empty()) {
      // TODO: Wait for all registered worker to terminate
      break;
    }

    // Pull log records from workers per epoch buffer
    {
      worker_map_lock_.Lock();

      if (is_running_ == false && worker_map_.empty()) {
        // TODO: Wait for all registered worker to terminate
        worker_map_lock_.Unlock();
        break;
      }

      size_t max_workers_persist_eid = INVALID_EPOCH_ID;


      auto worker_itr = worker_map_.begin();
      while (worker_itr != worker_map_.end()) {
        // For every alive worker, move its buffer to the logger's local buffer
        auto worker_ctx_ptr = worker_itr->second.get();

        size_t max_persist_eid = worker_ctx_ptr->current_eid - 1;
        size_t current_persist_eid = worker_ctx_ptr->persist_eid;

        PL_ASSERT(current_persist_eid <= max_persist_eid);

        if (current_persist_eid == max_persist_eid) {
          continue;
        }

        for (size_t epoch_id = current_persist_eid + 1; epoch_id <= max_persist_eid; ++epoch_id) {
          size_t epoch_idx = epoch_id % concurrency::EpochManager::GetEpochQueueCapacity();
          // get all the buffers that are associated with the epoch.
          auto &buffers = worker_ctx_ptr->per_epoch_buffer_ptrs[epoch_idx];

          if (buffers.empty() == true) {
            // no transaction log is generated within this epoch.
            // it's fine. simply ignore it.
            continue;
          }

          PersistEpochBegin(epoch_id);
          // persist all the buffers.
          while (buffers.empty() == false) {
            // Check if the buffer is empty
            // TODO: is it possible to have an empty buffer??? --YINGJUN
            if (buffers.top()->Empty()) {
              worker_ctx_ptr->buffer_pool.PutBuffer(std::move(buffers.top()));
            } else {
              PersistLogBuffer(std::move(buffers.top()));
            }
            PL_ASSERT(buffers.top() == nullptr);
            buffers.pop();
          }
          PersistEpochEnd(epoch_id);
          // Call fsync
          LoggingUtil::FFlushFsync(file_handle_);

        } // end for

        worker_ctx_ptr->persist_eid = max_persist_eid;

        if (max_workers_persist_eid == INVALID_EPOCH_ID || max_workers_persist_eid > max_persist_eid) {
          max_workers_persist_eid = max_persist_eid;
        }


        bool terminated = worker_ctx_ptr->terminated;

        if (terminated) {
          // Remove terminated workers
          worker_itr = worker_map_.erase(worker_itr);
        } else {
          worker_itr++;
        }

      } // end while

      PL_ASSERT(max_workers_persist_eid >= persist_epoch_id_);

      persist_epoch_id_ = max_workers_persist_eid;

      worker_map_lock_.Unlock();
    }
  }

  // Close the log file
  // TODO: Seek and write the integrity information in the header

  // Safely close the file
  bool res = LoggingUtil::CloseFile(file_handle_);
  if (res == false) {
    LOG_ERROR("Can not close log file under directory %s", log_dir_.c_str());
    exit(EXIT_FAILURE);
  }
}

void PhyLogLogger::PersistEpochBegin(const size_t epoch_id) {
  // Write down the epoch begin record  
  LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_BEGIN, epoch_id);

  logger_output_buffer_.Reset();
  record.Serialize(logger_output_buffer_);
  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle_.file);

}

void PhyLogLogger::PersistEpochEnd(const size_t epoch_id) {
  // Write down the epoch end record
  LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_END, epoch_id);

  logger_output_buffer_.Reset();
  record.Serialize(logger_output_buffer_);
  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle_.file);

}

void PhyLogLogger::PersistLogBuffer(std::unique_ptr<LogBuffer> log_buffer) {

  fwrite((const void *) (log_buffer->GetData()), log_buffer->GetSize(), 1, file_handle_.file);

  // Return the buffer to the worker
  log_buffer->Reset();
  {
    worker_map_lock_.Lock();
    auto itr = worker_map_.find(log_buffer->GetWorkerId());
    if (itr != worker_map_.end()) {
      // Release the buffer
      log_buffer.reset(nullptr);
    } else {
      // In this case, the worker is already terminated and removed
      itr->second->buffer_pool.PutBuffer(std::move(log_buffer));
    }
    worker_map_lock_.Unlock();
  }
}


}
}