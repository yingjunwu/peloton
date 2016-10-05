//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_logger.cpp
//
// Identification: src/backend/logging/epoch_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <algorithm>
#include <dirent.h>
#include <cstdio>
#include <backend/gc/gc_manager_factory.h>
#include <backend/concurrency/transaction_manager_factory.h>

#include "backend/catalog/manager.h"
#include "backend/concurrency/epoch_manager_factory.h"
#include "backend/expression/container_tuple.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"

#include "backend/logging/epoch_logger.h"

namespace peloton {
namespace logging {

void EpochLogger::RegisterWorker(EpochWorkerContext *phylog_worker_ctx) {
  worker_map_lock_.Lock();
  worker_map_[phylog_worker_ctx->worker_id].reset(phylog_worker_ctx);
  worker_map_lock_.Unlock();
}

void EpochLogger::DeregisterWorker(EpochWorkerContext *phylog_worker_ctx) {
  worker_map_lock_.Lock();
  worker_map_.erase(phylog_worker_ctx->worker_id);
  worker_map_lock_.Unlock();
}

void EpochLogger::Run() {
  // TODO: Ensure that we have called run recovery before

  // Get the file name
  // for now, let's assume that each logger uses a single file to record logs. --YINGJUN
  // SILO uses multiple files only to simplify the process of log truncation.
  std::string filename = GetLogFileFullPath(next_file_id_);

  // Create a new file
  if (LoggingUtil::OpenFile(filename.c_str(), "wb", file_handle_) == false) {
    LOG_ERROR("Unable to create log file %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }

  /**
   *  Main loop
   */
  while (true) {
    if (is_running_ == false) { break; }

    std::this_thread::sleep_for(
       std::chrono::milliseconds(concurrency::EpochManagerFactory::GetInstance().GetEpochLengthInMiliSec() / 4));
    
    // Pull log records from workers per epoch buffer
    {
      worker_map_lock_.Lock();

      size_t max_workers_persist_eid = INVALID_EPOCH_ID;
      for (auto &worker_entry : worker_map_) {
        // For every alive worker, move its buffer to the logger's local buffer
        auto worker_ctx_ptr = worker_entry.second.get();

        size_t max_persist_eid = worker_ctx_ptr->current_eid - 1;
        size_t current_persist_eid = worker_ctx_ptr->persist_eid;

        PL_ASSERT(current_persist_eid <= max_persist_eid);

        if (current_persist_eid == max_persist_eid) {
          continue;
        }
        for (size_t epoch_id = current_persist_eid + 1; epoch_id <= max_persist_eid; ++epoch_id) {
          size_t epoch_idx = epoch_id % concurrency::EpochManager::GetEpochQueueCapacity();
          // get all the snapshot associated with the epoch.
          std::unique_ptr<DeltaSnapshot> snapshot(std::move(worker_ctx_ptr->per_epoch_snapshot_ptrs[epoch_idx]));

          // if (buffers.empty() == true) {
          //   // no transaction log is generated within this epoch.
          //   // it's fine. simply ignore it.
          //   continue;
          // }
          PersistEpochBegin(epoch_id);
          
          // auto itr = worker_map_.find(snapshot->worker_id_);
          // if (itr != worker_map_.end()) {
          //   // In this case, the worker is already terminated and removed
          //   // itr->second->snapshot_pool.PutSnapshot(std::move(snapshot));
          // } else {
          //   // Release the snapshot
          //   // snapshot.reset(nullptr);
          // }


          // // persist all the buffers.
          // while (buffers.empty() == false) {
          //   // Check if the buffer is empty
          //   // TODO: is it possible to have an empty buffer??? --YINGJUN
          //   if (buffers.top()->Empty()) {
          //     worker_ctx_ptr->buffer_pool.PutBuffer(std::move(buffers.top()));
          //   } else {
          //     PersistLogBuffer(std::move(buffers.top()));
          //   }
          //   PL_ASSERT(buffers.top() == nullptr);
          //   buffers.pop();
          // }

          PersistEpochEnd(epoch_id);
          // Call fsync
          LoggingUtil::FFlushFsync(file_handle_);
        } // end for

        worker_ctx_ptr->persist_eid = max_persist_eid;

        if (max_workers_persist_eid == INVALID_EPOCH_ID || max_workers_persist_eid > max_persist_eid) {
          max_workers_persist_eid = max_persist_eid;
        }

      } // end for

      if (max_workers_persist_eid == INVALID_EPOCH_ID) {
        // in this case, it is likely that there's no registered worker or there's nothing to persist.
        worker_map_lock_.Unlock();
        continue;
      }

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

void EpochLogger::PersistEpochBegin(const size_t epoch_id) {
  // Write down the epoch begin record  
  LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_BEGIN, epoch_id);

  logger_output_buffer_.Reset();
  record.Serialize(logger_output_buffer_);
  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle_.file);
}

void EpochLogger::PersistEpochEnd(const size_t epoch_id) {
  // Write down the epoch end record
  LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_END, epoch_id);

  logger_output_buffer_.Reset();
  record.Serialize(logger_output_buffer_);
  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle_.file);

}

void EpochLogger::PersistLogBuffer(std::unique_ptr<LogBuffer> log_buffer) {

  fwrite((const void *) (log_buffer->GetData()), log_buffer->GetSize(), 1, file_handle_.file);

}


}
}