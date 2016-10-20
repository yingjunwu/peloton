//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// command_logger.cpp
//
// Identification: src/backend/logging/command_logger.cpp
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

#include "backend/logging/command_logger.h"

namespace peloton {
namespace logging {

void CommandLogger::RegisterWorker(WorkerContext *command_worker_ctx) {
  worker_map_lock_.Lock();
  worker_map_[command_worker_ctx->worker_id].reset(command_worker_ctx);
  worker_map_lock_.Unlock();
}

void CommandLogger::DeregisterWorker(WorkerContext *command_worker_ctx) {
  worker_map_lock_.Lock();
  worker_map_.erase(command_worker_ctx->worker_id);
  worker_map_lock_.Unlock();
}


void CommandLogger::Run() {
  // TODO: Ensure that we have called run recovery before

  concurrency::EpochManager &epoch_manager = concurrency::EpochManagerFactory::GetInstance();

  size_t file_epoch_count = (size_t)(new_file_interval_ / epoch_manager.GetEpochDurationMilliSecond());

  std::list<std::pair<FileHandle*, size_t>> file_handles;

  size_t current_file_eid = 0; //epoch_manager.GetCurrentEpochId();

  FileHandle *new_file_handle = new FileHandle();
  file_handles.push_back(std::make_pair(new_file_handle, current_file_eid));
  
  std::string filename = GetLogFileFullPath(current_file_eid);
  // Create a new file
  if (LoggingUtil::OpenFile(filename.c_str(), "wb", *new_file_handle) == false) {
    LOG_ERROR("Unable to create log file %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }

  /**
   *  Main loop
   */
  while (true) {
    if (is_running_ == false) { break; }

    std::this_thread::sleep_for(
       std::chrono::microseconds(epoch_manager.GetEpochLengthInMicroSecQuarter()));

    // Pull log records from workers per epoch buffer
    {
      worker_map_lock_.Lock();

      size_t min_workers_persist_eid = INVALID_EPOCH_ID;
      for (auto &worker_entry : worker_map_) {
        // For every alive worker, move its buffer to the logger's local buffer
        auto worker_ctx_ptr = worker_entry.second.get();

        size_t last_persist_eid = worker_ctx_ptr->persist_eid;
        size_t worker_current_eid = worker_ctx_ptr->current_eid;

        PL_ASSERT(last_persist_eid <= worker_current_eid);

        if (last_persist_eid == worker_current_eid) {
          // The worker makes no progress
          continue;
        }

        for (size_t epoch_id = last_persist_eid + 1; epoch_id < worker_current_eid; ++epoch_id) {

          size_t epoch_idx = epoch_id % epoch_manager.GetEpochQueueCapacity();
          // get all the buffers that are associated with the epoch.
          auto &buffers = worker_ctx_ptr->per_epoch_buffer_ptrs[epoch_idx];

          if (buffers.empty() == true) {
            // no transaction log is generated within this epoch.
            // or we have logged this epoch before.
            // just skip it.
            continue;
          }

          // if we have something to write, then check whether we need to create new file.
          FileHandle *file_handle = nullptr;
          
          for (auto &entry : file_handles) {
            if (epoch_id >= entry.second && epoch_id < entry.second + file_epoch_count) {
              file_handle = entry.first;
            }
          }
          while (file_handle == nullptr) {
            current_file_eid = current_file_eid + file_epoch_count;
            printf("create new file with epoch id = %lu, last persist eid = %lu, current eid = %lu\n", current_file_eid, last_persist_eid, worker_current_eid);
            FileHandle *new_file_handle = new FileHandle();
            file_handles.push_back(std::make_pair(new_file_handle, current_file_eid));

            std::string filename = GetLogFileFullPath(current_file_eid);
            // Create a new file
            if (LoggingUtil::OpenFile(filename.c_str(), "wb", *new_file_handle) == false) {
              LOG_ERROR("Unable to create log file %s\n", filename.c_str());
              exit(EXIT_FAILURE);
            }
            if (epoch_id >= current_file_eid && epoch_id < current_file_eid + file_epoch_count) {
              file_handle = new_file_handle;
              break;
            }
          }


          PersistEpochBegin(*file_handle, epoch_id);
          // persist all the buffers.
          while (buffers.empty() == false) {
            // Check if the buffer is empty
            // TODO: is it possible to have an empty buffer??? --YINGJUN
            if (buffers.top()->Empty()) {
              worker_ctx_ptr->buffer_pool.PutBuffer(std::move(buffers.top()));
            } else {
              PersistLogBuffer(*file_handle, std::move(buffers.top()));
            }
            PL_ASSERT(buffers.top() == nullptr);
            buffers.pop();
          }
          PersistEpochEnd(*file_handle, epoch_id);
          // Call fsync
          LoggingUtil::FFlushFsync(*file_handle);
        } // end for

        worker_ctx_ptr->persist_eid = worker_current_eid - 1;

        if (min_workers_persist_eid == INVALID_EPOCH_ID || min_workers_persist_eid > (worker_current_eid - 1)) {
          min_workers_persist_eid = worker_current_eid - 1;
        }

      } // end for

      if (min_workers_persist_eid == INVALID_EPOCH_ID) {
        // in this case, it is likely that there's no registered worker or there's nothing to persist.
        worker_map_lock_.Unlock();
        continue;
      }

      PL_ASSERT(min_workers_persist_eid >= persist_epoch_id_);

      persist_epoch_id_ = min_workers_persist_eid;

      auto list_iter = file_handles.begin();

      while (list_iter != file_handles.end()) {
        if (list_iter->second + file_epoch_count <= persist_epoch_id_) {
          FileHandle *file_handle = list_iter->first;
        
          // Safely close the file
          bool res = LoggingUtil::CloseFile(*file_handle);
          if (res == false) {
            LOG_ERROR("Cannot close log file under directory %s", log_dir_.c_str());
            exit(EXIT_FAILURE);
          }
          
          delete file_handle;
          file_handle = nullptr;

          list_iter = file_handles.erase(list_iter);

        } else {
          ++list_iter;
        }
      }


      worker_map_lock_.Unlock();
    }
  }

  // Close the log file
  // TODO: Seek and write the integrity information in the header

  // Safely close all the files

  auto list_iter = file_handles.begin();

  while (list_iter != file_handles.end()) {
    FileHandle *file_handle = list_iter->first;

    bool res = LoggingUtil::CloseFile(*file_handle);
    if (res == false) {
      LOG_ERROR("Cannot close log file under directory %s", log_dir_.c_str());
      exit(EXIT_FAILURE);
    }

    delete file_handle;
    file_handle = nullptr;

    list_iter = file_handles.erase(list_iter);
  }
}

void CommandLogger::PersistEpochBegin(FileHandle &file_handle, const size_t epoch_id) {
  // Write down the epoch begin record  
  LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_BEGIN, epoch_id);

  logger_output_buffer_.Reset();

  size_t start = logger_output_buffer_.Position();
  logger_output_buffer_.WriteInt(0);

  logger_output_buffer_.WriteEnumInSingleByte(LOGRECORD_TYPE_EPOCH_BEGIN);
  logger_output_buffer_.WriteLong((uint64_t) epoch_id);

  logger_output_buffer_.WriteIntAt(start, (int32_t) (logger_output_buffer_.Position() - start - sizeof(int32_t)));

  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle.file);
}

void CommandLogger::PersistEpochEnd(FileHandle &file_handle, const size_t epoch_id) {
  // Write down the epoch end record
  LogRecord record = LogRecordFactory::CreateEpochRecord(LOGRECORD_TYPE_EPOCH_END, epoch_id);

  logger_output_buffer_.Reset();

  size_t start = logger_output_buffer_.Position();
  logger_output_buffer_.WriteInt(0);

  logger_output_buffer_.WriteEnumInSingleByte(LOGRECORD_TYPE_EPOCH_END);
  logger_output_buffer_.WriteLong((uint64_t) epoch_id);

  logger_output_buffer_.WriteIntAt(start, (int32_t) (logger_output_buffer_.Position() - start - sizeof(int32_t)));

  fwrite((const void *) (logger_output_buffer_.Data()), logger_output_buffer_.Size(), 1, file_handle.file);

}

void CommandLogger::PersistLogBuffer(FileHandle &file_handle, std::unique_ptr<LogBuffer> log_buffer) {

  fwrite((const void *) (log_buffer->GetData()), log_buffer->GetSize(), 1, file_handle.file);

  // Return the buffer to the worker
  log_buffer->Reset();
  auto itr = worker_map_.find(log_buffer->GetWorkerId());
  if (itr != worker_map_.end()) {
    itr->second->buffer_pool.PutBuffer(std::move(log_buffer));
  } else {
    // In this case, the worker is already terminated and removed
    // Release the buffer
    log_buffer.reset(nullptr);
  }
}


}
}