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

#include "backend/logging/dep_logger.h"

namespace peloton {
  namespace logging {

    void DepLogger::RegisterWorker(std::shared_ptr<WorkerContext> worker_ctx) {
      worker_map_lock_.Lock();
      worker_map_[worker_ctx->worker_id] = worker_ctx;
      worker_map_lock_.Unlock();
    }

    void DepLogger::DeregisterWorker(WorkerContext *worker_ctx) {
      worker_map_lock_.Lock();
      worker_map_.erase(worker_ctx->worker_id);
      worker_map_lock_.Unlock();
    }


    void DepLogger::Run() {
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

      // Thread local status
      size_t current_global_eid;
      std::vector<size_t> worker_current_eids;
      /**
       *  Main loop
       */
      while (true) {
        if (is_running_ == false) { break; }

        std::this_thread::sleep_for(
          std::chrono::microseconds(concurrency::EpochManagerFactory::GetInstance().GetEpochLengthInMicroSecQuarter()));

        // Set thread local status
        worker_current_eids.clear();
        current_global_eid = epoch_manager.GetCurrentEpochId();
        // Should first load the global eid, then begin to check workers' states
        COMPILER_MEMORY_FENCE;

        // Pull log records from workers per epoch buffer
        {
          worker_map_lock_.Lock();
          size_t min_worker_cur_eid = MAX_EPOCH_ID;

          for (auto &worker_entry : worker_map_) {
            auto worker_ctx_ptr = worker_entry.second.get();

            size_t last_persist_eid = worker_ctx_ptr->persist_eid;
            // It's possible that the worker's current eid is larger than the global observed by the logger.
            // In such case, taking a min is safe.
            size_t worker_current_eid = std::min(current_global_eid,
                                                 epoch_manager.GetRwTxnWorkerCurrentEid(worker_ctx_ptr->transaction_worker_id));
            worker_current_eids.push_back(worker_current_eid);
            min_worker_cur_eid = std::min(min_worker_cur_eid, worker_current_eid);

            if (last_persist_eid == worker_current_eid) {
              // The worker makes no progress
              continue;
            }

            // Persist the worker's log buffer
            for (size_t epoch_id = last_persist_eid + 1; epoch_id < worker_current_eid; ++epoch_id) {
              size_t epoch_idx = epoch_id % concurrency::EpochManager::GetEpochQueueCapacity();
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
                // printf("create new file with epoch id = %lu, last persist eid = %lu, current eid = %lu\n", current_file_eid, last_persist_eid, worker_current_eid);
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

            // Update the last seen epoch id
            worker_ctx_ptr->persist_eid = worker_current_eid - 1;
          }

          // Publish status to the pepoch thread
          {
            status_lock_.Lock();
            logger_cur_eid_ = current_global_eid;
            workers_cur_eids_ = worker_current_eids;
            status_lock_.Unlock();
          }

          auto list_iter = file_handles.begin();

          while (list_iter != file_handles.end()) {
            if (min_worker_cur_eid != MAX_EPOCH_ID && list_iter->second + file_epoch_count <= min_worker_cur_eid) {
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

    void DepLogger::PersistEpochBegin(FileHandle &file_handle, const size_t epoch_id) {
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

    void DepLogger::PersistEpochEnd(FileHandle &file_handle, const size_t epoch_id) {
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

    void DepLogger::PersistLogBuffer(FileHandle &file_handle, std::unique_ptr<LogBuffer> log_buffer) {

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