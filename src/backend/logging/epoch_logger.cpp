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

#include "backend/gc/gc_manager_factory.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "backend/catalog/manager.h"
#include "backend/concurrency/epoch_manager_factory.h"
#include "backend/expression/container_tuple.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"

#include "backend/logging/epoch_logger.h"

namespace peloton {
namespace logging {

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

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
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