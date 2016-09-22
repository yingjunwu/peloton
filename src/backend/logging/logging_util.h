/*-------------------------------------------------------------------------
 *
 * logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LoggingUtil
//===--------------------------------------------------------------------===//

class LoggingUtil {
 public:
  static void FFlushFsync(FileHandle &file_handle);

  static bool InitFileHandle(const char *name, FileHandle &file_handle,
                             const char *mode);

  static size_t GetLogFileSize(FileHandle &file_handle);

  static bool IsFileTruncated(FileHandle &file_handle, size_t size_to_read);

  static size_t GetNextFrameSize(FileHandle &file_handle);

  static LogRecordType GetNextLogRecordType(FileHandle &file_handle);

  static int ExtractNumberFromFileName(const char *name);

  static storage::Tuple *ReadTupleRecordBody(catalog::Schema *schema,
                                             VarlenPool *pool,
                                             FileHandle &file_handle);

  static void SkipTupleRecordBody(FileHandle &file_handle);

  static int GetFileSizeFromFileName(const char *);

  static bool CreateDirectory(const char *dir_name, int mode);

  static bool RemoveDirectory(const char *dir_name, bool only_remove_file);

  static void Checkpointer::CreateFile() {
    // open checkpoint file and file descriptor
    std::string file_name = ConcatFileName(checkpoint_dir, ++checkpoint_version);
    bool success =
        LoggingUtil::InitFileHandle(file_name.c_str(), file_handle_, "ab");
    if (!success) {
      PL_ASSERT(false);
      return;
    }
    LOG_TRACE("Created a new checkpoint file: %s", file_name.c_str());
  }

  // Wrappers
  /**
   * @brief Read get table based on tuple record
   * @param tuple record
   * @return data table
   */
  static storage::DataTable *GetTable(TupleRecord &tupleRecord);
};

}  // namespace logging
}  // namespace peloton
