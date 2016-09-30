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
#include "backend/storage/data_table.h"
#include "backend/common/serializer.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LoggingUtil
//===--------------------------------------------------------------------===//

class LoggingUtil {
 public:
  // FILE SYSTEM RELATED OPERATIONS
  static bool CheckDirectoryExistence(const char *dir_name);

  static bool CreateDirectory(const char *dir_name, int mode);

  static bool RemoveDirectory(const char *dir_name, bool only_remove_file);
  
  static void FFlushFsync(FileHandle &file_handle);
  
  static bool CreateFile(const char *name, const char *mode, FileHandle &file_handle);

  static bool CloseFile(FileHandle &file_handle);

  static bool IsFileTruncated(FileHandle &file_handle, size_t size_to_read);
  
//  static size_t GetFileSize(FileHandle &file_handle);
//
//
//  // TUPLE TO LOG TRANSFORMATIONS
//  static bool SerializeTuple();
//
//  static bool DeserializeTuple();

  static void CompressData() {}

  static void DecompressData() {}


  // LOGGING/CHECKPOINTING SPECIFIC OPERATIONS
  /**
   * logging file name layout :
   * 
   * dir_name + "/" + prefix + "_" + epoch_id
   *
   *
   * logging file layout :
   *
   *  -----------------------------------------------------------------------------
   *  | txn_cid | database_id | table_id | operation_type | data | ... | txn_end_flag
   *  -----------------------------------------------------------------------------
   *
   * NOTE: this layout is designed for physiological logging.
   *
   * NOTE: tuple length can be obtained from the table schema.
   *
   */

  /**
   * logging file name layout :
   * 
   * dir_name + "/" + prefix + "_" + epoch_id
   *
   *
   * logging file layout :
   *
   *  -----------------------------------------------------------------------------
   *  | txn_cid | database_id | table_id | operation_type | column_count | columns | data | ... | txn_end_flag
   *  -----------------------------------------------------------------------------
   *
   * NOTE: this layout is designed for physiological delta logging.
   *
   * NOTE: tuple length can be obtained from the table schema.
   *
   */

  /**
   * logging file name layout :
   * 
   * dir_name + "/" + prefix + "_" + epoch_id
   *
   *
   * logging file layout :
   *
   *  -----------------------------------------------------------------------------
   *  | epoch_id | database_id | table_id | operation_type | column_count | columns | data | ... | epoch_end_flag
   *  -----------------------------------------------------------------------------
   *
   * NOTE: this layout is designed for epoch logging.
   *
   * NOTE: tuple length can be obtained from the table schema.
   *
   */


//    static GetLoggingFileFullPath();



//
//  static size_t GetNextFrameSize(FileHandle &file_handle);
//
//  static LogRecordType GetNextLogRecordType(FileHandle &file_handle);
//
//  static int ExtractNumberFromFileName(const char *name);
//
//  static storage::Tuple *ReadTupleRecordBody(catalog::Schema *schema,
//                                             VarlenPool *pool,
//                                             FileHandle &file_handle);
//
//  static void SkipTupleRecordBody(FileHandle &file_handle);
//
//  static int GetFileSizeFromFileName(const char *);

};

}  // namespace logging
}  // namespace peloton
