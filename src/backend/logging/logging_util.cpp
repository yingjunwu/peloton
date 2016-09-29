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

#include <sys/stat.h>
#include <dirent.h>
#include <cstring>

#include "backend/catalog/manager.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/database.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LoggingUtil
//===--------------------------------------------------------------------===//

bool LoggingUtil::CreateDirectory(const char *dir_name, int mode) {
  int return_val = mkdir(dir_name, mode);
  if (return_val == 0) {
    LOG_TRACE("Created directory %s successfully", dir_name);
  } else if (errno == EEXIST) {
    LOG_TRACE("Directory %s already exists", dir_name);
  } else {
    LOG_TRACE("Creating directory failed: %s", strerror(errno));
    return false;
  }
  return true;
}

bool LoggingUtil::CheckDirectoryExistence(const char *dir_name) {
  struct stat info;
  int return_val = stat(dir_name, &info);
  return return_val == 0 && S_ISDIR(info.st_mode);
}

/**
 * @return false if fail to remove directory
 */
bool LoggingUtil::RemoveDirectory(const char *dir_name, bool only_remove_file) {
  struct dirent *file;
  DIR *dir;

  dir = opendir(dir_name);
  if (dir == nullptr) {
    return true;
  }

  // XXX readdir is not thread safe???
  while ((file = readdir(dir)) != nullptr) {
    if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0) {
      continue;
    }
    char complete_path[256];
    strcpy(complete_path, dir_name);
    strcat(complete_path, "/");
    strcat(complete_path, file->d_name);
    auto ret_val = remove(complete_path);
    if (ret_val != 0) {
      LOG_ERROR("Failed to delete file: %s, error: %s", complete_path,
                strerror(errno));
    }
  }
  closedir(dir);
  if (!only_remove_file) {
    auto ret_val = remove(dir_name);
    if (ret_val != 0) {
      LOG_ERROR("Failed to delete dir: %s, error: %s", file->d_name,
                strerror(errno));
    }
  }
  return true;
}

void LoggingUtil::FFlushFsync(FileHandle &file_handle) {
  // First, flush
  PL_ASSERT(file_handle.fd != INVALID_FILE_DESCRIPTOR);
  if (file_handle.fd == INVALID_FILE_DESCRIPTOR) return;
  int ret = fflush(file_handle.file);
  if (ret != 0) {
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }
  // Finally, sync
  ret = fsync(file_handle.fd);
  if (ret != 0) {
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }
}

bool LoggingUtil::CreateFile(const char *name, const char *mode, FileHandle &file_handle) {
  auto file = fopen(name, mode);
  if (file == NULL) {
    LOG_ERROR("Checkpoint File is NULL");
    return false;
  } else {
    file_handle.file = file;
  }

  // also, get the descriptor
  auto fd = fileno(file);
  if (fd == INVALID_FILE_DESCRIPTOR) {
    LOG_ERROR("checkpoint_file_fd_ is -1");
    return false;
  } else {
    file_handle.fd = fd;
  }
  file_handle.size = 0;
  return true;
}

bool LoggingUtil::IsFileTruncated(FileHandle &file_handle,
                                  size_t size_to_read) {
  // Cache current position
  size_t current_position = ftell(file_handle.file);

  // Check if the actual file size is less than the expected file size
  // Current position + frame length
  if (current_position + size_to_read <= file_handle.size) {
    return false;
  } else {
    fseek(file_handle.file, 0, SEEK_END);
    return true;
  }
}

size_t LoggingUtil::GetFileSize(FileHandle &file_handle) {
  struct stat file_stats;
  fstat(file_handle.fd, &file_stats);
  return file_stats.st_size;
}

size_t LoggingUtil::GetNextFrameSize(FileHandle &file_handle) {
  size_t frame_size;
  char buffer[sizeof(int32_t)];

  // Check if the frame size is broken
  if (LoggingUtil::IsFileTruncated(file_handle, sizeof(buffer))) {
    return 0;
  }

  // Otherwise, read the frame size
  size_t ret = fread(buffer, 1, sizeof(buffer), file_handle.file);
  if (ret <= 0) {
    LOG_ERROR("Error occured in fread ");
  }

  // Read next 4 bytes as an integer
  CopySerializeInputBE frameCheck(buffer, sizeof(buffer));
  frame_size = (frameCheck.ReadInt()) + sizeof(buffer);

  // Move back by 4 bytes
  // So that tuple deserializer works later as expected
  int res = fseek(file_handle.file, -sizeof(buffer), SEEK_CUR);
  if (res == -1) {
    LOG_ERROR("Error occured in fseek ");
  }

  // Check if the frame is broken
  if (IsFileTruncated(file_handle, frame_size)) {
    return 0;
  }

  return frame_size;
}

LogRecordType LoggingUtil::GetNextLogRecordType(FileHandle &file_handle) {
  char buffer;

  // Check if the log record type is broken
  if (IsFileTruncated(file_handle, 1)) {
    LOG_TRACE("Log file is truncated");
    return LOGRECORD_TYPE_INVALID;
  }

  // Otherwise, read the log record type
  int ret = fread((void *)&buffer, 1, sizeof(char), file_handle.file);
  if (ret <= 0) {
    LOG_ERROR("Could not read from log file");
    return LOGRECORD_TYPE_INVALID;
  }

  CopySerializeInputBE input(&buffer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());

  return log_record_type;
}

int LoggingUtil::ExtractNumberFromFileName(const char *name) {
  std::string str(name);
  size_t start_index = str.find_first_of("0123456789");
  if (start_index != std::string::npos) {
    int end_index = str.find_first_not_of("0123456789", start_index);
    return atoi(str.substr(start_index, end_index - start_index).c_str());
  }
  LOG_ERROR("The last found log file doesn't have a version number.");
  return 0;
}

storage::Tuple *LoggingUtil::ReadTupleRecordBody(catalog::Schema *schema,
                                                 VarlenPool *pool,
                                                 FileHandle &file_handle) {
  // Check if the frame is broken
  size_t body_size = GetNextFrameSize(file_handle);
  if (body_size == 0) {
    LOG_ERROR("Body size is zero ");
    return nullptr;
  }

  // Read Body
  char body[body_size];
  int ret = fread(body, 1, sizeof(body), file_handle.file);
  if (ret <= 0) {
    LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInputBE tuple_body(body, body_size);

  // We create a tuple based on the message
  storage::Tuple *tuple = new storage::Tuple(schema, true);
  tuple->DeserializeFrom(tuple_body, pool);

  return tuple;
}

void LoggingUtil::SkipTupleRecordBody(FileHandle &file_handle) {
  // Check if the frame is broken
  size_t body_size = GetNextFrameSize(file_handle);
  if (body_size == 0) {
    LOG_ERROR("Body size is zero ");
  }

  // Read Body
  char body[body_size];
  int ret = fread(body, 1, sizeof(body), file_handle.file);
  if (ret <= 0) {
    LOG_ERROR("Error occured in fread ");
  }

  // TODO Is it necessary?
  CopySerializeInputBE tuple_body(body, body_size);
}


int LoggingUtil::GetFileSizeFromFileName(const char *file_name) {
  struct stat st;
  int ret_val;

  ret_val = stat(file_name, &st);
  if (ret_val == 0) return st.st_size;

  return -1;
}


}  // namespace logging
}  // namespace peloton
