//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/log_buffer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"

namespace peloton {
namespace logging {

class LogBuffer {
  LogBuffer(const LogBuffer &) = delete;
  LogBuffer &operator=(const LogBuffer &) = delete;
  LogBuffer(LogBuffer &&) = delete;
  LogBuffer &operator=(LogBuffer &&) = delete;

private:
  // constant
  const static size_t log_buffer_capacity_ = 1024 * 1024 * 32; // 32 MB

public:
  LogBuffer(size_t backend_id) : worker_id_(backend_id) {
    PL_MEMSET(data_, 0, log_buffer_capacity_);
  }
  ~LogBuffer() {}

  inline void Reset() { size_ = 0; }

  inline char *GetData() { return data_; }

  inline size_t GetSize() { return size_; }

  inline size_t GetWorkerId() { return worker_id_; }

  inline bool Empty() { return size_ == 0; }

  bool WriteData(const char *data, size_t len);

private:
  size_t worker_id_;
  size_t size_;
  char data_[log_buffer_capacity_];
};

}
}
