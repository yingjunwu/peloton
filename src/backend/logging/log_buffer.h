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
  LogBuffer() {
    PL_MEMSET(data_, 0, log_buffer_capacity_);
  }
  ~LogBuffer() {}

  void Reset() { size_ = 0; }

  char *GetData() { return data_; }

  bool Empty() { return size_ == 0; }

  bool WriteData(char *data, size_t len);

private:
  size_t size_;
  char data_[log_buffer_capacity_];
};

}
}
