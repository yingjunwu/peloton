//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_buffer.h
//
// Identification: src/backend/logging/log_buffer.h
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

  friend class LogBufferPool;
private:
  // constant
  const static size_t log_buffer_capacity_ = 1024 * 1024 * 32; // 32 MB

public:
  LogBuffer(size_t backend_id, size_t eid) : worker_id_(backend_id), eid_(eid), size_(0){
    PL_MEMSET(data_, 0, log_buffer_capacity_);
  }
  ~LogBuffer() {}

  inline void Reset() { size_ = 0; eid_ = INVALID_EPOCH_ID;}

  inline char *GetData() { return data_; }

  inline size_t GetSize() { return size_; }

  inline size_t GetEid() { return eid_; }

  inline size_t GetWorkerId() { return worker_id_; }

  inline bool Empty() { return size_ == 0; }

  bool WriteData(const char *data, size_t len);

private:
  size_t worker_id_;
  size_t eid_;
  size_t size_;
  char data_[log_buffer_capacity_];
};

}
}
