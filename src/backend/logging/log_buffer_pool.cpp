//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/log_buffer_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "log_buffer_pool.h"

namespace peloton {
namespace logging {

  // Acquire a log buffer from the buffer pool.
  // This function will be blocked until there is an available buffer.
  // Note that only one thread will call this function.
  std::unique_ptr<LogBuffer> LogBufferPool::GetBuffer() {
    size_t head_idx = head_ % buffer_queue_size_;
    while (true) {
      if (head_.load() < tail_.load()) {
        if (local_buffer_queue_[head_idx] == false) {
          // Not any buffer allocated now
          local_buffer_queue_[head_idx].reset(new LogBuffer(backend_logger_id_));
        }
        break;
      }

      // sleep a while, and try to get a new buffer
      _mm_pause();
    }

    head_++;
    return std::move(local_buffer_queue_[head_idx]);
  }

  void LogBufferPool::PutBuffer(std::unique_ptr<LogBuffer> buf) {
    PL_ASSERT(buf->GetBackendLoggerId() == backend_logger_id_);

    UNUSED_ATTRIBUTE size_t head_idx = head_ % buffer_queue_size_;
    size_t tail_idx = tail_ % buffer_queue_size_;
    // The buffer pool must not be full
    PL_ASSERT(tail_idx != head_idx);
    // The tail pos must be null
    PL_ASSERT(local_buffer_queue_[tail_idx] == false);
    // The returned buffer must be empty
    PL_ASSERT(buf->Empty() == true);
    local_buffer_queue_[tail_idx].reset(buf.release());
    tail_++;
  }

}
}
