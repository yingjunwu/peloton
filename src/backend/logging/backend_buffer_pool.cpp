//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/backend_buffer_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend_buffer_pool.h"

namespace peloton {
namespace logging {

  // Acquire a log buffer from the buffer pool.
  // This function will be blocked until there is an available buffer.
  // Note that only one thread will call this function.
  std::unique_ptr<LogBuffer> BackendBufferPool::GetBuffer() {
    while (head_.load() < tail_.load()) {
      std::unique_ptr<LogBuffer> buffer_ptr = local_buffer_queue_[head_++];
      if (buffer_ptr == false) {
        // Not any buffer allocated now

      }
    }
  }

  void BackendBufferPool::PutBuffer(std::unique_ptr<LogBuffer> buf) {

  }

}
}
