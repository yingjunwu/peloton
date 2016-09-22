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

#pragma once

#include <vector>

#include "backend/common/macros.h"
#include "backend/common/types.h"
#include "backend/logging/log_buffer.h"
#include "backend/common/platform.h"


namespace peloton {
namespace logging {
  class BackendBufferPool {
    BackendBufferPool(const BackendBufferPool &) = delete;
    BackendBufferPool &operator=(const BackendBufferPool &) = delete;
    BackendBufferPool(const BackendBufferPool &&) = delete;
    BackendBufferPool &operator=(const BackendBufferPool &&) = delete;

  public:
    BackendBufferPool() : head_(0), tail_(buffer_queue_size_), local_buffer_queue_(buffer_queue_size_) {}

    std::unique_ptr<LogBuffer> GetBuffer();

    void PutBuffer(std::unique_ptr<LogBuffer> buf);

  private:
    static const size_t buffer_queue_size_ = 16;
    std::atomic<size_t> head_;
    std::atomic<size_t> tail_;

    std::vector<std::unique_ptr<LogBuffer>> local_buffer_queue_;
};

}
}
