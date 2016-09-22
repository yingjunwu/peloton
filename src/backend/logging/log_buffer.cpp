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


#include "backend/common/macros.h"
#include "backend/logging/log_buffer.h"

namespace peloton {
namespace logging {

bool LogBuffer::WriteData(const char *data, size_t len) {
  if (size_ + len > log_buffer_capacity_) {
    return false;
  } else {
    PL_ASSERT(data);
    PL_ASSERT(len);
    PL_MEMCPY(data_ + size_, data, len);
    size_ += len;
    return true;
  }
}

}
}