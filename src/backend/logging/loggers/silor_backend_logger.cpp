//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/silor_backend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/logging/loggers/silor_backend_logger.h"

namespace peloton {
namespace logging {

void SiloRBackendLogger::WriteRecord(LogRecord &record) {
  // Serialize the record into the local buffer

  // TODO: Try to avoid an extra copy by modifying the API of the output buffer
  output_buffer_.Reset();
  record.Serialize(output_buffer_);

  // Try to write to the current buffer
  PL_ASSERT(current_buffer_ptr_ != nullptr);
  if (current_buffer_ptr_->WriteData(output_buffer_.Data(), output_buffer_.Size()));
}

}
}