//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_record.h
//
// Identification: src/backend/logging/log_record.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/expression/container_tuple.h"
#include "backend/catalog/manager.h"
#include "backend/logging/log_record.h"

namespace peloton {
namespace logging {

void LogRecord::Serialize(CopySerializeOutput &output){
  // First output the record type
  output.WriteEnumInSingleByte(log_record_type_);

  // According to the record type, write the following header info
  switch (log_record_type_) {
    case LOGRECORD_TYPE_TUPLE_INSERT:
    case LOGRECORD_TYPE_TUPLE_DELETE:
    case LOGRECORD_TYPE_TUPLE_UPDATE: {
      auto &manager = catalog::Manager::GetInstance();
      expression::ContainerTuple<storage::TileGroup> container_tuple(
        manager.GetTileGroup(tuple_pos_.block).get(), tuple_pos_.offset
      );
      container_tuple.SerializeTo(output);
      break;
    }
    case LOGRECORD_TYPE_TRANSACTION_BEGIN:
    case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
      output.WriteLong(cid_);
      break;
    }
    case LOGRECORD_TYPE_EPOCH_BEGIN:
    case LOGRECORD_TYPE_EPOCH_END: {
      output.WriteLong(*(reinterpret_cast<uint64_t *>(&eid_)));
      break;
    }
    default: {
      LOG_ERROR("Unsupported log record type");
      PL_ASSERT(false);
    }
  }
}

// void LogRecord::Deserialize(CopySerializeInput &input UNUSED_ATTRIBUTE) {
//     // TODO: Implement it
// }

}
}