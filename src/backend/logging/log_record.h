//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_record.h
//
// Identification: src/backend/logging/log_record.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "backend/common/types.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/common/serializer.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LogRecord
//===--------------------------------------------------------------------===//

class LogRecord {
public:
  LogRecord(LogRecordType log_record_type)
      : log_record_type_(log_record_type), tuple_pos_(INVALID_ITEMPOINTER),
        eid_(INVALID_EPOCH_ID), cid_(INVALID_CID) {
    PL_ASSERT(log_record_type != LOGRECORD_TYPE_INVALID);
  }

  virtual ~LogRecord() {}

  inline LogRecordType GetType() const { return log_record_type_; }

  inline void SetItemPointer(const ItemPointer &pos) { tuple_pos_ = pos; }

  inline void SetEpochId(const size_t epoch_id) { eid_ = epoch_id; }

  inline void SetCommitId(const cid_t commit_id) { cid_ = commit_id; }

  inline ItemPointer GetItemPointer() { return tuple_pos_; }

  inline size_t GetEpochId() { return eid_; }

  inline cid_t GetCommitId() { return cid_; }

  void Serialize(CopySerializeOutput &output);

  void Deserialize(CopySerializeInput &input);

private:
  LogRecordType log_record_type_ = LOGRECORD_TYPE_INVALID;

  ItemPointer tuple_pos_;

  size_t eid_;

  cid_t cid_;
};

}  // namespace logging
}  // namespace peloton
