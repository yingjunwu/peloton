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

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// LogRecord
//===--------------------------------------------------------------------===//
class LogRecordFactory {
public:
  static LogRecord CreateTupleRecord(LogRecordType log_type, const ItemPointer &pos) {
    return LogRecord(log_type, pos, INVALID_EPOCH_ID, INVALID_CID);
  }

  static LogRecord CreateTxnRecord(LogRecordType log_type, cid_t commit_id) {
    return LogRecord(log_type, INVALID_ITEMPOINTER, INVALID_EPOCH_ID, commit_id);
  }

  static LogRecord CreateEpochRecord(LogRecordType log_type, size_t epoch_id) {
    return LogRecord(log_type, INVALID_ITEMPOINTER, epoch_id, INVALID_CID);
  }
};

class LogRecord {
  friend class LogRecordFactory;
private:
  LogRecord(LogRecordType log_type, const ItemPointer &pos, size_t epoch_id, cid_t commit_id)
    : log_record_type_(log_type), tuple_pos_(pos), eid_(epoch_id), cid_(commit_id) {}

public:
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
