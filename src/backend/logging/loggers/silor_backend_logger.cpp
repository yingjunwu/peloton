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

#include "backend/concurrency/epoch_manager.h"
#include "backend/logging/log_record.h"
#include "backend/logging/loggers/silor_backend_logger.h"

namespace peloton {
namespace logging {

std::vector<SiloRBackendLogger::LogBufferMap> per_epoch_buffermaps_vector =
  std::vector<SiloRBackendLogger::LogBufferMap>(concurrency::EpochManager::GetEpochQueueCapacity());

void SiloRBackendLogger::WriteRecord(LogRecord &record) {
  // Serialize the record into the local buffer

  // TODO: Try to avoid an extra copy by modifying the API of the output buffer
  output_buffer_.Reset();
  record.Serialize(output_buffer_);

  // Try to write to the current buffer
  PL_ASSERT(current_buffer_ptr_ != nullptr);
  if (current_buffer_ptr_->WriteData(output_buffer_.Data(), output_buffer_.Size()) == false) {
    // Not enough buffer, publish the old buffer and try to get a new one
    PublishCurrentLogBuffer();
    // TODO: remove it after correctly implementing PublishCurrentLogBuffer
    PL_ASSERT(current_buffer_ptr_ == nullptr);
    // May block here
    RegisterBufferToEpoch(std::move(buffer_pool_.GetBuffer()));
    current_buffer_ptr_ = GetCurrentLogBufferPtr();
    // Write it again
    bool res = current_buffer_ptr_->WriteData(output_buffer_.Data(), output_buffer_.Size());
    PL_ASSERT(res == true);
  }
}

void SiloRBackendLogger::LogUpdate(const ItemPointer &tuple_pos) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE, tuple_pos);
  WriteRecord(record);
}

void SiloRBackendLogger::LogInsert(const ItemPointer &tuple_pos) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LOGRECORD_TYPE_TUPLE_INSERT, tuple_pos);
  WriteRecord(record);
}

// Note that the tuple pos in the DELETED record is the tuple being deleted
// TODO: Any conflict with the GC manager?
void SiloRBackendLogger::LogDelete(const ItemPointer &tuple_pos) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LOGRECORD_TYPE_TUPLE_DELETE, tuple_pos);
  WriteRecord(record);
}

// This function is called in the transcation manager's commit process
// We won't have any dirty log record for aborted txns
void SiloRBackendLogger::StartTxn(concurrency::Transaction *txn) {
  // Check if the epoch has changed
  size_t txn_eid = txn->GetEpochId();

  if (current_eid_ == INVALID_EPOCH_ID) {
    // Init the backend logger's epoch_id
    current_eid_ = txn_eid;
    RegisterBufferToEpoch(std::move(buffer_pool_.GetBuffer()));
    current_buffer_ptr_ = GetCurrentLogBufferPtr();
  } else if (current_eid_ != txn_eid) {
    PL_ASSERT(current_eid_ < txn_eid);
    current_eid_ = txn_eid;
    // Get a new buffer, may block here
    RegisterBufferToEpoch(std::move(buffer_pool_.GetBuffer()));
    current_buffer_ptr_ = GetCurrentLogBufferPtr();
  }

  cid_t txn_cid = txn->GetBeginCommitId();

  // May not hold for the to opt protocol
  PL_ASSERT(txn_cid != current_cid_);
  current_cid_ = txn_cid;
  LogRecord record = LogRecordFactory::CreateTxnRecord(LOGRECORD_TYPE_TRANSACTION_BEGIN, current_cid_);
  WriteRecord(record);
}

void SiloRBackendLogger::CommitCurrentTxn() {
  // Write down a commit record for the current transaction
  LogRecord record = LogRecordFactory::CreateTxnRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT, current_cid_);
  WriteRecord(record);
}

void SiloRBackendLogger::PublishCurrentLogBuffer() {

}

}
}