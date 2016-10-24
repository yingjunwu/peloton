//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction.h
//
// Identification: src/backend/concurrency/transaction.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <atomic>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>

#include "backend/common/printable.h"
#include "backend/common/types.h"
#include "backend/common/exception.h"

#include "backend/common/serializer.h"


namespace peloton {

class TransactionParameter {
public:
  virtual void SerializeTo(SerializeOutput &output) = 0;
  virtual void DeserializeFrom(SerializeInputBE &input) = 0;
};

namespace concurrency {

//===--------------------------------------------------------------------===//
// Transaction
//===--------------------------------------------------------------------===//

enum RWType {
  RW_TYPE_READ,
  RW_TYPE_UPDATE,
  RW_TYPE_INSERT,
  RW_TYPE_DELETE,
  RW_TYPE_INS_DEL  // delete after insert.
};

struct TsContent {
  TsContent() {
    rw_type_ = RW_TYPE_READ;
    begin_cid_ = 0;
  }
  TsContent(const RWType rw_type, const cid_t &begin_cid) {
    rw_type_ = rw_type;
    begin_cid_ = begin_cid;
  }

  RWType rw_type_;
  cid_t begin_cid_;
};

class Transaction : public Printable {
  Transaction(Transaction const &) = delete;

 public:
  Transaction()
      : txn_id_(INVALID_TXN_ID),
        begin_cid_(INVALID_CID),
        end_cid_(MAX_CID),
        static_read_only_(false),
        is_written_(false),
        insert_count_(0) {}

  Transaction(const txn_id_t &txn_id)
      : txn_id_(txn_id),
        begin_cid_(INVALID_CID),
        end_cid_(MAX_CID),
        static_read_only_(false),
        is_written_(false),
        insert_count_(0) {}

  Transaction(const txn_id_t &txn_id, bool read_only, const cid_t &begin_cid)
    : txn_id_(txn_id),
      begin_cid_(begin_cid),
      end_cid_(MAX_CID),
      static_read_only_(read_only),
      is_written_(false),
      insert_count_(0) {}


  Transaction(const txn_id_t &txn_id, const cid_t &begin_cid)
      : txn_id_(txn_id),
        begin_cid_(begin_cid),
        end_cid_(MAX_CID),
        static_read_only_(false),
        is_written_(false),
        insert_count_(0) {}

  ~Transaction() {}

  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//

  inline txn_id_t GetTransactionId() const { return txn_id_; }

  inline cid_t GetBeginCommitId() const { return begin_cid_; }

  inline cid_t GetEndCommitId() const { return end_cid_; }

  inline size_t GetEpochId() const { return epoch_id_; }

  inline void SetEndCommitId(cid_t eid) { end_cid_ = eid; }

  inline void SetEpochId(const size_t eid) { epoch_id_ = eid; }

  inline int GetTransactionType() {
    return transaction_type_;
  }

  inline void SetTransactionType(const int transaction_type) {
    transaction_type_ = transaction_type;
  }

  inline void SetTransactionParam(TransactionParameter *txn_param) {
    txn_param_ = txn_param;
  }

  void RecordRead(const ItemPointer &);

  void RecordUpdate(const ItemPointer &);

  void RecordInsert(const ItemPointer &);

  // Return true if we detect INS_DEL
  bool RecordDelete(const ItemPointer &);

  //const std::map<oid_t, std::map<oid_t, RWType>> &GetRWSet();
  inline const std::unordered_map<oid_t, std::unordered_map<oid_t, RWType>> &GetRWSet() {
    return rw_set_;
  }

  void RecordReadTs(const ItemPointer &, const cid_t &);

  void RecordUpdateTs(const ItemPointer &);

  void RecordInsertTs(const ItemPointer &);

  // Return true if we detect INS_DEL
  bool RecordDeleteTs(const ItemPointer &);

  inline const std::unordered_map<oid_t, std::unordered_map<oid_t, TsContent>> &GetTsRWSet() {
    return ts_rw_set_;
  }

  cid_t GetMinCid() const {
    cid_t min_ts = 0;
    for (auto &entries : ts_rw_set_) {
      for (auto &entry : entries.second) {
        if (entry.second.begin_cid_ > min_ts) {
          min_ts = entry.second.begin_cid_;
        }
      }
    }
    return min_ts;
  }


  // Get a string representation for debugging
  const std::string GetInfo() const;

  inline bool IsStaticReadOnlyTxn() const {
    return static_read_only_;
  }

  // Set result and status
  inline void SetResult(Result result) { result_ = result; }

  // Get result and status
  inline Result GetResult() const { return result_; }

  inline bool IsReadOnly() const {
    return is_written_ == false && insert_count_ == 0;
  }

 public:

  int transaction_type_;

  TransactionParameter *txn_param_;


 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // transaction id
  txn_id_t txn_id_;

  // start commit id
  cid_t begin_cid_;

  // end commit id
  cid_t end_cid_;

  // epoch id
  size_t epoch_id_;

  // read only flag
  bool static_read_only_;

  // in the protocols we have implemented, there's no need to maintain an ordered rw-set.
  // this is because the optimistic mechanism locks writes and handles aborts aggressively.
  // it does not need to reorder rw-set to guarantee deadlock-free.
  //std::map<oid_t, std::map<oid_t, RWType>> rw_set_;
  std::unordered_map<oid_t, std::unordered_map<oid_t, RWType>> rw_set_;

  std::unordered_map<oid_t, std::unordered_map<oid_t, TsContent>> ts_rw_set_;

  // result of the transaction
  Result result_ = peloton::RESULT_SUCCESS;

  bool is_written_;
  size_t insert_count_;

public:
  cid_t lower_bound_cid_;
  cid_t upper_bound_cid_;
};

}  // End concurrency namespace
}  // End peloton namespace
