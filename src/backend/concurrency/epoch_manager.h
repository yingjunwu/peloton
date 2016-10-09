//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_manager.h
//
// Identification: src/backend/concurrency/epoch_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <thread>
#include <vector>

#include "backend/common/macros.h"
#include "backend/common/types.h"
#include "backend/common/platform.h"

namespace peloton {
namespace concurrency {

#define EPOCH_LENGTH 40

struct Epoch {
  std::atomic<int> ro_txn_ref_count_;
  std::atomic<int> rw_txn_ref_count_;
  std::atomic<size_t> id_generator_;

  Epoch()
    :ro_txn_ref_count_(0), rw_txn_ref_count_(0), id_generator_(0) {}

  void Init() {
    ro_txn_ref_count_ = 0;
    rw_txn_ref_count_ = 0;
    id_generator_ = 0;
  }
};

class EpochManager {
  EpochManager(const EpochManager&) = delete;

 public:
  EpochManager(const int epoch_length)
 : epoch_duration_milisec_(epoch_length) {}

  virtual ~EpochManager() {}

  virtual void StartEpochManager() = 0;

  virtual void Reset() = 0;

  static inline size_t GetEpochQueueCapacity() { return epoch_queue_size_; }
  static inline size_t GetEidFromCid(cid_t cid) {
    return (cid >> 32);
  }

  static inline cid_t GetReadonlyCidFromEid(size_t eid) {
    return (eid << 32) | low_32_bit_mask_;
  }

  int GetEpochLengthInMiliSec() const {
    return epoch_duration_milisec_;
  }

  virtual size_t GetCurrentEpoch() = 0;

  // Get a eid that is larger than all the running transactions
  // TODO: See if we can delete this method
  virtual size_t GetCurrentCid() = 0;

  virtual cid_t EnterReadOnlyEpoch() = 0;

  // Return a timestamp, higher 32 bits are eid and lower 32 bits are tid within epoch
  virtual cid_t EnterEpoch() = 0;

  virtual void ExitReadOnlyEpoch(size_t epoch) = 0;

  virtual void ExitEpoch(size_t epoch) = 0;

  // assume we store epoch_store max_store previously
  virtual size_t GetMaxDeadEid() = 0;

  virtual size_t GetReadonlyEid() = 0;

protected:
  // queue size
  static const size_t epoch_queue_size_ = 4096;
  static const int safety_interval_ = 2;
  static const size_t low_32_bit_mask_ = 0xffffffff;

  const int epoch_duration_milisec_;
};


}
}

