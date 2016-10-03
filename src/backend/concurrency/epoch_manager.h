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

/*
  Epoch queue layout:
     current epoch               queue tail                reclaim tail
    /                           /                          /
    +--------+--------+--------+--------+--------+--------+--------+-------
    | head   | safety |  ....  |readonly| safety |  ....  |gc usage|  ....
    +--------+--------+--------+--------+--------+--------+--------+-------
    New                                                   Old

  Note:
    1) Queue tail epoch and epochs which is older than it have 0 rw txn ref count
    2) Reclaim tail epoch and epochs which is older than it have 0 ro txn ref count
    3) Reclaim tail is at least 2 turns older than the queue tail epoch
    4) Queue tail is at least 2 turns older than the head epoch
*/

class EpochManager {
  EpochManager(const EpochManager&) = delete;

 public:
  EpochManager(const int epoch_length)
 : epoch_length_(epoch_length),
   epoch_queue_(epoch_queue_size_) {
    
    InitEpochQueue();
    
    finish_ = false;
    ts_thread_ = std::thread(&EpochManager::Start, this);
  }

  ~EpochManager() {
    finish_ = true;
    ts_thread_.join();
  }

  void Reset() {
    finish_ = true;
    ts_thread_.join();

    InitEpochQueue();

    finish_ = false;
    ts_thread_ = std::thread(&EpochManager::Start, this);
  }

  inline void InitEpochQueue() {
    for (int i = 0; i < epoch_length_; ++i) {
      epoch_queue_[i].Init();
    }

    queue_tail_token_ = true;
    reclaim_tail_token_ = true;

    current_epoch_ = START_EPOCH_ID + 2; // 2
    queue_tail_ = START_EPOCH_ID + 1;    // 1
    reclaim_tail_ = START_EPOCH_ID;      // 0
  }

  static size_t GetEpochQueueCapacity() { return epoch_queue_size_; }

  size_t GetCurrentEpoch() {
    return current_epoch_.load();
  }

  // Get a eid that is larger than all the running transactions
  // TODO: See if we can delete this method
  size_t GetSafeMaxCid() {
    size_t eid = current_epoch_.load();
    size_t epoch_idx = eid % epoch_queue_size_;
    size_t id = epoch_queue_[epoch_idx].id_generator_++;

    // TODO: Also add validation here
    return (eid << 32) | (id & low_32_bit_mask_);
  }

  cid_t EnterReadOnlyEpoch() {
    auto epoch = queue_tail_.load();

    size_t epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].ro_txn_ref_count_++;

    // Validation
    auto epoch_validate = queue_tail_.load();
    while (epoch != epoch_validate) {
      epoch_queue_[epoch_idx].ro_txn_ref_count_--;
      epoch = epoch_validate;
      epoch_idx = epoch % epoch_queue_size_;
      epoch_queue_[epoch_idx].ro_txn_ref_count_++;
      epoch_validate = queue_tail_.load();
    }

    return (epoch << 32) | low_32_bit_mask_;
  }

  // Return a timestamp, higher 32 bits are eid and lower 32 bits are tid within epoch
  cid_t EnterEpoch() {
    auto epoch = current_epoch_.load();

    size_t epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].rw_txn_ref_count_++;

    // Validation
    auto epoch_validate = current_epoch_.load();
    while (epoch_validate != epoch) {
      epoch_queue_[epoch_idx].rw_txn_ref_count_--;
      epoch = epoch_validate;
      epoch_idx = epoch % epoch_queue_size_;
      epoch_queue_[epoch_idx].rw_txn_ref_count_++;
      epoch_validate = current_epoch_.load();
    }

    auto id = epoch_queue_[epoch_idx].id_generator_++;

    return (epoch << 32) | (id & low_32_bit_mask_);
  }

  void ExitReadOnlyEpoch(size_t epoch) {
    PL_ASSERT(epoch >= reclaim_tail_);
    PL_ASSERT(epoch <= queue_tail_);

    auto epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].ro_txn_ref_count_--;
  }

  void ExitEpoch(size_t epoch) {
    PL_ASSERT(epoch >= queue_tail_);
    PL_ASSERT(epoch <= current_epoch_);

    auto epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].rw_txn_ref_count_--;
  }

  // assume we store epoch_store max_store previously
  size_t GetMaxDeadEid() {
    IncreaseQueueTail();
    IncreaseReclaimTail();
    return reclaim_tail_.load();
  }

  size_t GetReadonlyEid() {
    IncreaseQueueTail();
    return queue_tail_.load();
  }

  static size_t GetEidFromCid(cid_t cid) {
    return (cid >> 32);
  }

 private:
  void Start() {
    while (!finish_) {
      // the epoch advances every 40 milliseconds.
      std::this_thread::sleep_for(std::chrono::milliseconds(epoch_length_));

      auto next_idx = (current_epoch_.load() + 1) % epoch_queue_size_;
      auto tail_idx = reclaim_tail_.load() % epoch_queue_size_;

//      if (current_epoch_.load() % 10 == 0) {
//        fprintf(stderr, "head = %d, queue_tail = %d, reclaim_tail = %d\n",
//                (int)current_epoch_.load(),
//                (int)queue_tail_.load(),
//                (int)reclaim_tail_.load()
//        );
//        fprintf(stderr, "gc_ts = %d, ro_ts = %d\n", (int)max_cid_gc_, (int)max_cid_ro_);
//      }

      if(next_idx  == tail_idx) {
        // overflow
        // in this case, just increase tail
        IncreaseQueueTail();
        IncreaseReclaimTail();
        continue;
      }

      // we have to init it first, then increase current epoch
      // otherwise may read dirty data
      epoch_queue_[next_idx].Init();
      current_epoch_++;

      IncreaseQueueTail();
      IncreaseReclaimTail();
    }
  }

  void IncreaseReclaimTail() {
    bool expect = true, desired = false;
    if(!reclaim_tail_token_.compare_exchange_weak(expect, desired)){
      // someone now is increasing tail
      return;
    }

    auto current = queue_tail_.load();
    auto tail = reclaim_tail_.load();

    while(true) {
      auto idx = tail % epoch_queue_size_;

      // inc tail until we find an epoch that has running txn
      if(epoch_queue_[idx].ro_txn_ref_count_ > 0) {
        break;
      }

      tail++;

      if(tail + safety_interval_ >= current) {
        tail = current - 1;
        break;
      }
    }

    reclaim_tail_ = tail;

    expect = false;
    desired = true;

    reclaim_tail_token_.compare_exchange_weak(expect, desired);
    return;
  }

  void IncreaseQueueTail() {
    bool expect = true, desired = false;
    if(!queue_tail_token_.compare_exchange_weak(expect, desired)){
      // someone now is increasing tail
      return;
    }

    auto current = current_epoch_.load();
    auto tail = queue_tail_.load();

    while(true) {
      auto idx = tail % epoch_queue_size_;

      // inc tail until we find an epoch that has running txn
      if(epoch_queue_[idx].rw_txn_ref_count_ > 0) {
        break;
      }

      tail++;

      if(tail + safety_interval_ >= current) {
        tail = current - 1;
        break;
      }
    }

    queue_tail_ = tail;

    expect = false;
    desired = true;

    queue_tail_token_.compare_exchange_weak(expect, desired);
    return;
  }

private:
  // queue size
  static const size_t epoch_queue_size_ = 4096;

  static const int safety_interval_ = 2;
  static const size_t low_32_bit_mask_ = 0xffffffff;
  const int epoch_length_;

  // Epoch vector
  std::vector<Epoch> epoch_queue_;
  std::atomic<size_t> queue_tail_;
  std::atomic<size_t> reclaim_tail_;
  std::atomic<size_t> current_epoch_;
  std::atomic<bool> queue_tail_token_;
  std::atomic<bool> reclaim_tail_token_;
  bool finish_;

  std::thread ts_thread_;
};


}
}

