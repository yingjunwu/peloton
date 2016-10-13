//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// snapshot_epoch_manager.h
//
// Identification: src/backend/concurrency/snapshot_epoch_manager.h
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
#include "backend/concurrency/epoch_manager.h"
#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace concurrency {

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

class SnapshotEpochManager : public EpochManager {
  SnapshotEpochManager(const SnapshotEpochManager&) = delete;

  struct Epoch {
    std::atomic<int> txn_ref_count_;
    std::atomic<size_t> id_generator_;

    Epoch()
      :txn_ref_count_(0), id_generator_(0) {}

    void Init() {
      txn_ref_count_ = 0;
      id_generator_ = 0;
    }
  };

public:
  static SnapshotEpochManager& GetInstance(const double epoch_length) {
    static SnapshotEpochManager epochManager(epoch_length);
    return epochManager;
  }

  SnapshotEpochManager(const double epoch_length)
    : EpochManager(epoch_length),
      epoch_queue_(epoch_queue_size_),
      repoch_queue_(epoch_queue_size_) {
    InitEpochQueue();
    StartEpochManager();
  }

  virtual ~SnapshotEpochManager() override {
    finish_ = true;
    if (ts_thread_ != nullptr) {
      ts_thread_->join();
    }
  }

  virtual void StartEpochManager() override {
    finish_ = false;
    ts_thread_.reset(new std::thread(&SnapshotEpochManager::Start, this));
  }

  virtual void Reset() override {
    finish_ = true;
    ts_thread_->join();

    InitEpochQueue();

    finish_ = false;
    ts_thread_.reset(new std::thread(&SnapshotEpochManager::Start, this));
  }


  virtual size_t GetCurrentEpochId() override {
    return current_epoch_id_.load();
  }

  // Get a eid that is larger than all the running transactions
  // TODO: See if we can delete this method
  virtual size_t GetCurrentCid() override {
    size_t eid = current_epoch_id_.load();
    size_t epoch_idx = eid % epoch_queue_size_;

    size_t id = epoch_queue_[epoch_idx].id_generator_++;

    // TODO: Also add validation here
    // No need... this function is used for statistic purpose -- Jiexi
    return (eid << 32) | (id & low_32_bit_mask_);
  }

  virtual cid_t EnterReadOnlyEpoch() override {
    auto repoch = CalaulateCurrentReid();
    size_t repoch_idx = repoch % epoch_queue_size_;

    repoch_queue_[repoch_idx].txn_ref_count_++;

    // Validation
    auto repoch_validate = CalaulateCurrentReid();
    while (repoch != repoch_validate) {
      repoch_queue_[repoch_idx].txn_ref_count_--;
      repoch = repoch_validate;
      repoch_idx = repoch % epoch_queue_size_;
      repoch_queue_[repoch_idx].txn_ref_count_++;
      repoch_validate = CalaulateCurrentReid();
    }

    return GetReadonlyCidFromEid(repoch * ro_epoch_frequency_); // Lower 32 bits are all 1
  }

  // Return a timestamp, higher 32 bits are eid and lower 32 bits are tid within epoch
  virtual cid_t EnterEpoch() override {
    auto epoch = current_epoch_id_.load();

    size_t epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].txn_ref_count_++;

    // Validation
    auto epoch_validate = current_epoch_id_.load();
    while (epoch_validate != epoch) {
      epoch_queue_[epoch_idx].txn_ref_count_--;
      epoch = epoch_validate;
      epoch_idx = epoch % epoch_queue_size_;
      epoch_queue_[epoch_idx].txn_ref_count_++;
      epoch_validate = current_epoch_id_.load();
    }

    auto id = epoch_queue_[epoch_idx].id_generator_++;

    return (epoch << 32) | (id & low_32_bit_mask_);
  }

  virtual void ExitReadOnlyEpoch(size_t epoch) override {
    PL_ASSERT(epoch % ro_epoch_frequency_ == 0);
    PL_ASSERT(epoch <= queue_tail_);
    PL_ASSERT(epoch >= ro_queue_tail_ * ro_epoch_frequency_);

    auto repoch_idx = (epoch / ro_epoch_frequency_) % epoch_queue_size_;
    PL_ASSERT(repoch_queue_[repoch_idx].txn_ref_count_ > 0);
    repoch_queue_[repoch_idx].txn_ref_count_--;
  }

  virtual void ExitEpoch(size_t epoch) override {
    PL_ASSERT(epoch >= queue_tail_);
    PL_ASSERT(epoch <= current_epoch_id_);

    auto epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].txn_ref_count_--;
  }

  // assume we store epoch_store max_store previously
  virtual size_t GetMaxDeadEid() override { return GetMaxDeadEidForRwGC(); }

  size_t GetMaxDeadEidForRwGC() {
    IncreaseQueueTail();
    return queue_tail_.load();
  };

  size_t GetMaxDeadEidForSnapshotGC() {
    // It is unnecessary to also call IncreaseQueueTail() here. -- Jiexi
    IncreaseReadonlyQueueTail();
    return ro_queue_tail_.load() * ro_epoch_frequency_; // Transform from reid to eid
  };

  virtual size_t GetReadonlyEid() override {
    IncreaseQueueTail();
    return CalaulateCurrentReid() * ro_epoch_frequency_;
  }

  // Round the eid down by ro_epoch_frequency
  static size_t GetNearestSnapshotEpochId(size_t eid) {
    return (eid / ro_epoch_frequency_) * ro_epoch_frequency_;
  }

  virtual int GetActiveRwTxnCount(size_t eid) override {
    size_t epoch_idx = eid % epoch_queue_size_;
    return epoch_queue_[epoch_idx].txn_ref_count_.load();
  }

  virtual int GetActiveRoTxnCount(size_t eid) override {
    PL_ASSERT(eid % ro_epoch_frequency_ == 0);
    size_t ro_epoch_idx = (eid / ro_epoch_frequency_) % epoch_queue_size_;
    return repoch_queue_[ro_epoch_idx].txn_ref_count_.load();
  }

private:
  void Start() {
    while (!finish_) {
      // the epoch advances every 40 milliseconds.
      std::this_thread::sleep_for(std::chrono::microseconds(size_t(epoch_duration_millisec_ * 1000)));

      auto next_idx = (current_epoch_id_.load() + 1) % epoch_queue_size_;
      auto tail_idx = queue_tail_.load() % epoch_queue_size_;


      if(next_idx  == tail_idx) {
        // overflow
        // in this case, just increase tail
        IncreaseQueueTail();
        continue;
      }

      // we have to init it first, then increase current epoch
      // otherwise may read dirty data
      epoch_queue_[next_idx].Init();
      current_epoch_id_++;

      // No need to init the read only epoch because:
      //      1) We don't use the id generator field
      //      2) The ref count is naturally 0 when the epoch becomes read only epoch tail

      IncreaseQueueTail();
      IncreaseReadonlyQueueTail();
    }
  }

  inline void InitEpochQueue() {
    for (size_t i = 0; i < epoch_queue_size_; ++i) {
      epoch_queue_[i].Init();
      repoch_queue_[i].Init();
    }

    queue_tail_token_ = true;
    ro_queue_tail_token_ = true;

    // Propely init the significant epochs with safe interval
    ro_queue_tail_ = START_EPOCH_ID;
    queue_tail_ = ro_epoch_frequency_ * (ro_queue_tail_ + 1 + safety_interval_);    // 40
    current_epoch_id_ = queue_tail_ + 1 + safety_interval_; // 41

    // current reid = 1
  }

  void IncreaseQueueTail() {
    // Lock
    bool expect = true, desired = false;
    if(!queue_tail_token_.compare_exchange_weak(expect, desired)){
      // someone now is increasing tail
      return;
    }

    auto current = current_epoch_id_.load();
    auto tail = queue_tail_.load();

    while(true) {
      auto idx = tail % epoch_queue_size_;

      // inc tail until we find an epoch that has running txn
      if(epoch_queue_[idx].txn_ref_count_ > 0) {
        break;
      }

      tail++;

      if(tail + safety_interval_ >= current) {
        // Reset the tail when tail overlaps the head (including the safety interval)
        tail = current - 1 - safety_interval_;
        break;
      }
    }

    queue_tail_ = tail;

    // Unlock
    expect = false;
    desired = true;
    queue_tail_token_.compare_exchange_weak(expect, desired);
    return;
  }


  void IncreaseReadonlyQueueTail() {
    // Lock
    bool expect = true, desired = false;
    if(!ro_queue_tail_token_.compare_exchange_weak(expect, desired)){
      // someone now is increasing tail
      return;
    }

    auto current = CalaulateCurrentReid();
    auto tail = ro_queue_tail_.load();

    while(true) {
      auto idx = tail  % epoch_queue_size_;

      // inc tail until we find an epoch that has running txn
      if(repoch_queue_[idx].txn_ref_count_ > 0) {
        break;
      }

      tail++;

      if(tail + safety_interval_ >= current) {
        // Reset the tail when tail overlaps the head (including the safety interval)
        tail = current - 1 - safety_interval_;
        break;
      }
    }

    ro_queue_tail_ = tail;

    // Unlock
    expect = false;
    desired = true;
    ro_queue_tail_token_.compare_exchange_weak(expect, desired);
    return;
  }

  // reid grows 40 times slower than rw id
  size_t CalaulateCurrentReid() {
    size_t tail_eid = queue_tail_.load();
    return (tail_eid / ro_epoch_frequency_);
  }

private:
  // Queue tail token
  std::atomic<bool> queue_tail_token_;
  std::atomic<bool> ro_queue_tail_token_;

  // Read write epoch vector
  std::vector<Epoch> epoch_queue_;
  std::atomic<size_t> queue_tail_;
  std::atomic<size_t> current_epoch_id_;

  // Read only epoch frequency
  static const int ro_epoch_frequency_ = 20; // TODO: remove this magic number.

  // Read only epoch vector
  std::vector<Epoch> repoch_queue_;
  std::atomic<size_t> ro_queue_tail_;

  bool finish_;
  std::unique_ptr<std::thread> ts_thread_;
};


}
}

