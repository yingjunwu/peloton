//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// single_queue_epoch_manager.h
//
// Identification: src/backend/concurrency/single_queue_epoch_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <thread>
#include <vector>
#include <unordered_set>

#include "backend/common/macros.h"
#include "backend/common/types.h"
#include "backend/common/platform.h"
#include "backend/concurrency/epoch_manager.h"

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

class SingleQueueEpochManager : public EpochManager {
  SingleQueueEpochManager(const SingleQueueEpochManager&) = delete;

  struct Epoch {
    std::atomic<int> ro_txn_ref_count_;
    std::atomic<int> rw_txn_ref_count_;
    std::atomic<size_t> id_generator_;

    Spinlock spinlock_;
    std::unordered_set<size_t> dependency_set_;

    void InsertDependency(const size_t &epoch_id) {
      spinlock_.Lock();

      dependency_set_.insert(epoch_id);

      spinlock_.Unlock();
    }

    Epoch()
      :ro_txn_ref_count_(0), rw_txn_ref_count_(0), id_generator_(0) {}

    void Init() {
      ro_txn_ref_count_ = 0;
      rw_txn_ref_count_ = 0;
      id_generator_ = 0;

      dependency_set_.clear();
    }
  };

public:
  static SingleQueueEpochManager& GetInstance(const double epoch_length) {
    static SingleQueueEpochManager epochManager(epoch_length);
    return epochManager;
  }

  SingleQueueEpochManager(const double epoch_length)
    : EpochManager(epoch_length),
      epoch_queue_(epoch_queue_size_) {
    InitEpochQueue();
    StartEpochManager();
  }

  virtual ~SingleQueueEpochManager() override {
    finish_ = true;
    if (ts_thread_ != nullptr) {
      ts_thread_->join();
    }
  }

  ///////////////////////////////////////////////
  // DEPENDENCY-RELATED OPERATIONS
  void RegisterEpochDependency(const size_t &epoch_id) override {
    size_t eid = current_epoch_id_.load();
    size_t epoch_idx = eid % epoch_queue_size_;

    epoch_queue_[epoch_idx].InsertDependency(epoch_id);
  }



  ///////////////////////////////////////////////
  

  virtual void StartEpochManager() override {
    finish_ = false;
    ts_thread_.reset(new std::thread(&SingleQueueEpochManager::Start, this));
  }

  virtual void Reset() override {
    finish_ = true;
    ts_thread_->join();

    InitEpochQueue();

    finish_ = false;
    ts_thread_.reset(new std::thread(&SingleQueueEpochManager::Start, this));
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

    return GetReadonlyCidFromEid(epoch);
  }

  // Return a timestamp, higher 32 bits are eid and lower 32 bits are tid within epoch
  virtual cid_t EnterEpoch() override {
    auto epoch = current_epoch_id_.load();

    size_t epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].rw_txn_ref_count_++;

    // Validation
    auto epoch_validate = current_epoch_id_.load();
    while (epoch_validate != epoch) {
      epoch_queue_[epoch_idx].rw_txn_ref_count_--;
      epoch = epoch_validate;
      epoch_idx = epoch % epoch_queue_size_;
      epoch_queue_[epoch_idx].rw_txn_ref_count_++;
      epoch_validate = current_epoch_id_.load();
    }

    auto id = epoch_queue_[epoch_idx].id_generator_++;

    return (epoch << 32) | (id & low_32_bit_mask_);
  }

  virtual void ExitReadOnlyEpoch(size_t epoch) override {
    PL_ASSERT(epoch >= reclaim_tail_);
    PL_ASSERT(epoch <= queue_tail_);

    auto epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].ro_txn_ref_count_--;
  }

  virtual void ExitEpoch(size_t epoch) override {
    PL_ASSERT(epoch >= queue_tail_);
    PL_ASSERT(epoch <= current_epoch_id_);

    auto epoch_idx = epoch % epoch_queue_size_;
    epoch_queue_[epoch_idx].rw_txn_ref_count_--;
  }

  // assume we store epoch_store max_store previously
  virtual size_t GetMaxDeadEid() override {
    IncreaseQueueTail();
    IncreaseReclaimTail();
    return reclaim_tail_.load();
  }

  virtual size_t GetReadonlyEid() override {
    IncreaseQueueTail();
    return queue_tail_.load();
  }


private:
  void Start() {
    while (!finish_) {
      // the epoch advances every 40 milliseconds.
      std::this_thread::sleep_for(std::chrono::microseconds(size_t(epoch_duration_millisec_ * 1000)));

      auto next_idx = (current_epoch_id_.load() + 1) % epoch_queue_size_;
      auto tail_idx = reclaim_tail_.load() % epoch_queue_size_;


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
      current_epoch_id_++;

      IncreaseQueueTail();
      IncreaseReclaimTail();
    }
  }

  inline void InitEpochQueue() {
    for (size_t i = 0; i < epoch_queue_size_; ++i) {
      epoch_queue_[i].Init();
    }

    queue_tail_token_ = true;
    reclaim_tail_token_ = true;

    // Propely init the significant epochs with safe interval
    reclaim_tail_ = START_EPOCH_ID;
    queue_tail_ = reclaim_tail_ + 1 + safety_interval_;
    current_epoch_id_ = queue_tail_ + 1 + safety_interval_;
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
        // Reset the tail when tail overlaps the head (including the safety interval)
        tail = current - 1 - safety_interval_;
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

    auto current = current_epoch_id_.load();
    auto tail = queue_tail_.load();

    while(true) {
      auto idx = tail % epoch_queue_size_;

      // inc tail until we find an epoch that has running txn
      if(epoch_queue_[idx].rw_txn_ref_count_ > 0) {
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

    expect = false;
    desired = true;

    queue_tail_token_.compare_exchange_weak(expect, desired);
    return;
  }

private:

  // Epoch vector
  std::vector<Epoch> epoch_queue_;
  std::atomic<size_t> queue_tail_;
  std::atomic<size_t> reclaim_tail_;
  std::atomic<size_t> current_epoch_id_;
  std::atomic<bool> queue_tail_token_;
  std::atomic<bool> reclaim_tail_token_;
  bool finish_;

  std::unique_ptr<std::thread> ts_thread_;
};


}
}

