//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// localized_snapshot_epoch_manager.h
//
// Identification: src/backend/concurrency/localized_snapshot_epoch_manager.h
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

namespace peloton {
  namespace concurrency {
    extern thread_local size_t lt_txn_worker_id;

  class LocalizedSnapshotEpochManager : public EpochManager {
      LocalizedSnapshotEpochManager(const LocalizedSnapshotEpochManager&) = delete;

      struct EpochContext {
        size_t current_epoch;

        void Reset() {
          current_epoch = MAX_EPOCH_ID;
        }
      };

      union EpochIdCacheLineHolder {
        EpochContext epoch_ctx;
        uint64_t data[8];
        EpochIdCacheLineHolder() {
          memset((void*) data, 0, sizeof(uint64_t) * 8);
        }
      };

    public:
      static LocalizedSnapshotEpochManager& GetInstance(const double epoch_length, size_t max_worker_count) {
        static LocalizedSnapshotEpochManager epoch_manager(epoch_length, max_worker_count);
        return epoch_manager;
      }

      LocalizedSnapshotEpochManager(const double epoch_length, size_t max_worker_count)
        : EpochManager(epoch_length), txnid_generator_(0), worker_current_epoch_ctxs_(max_worker_count),
          ro_worker_current_epoch_ctxs_(max_worker_count), worker_id_generator_(0),
          global_current_epoch_(START_EPOCH_ID * ro_epoch_frequency_ + 1)
        {
          StartEpochManager();
        }

      virtual ~LocalizedSnapshotEpochManager() {
        finish_ = true;
        if (ts_thread_ != nullptr) {
          ts_thread_->join();
        }
      }

      virtual void StartEpochManager() {
        size_t max_worker_count = worker_current_epoch_ctxs_.size();
        for (size_t i = 0; i < max_worker_count; ++i) {
          worker_current_epoch_ctxs_[i].epoch_ctx.Reset();
          ro_worker_current_epoch_ctxs_[i].epoch_ctx.Reset();
        }

        finish_ = false;
        PL_ASSERT(ts_thread_ == nullptr);
        global_current_epoch_ = START_EPOCH_ID;
        ts_thread_.reset(new std::thread(&LocalizedSnapshotEpochManager::Start, this));
      };

      virtual void Reset() {
        finish_ = true;
        if (ts_thread_ != nullptr) {
          ts_thread_->join();
        }
        ts_thread_.reset(nullptr);

        global_current_epoch_ = START_EPOCH_ID;
        StartEpochManager();
      };


      virtual size_t GetCurrentEpochId() {
        return global_current_epoch_.load();
      };

      // Get a eid that is larger than all the running transactions
      // TODO: See if we can delete this method
      virtual size_t GetCurrentCid() {
        return MAX_CID;
      };

      virtual cid_t EnterReadOnlyEpoch() {
        PL_ASSERT(lt_txn_worker_id != INVALID_TXN_WORKER_ID);
        size_t current_eid = global_current_epoch_.load();
        size_t eid = GetNearestSnapshotEpochId(current_eid);
        ro_worker_current_epoch_ctxs_[lt_txn_worker_id].epoch_ctx.current_epoch = eid;
        // readonly txn cid's lower 32bits are all 0
        return (eid << 32) | low_32_bit_mask_;
      };

      // Return a timestamp, higher 32 bits are eid and lower 32 bits are tid within epoch
      virtual cid_t EnterEpoch() {
        PL_ASSERT(lt_txn_worker_id != INVALID_TXN_WORKER_ID);
        size_t eid = GetCurrentEpochId();
        worker_current_epoch_ctxs_[lt_txn_worker_id].epoch_ctx.current_epoch = eid;
        uint32_t txn_id = txnid_generator_++;
        return (eid << 32) | txn_id;
      };

      virtual void ExitReadOnlyEpoch(UNUSED_ATTRIBUTE size_t epoch) {
        PL_ASSERT(lt_txn_worker_id != INVALID_TXN_WORKER_ID);
        ro_worker_current_epoch_ctxs_[lt_txn_worker_id].epoch_ctx.Reset();
      };

      virtual void ExitEpoch(UNUSED_ATTRIBUTE size_t epoch) {
        PL_ASSERT(lt_txn_worker_id != INVALID_TXN_WORKER_ID);
        worker_current_epoch_ctxs_[lt_txn_worker_id].epoch_ctx.Reset();
      };

      // assume we store epoch_store max_store previously
      virtual size_t GetMaxDeadEid() {
        return GetMaxDeadEidForRwGC();
      };

      virtual void RegisterTxnWorker(bool read_only) {
        if (read_only == false) {
          // ro txn
          lt_txn_worker_id = worker_id_generator_++;
          ro_worker_current_epoch_ctxs_[lt_txn_worker_id].epoch_ctx.Reset();
        } else {
          // rw txn
          lt_txn_worker_id = worker_id_generator_++;
          worker_current_epoch_ctxs_[lt_txn_worker_id].epoch_ctx.Reset();
        }
      }

    virtual size_t GetRwTxnWorkerCurrentEid(size_t txn_worker_id) override {
      return worker_current_epoch_ctxs_[txn_worker_id].epoch_ctx.current_epoch;
    }

    size_t GetMaxDeadEidForRwGC() {
      size_t cur_eid = global_current_epoch_.load();
      COMPILER_MEMORY_FENCE;
      size_t min_eid = GetMin(worker_current_epoch_ctxs_);
      return std::min(cur_eid, min_eid) - 1;
    };

    size_t GetMaxDeadEidForSnapshotGC() {
      // It is unnecessary to also call IncreaseQueueTail() here. -- Jiexi
      size_t cur_eid = global_current_epoch_.load();
      size_t cur_ro_eid = GetNearestSnapshotEpochId(cur_eid);
      COMPILER_MEMORY_FENCE;
      size_t min_ro_eid = GetMin(ro_worker_current_epoch_ctxs_);
      return std::min(cur_ro_eid, min_ro_eid) - 1;
    };


    // Round the eid down by ro_epoch_frequency
    static size_t GetNearestSnapshotEpochId(size_t eid) {
      return (eid / ro_epoch_frequency_) * ro_epoch_frequency_;
    }

  private:
      void Start() {
        while (finish_ == false) {
          std::this_thread::sleep_for(std::chrono::microseconds(size_t(epoch_duration_millisec_ * 1000)));

          // Check we don't overflow
          size_t tail = GetMaxDeadEid();
          size_t head = global_current_epoch_.load();

          if (head - tail >= GetEpochQueueCapacity()) {
            LOG_ERROR("Epoch queue over flow");
          } else {
            global_current_epoch_++;
          }
        }
      }

      size_t GetMin(std::vector<EpochIdCacheLineHolder> &ctxs) {
        size_t worker_count = worker_id_generator_.load();
        size_t min_eid = MAX_EPOCH_ID;
        for (size_t i = 0; i < worker_count; ++i) {
          min_eid = std::min(ctxs[i].epoch_ctx.current_epoch, min_eid);
        }
        return min_eid;
      }

    private:
      // Read only epoch frequency
      static const int ro_epoch_frequency_ = 20; // TODO: remove this magic number.

      std::atomic<uint32_t> txnid_generator_;

      std::vector<EpochIdCacheLineHolder> worker_current_epoch_ctxs_;
      std::vector<EpochIdCacheLineHolder> ro_worker_current_epoch_ctxs_;
      std::atomic<size_t> worker_id_generator_;
      std::atomic<size_t> global_current_epoch_;
      bool finish_;
      std::unique_ptr<std::thread> ts_thread_;
    };


  }
}

