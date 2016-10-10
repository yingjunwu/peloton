//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// durability_factory.cpp
//
// Identification: src/backend/logging/durability_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/logging/durability_factory.h"
#include "backend/logging/phylog_worker_context.h"

namespace peloton {
namespace logging {

  LoggingType DurabilityFactory::logging_type_ = LOGGING_TYPE_INVALID;

  CheckpointType DurabilityFactory::checkpoint_type_ = CHECKPOINT_TYPE_INVALID;

  TimerType DurabilityFactory::timer_type_ = TIMER_OFF;


  void DurabilityFactory::StartTxnTimer(size_t eid, PhylogWorkerContext *worker_ctx) {
    if (DurabilityFactory::GetTimerType() == TIMER_OFF) return;

    uint64_t cur_time_usec = GetCurrentTimeInUsec();

    auto itr = worker_ctx->pending_txn_timers.find(eid);
    if (itr == worker_ctx->pending_txn_timers.end()) {
      itr = (worker_ctx->pending_txn_timers.emplace(eid, std::vector<uint64_t>())).first;
    }
    itr->second.emplace_back(cur_time_usec);
  }

  void DurabilityFactory::StopTimersByPepoch(size_t persist_eid, PhylogWorkerContext *worker_ctx) {
    if (DurabilityFactory::GetTimerType() == TIMER_OFF) return;

    uint64_t commit_time_usec = GetCurrentTimeInUsec();
    auto upper_itr = worker_ctx->pending_txn_timers.upper_bound(persist_eid);
    auto itr = worker_ctx->pending_txn_timers.begin();

    while (itr != upper_itr) {
      for (uint64_t txn_start_us : itr->second) {
        // printf("delta = %d\n", (int)(commit_time_usec - txn_start_us));
        worker_ctx->txn_summary.AddTxnLatReport(commit_time_usec - txn_start_us, (timer_type_ == TIMER_DISTRIBUTION));
      }
      itr = worker_ctx->pending_txn_timers.erase(itr);
    }
  }



  void DurabilityFactory::StartTxnTimer(size_t eid, EpochWorkerContext *worker_ctx) {
    if (DurabilityFactory::GetTimerType() == TIMER_OFF) return;

    uint64_t cur_time_usec = GetCurrentTimeInUsec();

    auto itr = worker_ctx->pending_txn_timers.find(eid);
    if (itr == worker_ctx->pending_txn_timers.end()) {
      itr = (worker_ctx->pending_txn_timers.emplace(eid, std::vector<uint64_t>())).first;
    }
    itr->second.emplace_back(cur_time_usec);
  }

  void DurabilityFactory::StopTimersByPepoch(size_t persist_eid, EpochWorkerContext *worker_ctx) {
    if (DurabilityFactory::GetTimerType() == TIMER_OFF) return;

    uint64_t commit_time_usec = GetCurrentTimeInUsec();
    auto upper_itr = worker_ctx->pending_txn_timers.upper_bound(persist_eid);
    auto itr = worker_ctx->pending_txn_timers.begin();

    while (itr != upper_itr) {
      for (uint64_t txn_start_us : itr->second) {
        // printf("delta = %d\n", (int)(commit_time_usec - txn_start_us));
        worker_ctx->txn_summary.AddTxnLatReport(commit_time_usec - txn_start_us, (timer_type_ == TIMER_DISTRIBUTION));
      }
      itr = worker_ctx->pending_txn_timers.erase(itr);
    }
  }

}
}
