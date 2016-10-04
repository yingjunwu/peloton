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

#include "durability_factory.h"

namespace peloton {
namespace logging {

  LoggingType DurabilityFactory::logging_type_ = LOGGING_TYPE_INVALID;

  CheckpointType DurabilityFactory::checkpoint_type_ = CHECKPOINT_TYPE_INVALID;

  bool DurabilityFactory::timer_flag = false;


  void DurabilityFactory::StartTxnTimer(size_t eid, WorkerLogContext *worker_ctx) {
    if (DurabilityFactory::GetTimerFlag() == false) return;

    uint64_t cur_time_usec = GetCurrentTimeInUsec();

    auto itr = worker_ctx->pending_txn_timers.find(eid);
    if (itr == worker_ctx->pending_txn_timers.end()) {
      itr = (worker_ctx->pending_txn_timers.emplace(eid, std::vector<uint64_t>())).first;
    }
    itr->second.emplace_back(cur_time_usec);
  }

  void DurabilityFactory::StopTimersByPepoch(size_t persist_eid, WorkerLogContext *worker_ctx) {
    if (DurabilityFactory::GetTimerFlag() == false) return;

    uint64_t commit_time_usec = GetCurrentTimeInUsec();
    auto upper_itr = worker_ctx->pending_txn_timers.upper_bound(persist_eid);
    auto itr = worker_ctx->pending_txn_timers.begin();

    while (itr != upper_itr) {
      for (uint64_t txn_start_us : itr->second) {
        worker_ctx->txn_summary.AddTxnLatReport(commit_time_usec - txn_start_us);
      }
      itr = worker_ctx->pending_txn_timers.erase(itr);
    }
  }
}
}
