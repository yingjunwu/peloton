//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "backend/logging/durability_factory.h"
#include "backend/logging/log_manager.h"

namespace peloton {
namespace logging {

  thread_local WorkerContext* tl_worker_ctx = nullptr;

  void LogManager::StartTxn(concurrency::Transaction *txn) {
    PL_ASSERT(tl_worker_ctx);
    size_t txn_eid = txn->GetEpochId();

    // Record the txn timer
    DurabilityFactory::StartTxnTimer(txn_eid, tl_worker_ctx);

    PL_ASSERT(tl_worker_ctx->current_commit_eid == INVALID_EPOCH_ID || tl_worker_ctx->current_commit_eid <= txn_eid);

    // Handle the epoch id
    if (tl_worker_ctx->current_commit_eid == INVALID_EPOCH_ID
      || tl_worker_ctx->current_commit_eid != txn_eid) {
      // if this is a new epoch, then write to a new buffer
      tl_worker_ctx->current_commit_eid = txn_eid;
      RegisterNewBufferToEpoch(std::move(tl_worker_ctx->buffer_pool.GetBuffer(txn_eid)));
    }

    // Handle the commit id
    cid_t txn_cid = txn->GetEndCommitId();
    tl_worker_ctx->current_cid = txn_cid;
  }

  void LogManager::FinishPendingTxn() {
    PL_ASSERT(tl_worker_ctx);
    size_t glob_peid = global_persist_epoch_id_.load();
    DurabilityFactory::StopTimersByPepoch(glob_peid, tl_worker_ctx);
  }

}
}