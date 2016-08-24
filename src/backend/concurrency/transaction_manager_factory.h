//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager_factory.h
//
// Identification: src/backend/concurrency/transaction_manager_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/optimistic_txn_manager.h"
#include "backend/concurrency/pessimistic_txn_manager.h"
#include "backend/concurrency/speculative_read_txn_manager.h"
#include "backend/concurrency/eager_write_txn_manager.h"
#include "backend/concurrency/ts_order_txn_manager.h"
#include "backend/concurrency/ssi_txn_manager.h"
#include "backend/concurrency/optimistic_rb_txn_manager.h"
#include "backend/concurrency/optimistic_central_rb_txn_manager.h"
#include "backend/concurrency/ts_order_central_rb_txn_manager.h"
#include "backend/concurrency/optimistic_n2o_txn_manager.h"
#include "backend/concurrency/ts_order_rb_txn_manager.h"
#include "backend/concurrency/ts_order_n2o_txn_manager.h"
#include "backend/concurrency/ts_order_full_rb_txn_manager.h"
#include "backend/concurrency/ts_order_full_central_rb_txn_manager.h"
#include "backend/concurrency/ts_order_opt_n2o_txn_manager.h"
#include "backend/concurrency/ts_order_sv_txn_manager.h"
#include "backend/concurrency/ts_order_best_n2o_txn_manager.h"

namespace peloton {
namespace concurrency {
class TransactionManagerFactory {
 public:
  static TransactionManager &GetInstance() {
    switch (protocol_) {
      case CONCURRENCY_TYPE_OPTIMISTIC:
       return OptimisticTxnManager::GetInstance();
      case CONCURRENCY_TYPE_PESSIMISTIC:
       return PessimisticTxnManager::GetInstance();
      case CONCURRENCY_TYPE_SPECULATIVE_READ:
       return SpeculativeReadTxnManager::GetInstance();
      case CONCURRENCY_TYPE_EAGER_WRITE:
       return EagerWriteTxnManager::GetInstance();
      case CONCURRENCY_TYPE_SSI:
       return SsiTxnManager::GetInstance();
      case CONCURRENCY_TYPE_TO:
       return TsOrderTxnManager::GetInstance();
      case CONCURRENCY_TYPE_OCC_RB:
       return OptimisticRbTxnManager::GetInstance();
      case CONCURRENCY_TYPE_OCC_CENTRAL_RB:
       return OptimisticCentralRbTxnManager::GetInstance();
      case CONCURRENCY_TYPE_TO_CENTRAL_RB:
       return TsOrderCentralRbTxnManager::GetInstance();
      case CONCURRENCY_TYPE_OCC_N2O:
       return OptimisticN2OTxnManager::GetInstance();
      case CONCURRENCY_TYPE_TO_RB:
       return TsOrderRbTxnManager::GetInstance();
      case CONCURRENCY_TYPE_TO_N2O:
       return TsOrderN2OTxnManager::GetInstance();
      case CONCURRENCY_TYPE_TO_FULL_RB:
       return TsOrderFullRbTxnManager::GetInstance();
      case CONCURRENCY_TYPE_TO_FULL_CENTRAL_RB:
       return TsOrderFullCentralRbTxnManager::GetInstance();
      case CONCURRENCY_TYPE_TO_OPT_N2O:
       return TsOrderOptN2OTxnManager::GetInstance();
      case CONCURRENCY_TYPE_TO_SV:
       return TsOrderSVTxnManager::GetInstance();
      case CONCURRENCY_TYPE_TO_BEST_N2O:
       return TsOrderBestN2OTxnManager::GetInstance();
      default:
       return OptimisticTxnManager::GetInstance();
    }
  }

  static void Configure(ConcurrencyType protocol,
                        IsolationLevelType level = ISOLATION_LEVEL_TYPE_FULL) {
    protocol_ = protocol;
    isolation_level_ = level;
  }

  static ConcurrencyType GetProtocol() { return protocol_; }

  static IsolationLevelType GetIsolationLevel() { return isolation_level_; }

  static bool IsRB() {
    return protocol_ == CONCURRENCY_TYPE_TO_RB ||
           protocol_ == CONCURRENCY_TYPE_OCC_RB ||
           protocol_ == CONCURRENCY_TYPE_OCC_CENTRAL_RB ||
           protocol_ == CONCURRENCY_TYPE_TO_CENTRAL_RB ||
           protocol_ == CONCURRENCY_TYPE_TO_FULL_RB ||
           protocol_ == CONCURRENCY_TYPE_TO_FULL_CENTRAL_RB;
  }

  static bool IsN2O() {
    return protocol_ == CONCURRENCY_TYPE_OCC_N2O ||
           protocol_ == CONCURRENCY_TYPE_TO_N2O ||
           protocol_ == CONCURRENCY_TYPE_TO_OPT_N2O ||
           protocol_ == CONCURRENCY_TYPE_TO_BEST_N2O;
  }

  static bool IsSV() {
    return protocol_ == CONCURRENCY_TYPE_TO_SV;
  }

  static bool IsOptN2O() {
    return protocol_ == CONCURRENCY_TYPE_TO_OPT_N2O;
  }

 private:
  static ConcurrencyType protocol_;
  static IsolationLevelType isolation_level_;
};
}
}
