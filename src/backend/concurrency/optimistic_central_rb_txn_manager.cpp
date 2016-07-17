

#include "optimistic_central_rb_txn_manager.h"

#include "backend/common/platform.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/concurrency/transaction.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"

namespace peloton {

namespace concurrency {

OptimisticCentralRbTxnManager &OptimisticCentralRbTxnManager::GetInstance() {
  static OptimisticCentralRbTxnManager txn_manager;
  return txn_manager;
}

}  // End storage namespace
}  // End peloton namespace
