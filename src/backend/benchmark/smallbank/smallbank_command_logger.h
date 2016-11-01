//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// command_logger.h
//
// Identification: src/backend/benchmark/smallbank/command_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/logging/command_logger.h"
#include "backend/benchmark/smallbank/smallbank_workload.h"

namespace peloton {

namespace logging {
class CommandLogger;
}

namespace benchmark {
namespace smallbank {

  class SmallbankCommandLogger : public logging::CommandLogger {

  public:
    SmallbankCommandLogger(const size_t &logger_id, const std::string &log_dir) :
      logging::CommandLogger(logger_id, log_dir) {}

    ~SmallbankCommandLogger() {}

  private:
    virtual TransactionParameter* DeserializeParameter(const int transaction_type, CopySerializeInputBE &input) override {
      TransactionParameter * param = nullptr;
      if (transaction_type == SMALLBANK_TRANSACTION_TYPE_AMALGAMATE) {
        param = new AmalgamateParams();
      } else if (transaction_type == SMALLBANK_TRANSACTION_TYPE_BALANCE) {
        param = new BalanceParams();
      } else if (transaction_type == SMALLBANK_TRANSACTION_TYPE_DEPOSIT_CHECKING) {
        param = new DepositCheckingParams();
      } else if (transaction_type == SMALLBANK_TRANSACTION_TYPE_TRANSACT_SAVING) {
        param = new TransactSavingParams();
      } else if (transaction_type == SMALLBANK_TRANSACTION_TYPE_WRITE_CHECK) {
        param = new WriteCheckParams();
      } else {
        LOG_ERROR("wrong transaction type : %d", transaction_type);
      } 
      param->DeserializeFrom(input);
      return param;
    }
  };

}
}
}