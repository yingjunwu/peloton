//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// command_logger.h
//
// Identification: src/backend/benchmark/tpcc/command_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/logging/command_logger.h"
#include "backend/benchmark/tpcc/tpcc_workload.h"

namespace peloton {

namespace logging {
class CommandLogger;
}

namespace benchmark {
namespace tpcc {

  class TpccCommandLogger : public logging::CommandLogger {

  public:
    TpccCommandLogger(const size_t &logger_id, const std::string &log_dir) :
      logging::CommandLogger(logger_id, log_dir) {}

    ~TpccCommandLogger() {}

  private:
    virtual TransactionParameter* DeserializeParameter(const int transaction_type, CopySerializeInputBE &input) override {
      TransactionParameter * param = nullptr;
      if (transaction_type == TPCC_TRANSACTION_TYPE_DELIVERY) {
        param = new DeliveryParams();
      } else if (transaction_type == TPCC_TRANSACTION_TYPE_NEW_ORDER) {
        param = new NewOrderParams();
      } else if (transaction_type == TPCC_TRANSACTION_TYPE_PAYMENT) {
        param = new PaymentParams();
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