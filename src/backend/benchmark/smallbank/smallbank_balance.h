//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/benchmark_common.h"
#include "backend/benchmark/smallbank/smallbank_loader.h"
#include "backend/benchmark/smallbank/smallbank_configuration.h"
#include "backend/executor/abstract_executor.h"
#include "backend/storage/data_table.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"

namespace peloton {
namespace benchmark {
namespace smallbank {

struct BalancePlans {

  executor::IndexScanExecutor* accounts_index_scan_executor_;
  executor::IndexScanExecutor* savings_index_scan_executor_;
  executor::IndexScanExecutor* checking_index_scan_executor_;

  void SetContext(executor::ExecutorContext* context) {
    accounts_index_scan_executor_->SetContext(context);
    savings_index_scan_executor_->SetContext(context);
    checking_index_scan_executor_->SetContext(context);

  }

  void Cleanup() {

    delete accounts_index_scan_executor_;
    accounts_index_scan_executor_ = nullptr;

    delete savings_index_scan_executor_;
    savings_index_scan_executor_ = nullptr;

    delete checking_index_scan_executor_;
    checking_index_scan_executor_ = nullptr;
  }
};


struct BalanceParams : public TransactionParameter {
  
  int custid;

  virtual void SerializeTo(SerializeOutput &output) override {

    output.WriteLong(custid);
  }

  virtual void DeserializeFrom(SerializeInputBE &input) override {

    custid = input.ReadLong();
  }
};


BalancePlans PrepareBalancePlan();


void GenerateBalanceParams(ZipfDistribution &zipf, BalanceParams &params);

bool RunBalance(BalancePlans &balance_plans, BalanceParams &params, bool is_adhoc = false);

}
}
}  // end namespace peloton
