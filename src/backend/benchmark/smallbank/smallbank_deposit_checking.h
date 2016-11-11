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

struct DepositCheckingPlans {

  executor::IndexScanExecutor* accounts_index_scan_executor_;
  executor::IndexScanExecutor* checking_index_scan_executor_;

  executor::IndexScanExecutor* checking_update_index_scan_executor_;
  executor::UpdateExecutor* checking_update_executor_;

  void SetContext(executor::ExecutorContext* context) {
    accounts_index_scan_executor_->SetContext(context);
    checking_index_scan_executor_->SetContext(context);
    checking_update_index_scan_executor_->SetContext(context);
    checking_update_executor_->SetContext(context);
  }

  virtual void Cleanup() {

    delete accounts_index_scan_executor_;
    accounts_index_scan_executor_ = nullptr;

    delete checking_index_scan_executor_;
    checking_index_scan_executor_ = nullptr;

    delete checking_update_index_scan_executor_;
    checking_update_index_scan_executor_ = nullptr;

    delete checking_update_executor_;
    checking_update_executor_ = nullptr;
  }

};

struct DepositCheckingParams : public TransactionParameter {
  
  int custid;

  int increase;

  virtual void SerializeTo(SerializeOutput &output) override {

    output.WriteLong(custid);
    output.WriteLong(increase);
  }

  virtual void DeserializeFrom(SerializeInputBE &input) override {

    custid = input.ReadLong();
    increase = input.ReadLong();
  }
};

DepositCheckingPlans PrepareDepositCheckingPlan();


void GenerateDepositCheckingParams(ZipfDistribution &zipf, DepositCheckingParams &params);

bool RunDepositChecking(DepositCheckingPlans &deposit_checking_plans, DepositCheckingParams &params, bool is_adhoc = false);

}
}
}  // end namespace peloton