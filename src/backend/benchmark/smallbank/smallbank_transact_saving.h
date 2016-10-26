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

struct TransactSavingPlans {

  executor::IndexScanExecutor* accounts_index_scan_executor_;

  executor::IndexScanExecutor* saving_index_scan_executor_;
  executor::IndexScanExecutor* saving_update_index_scan_executor_;
  executor::UpdateExecutor* saving_update_executor_;


  void SetContext(executor::ExecutorContext* context) {
    accounts_index_scan_executor_->SetContext(context);
    saving_index_scan_executor_->SetContext(context);
    saving_update_index_scan_executor_->SetContext(context);
    saving_update_executor_->SetContext(context);
  }

  virtual void Cleanup() {

    delete accounts_index_scan_executor_;
    accounts_index_scan_executor_ = nullptr;

    delete saving_index_scan_executor_;
    saving_index_scan_executor_ = nullptr;

    delete saving_update_index_scan_executor_;
    saving_update_index_scan_executor_ = nullptr;

    delete saving_update_executor_;
    saving_update_executor_ = nullptr;
  }

};

struct TransactSavingParams : public TransactionParameter {
  
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


TransactSavingPlans PrepareTransactSavingPlan();


void GenerateTransactSavingParams(ZipfDistribution &zipf, TransactSavingParams &params);

bool RunTransactSaving(TransactSavingPlans &transact_saving_plans, TransactSavingParams &params, bool is_adhoc = false);

}
}
}  // end namespace peloton
