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
#include "backend/concurrency/transaction_scheduler.h"

namespace peloton {
namespace benchmark {
namespace smallbank {

extern configuration state;
extern int RUNNING_REF_THRESHOLD;

class TransactSaving : public concurrency::TransactionQuery {
 public:
  TransactSaving()
      : accounts_index_scan_executor_(nullptr),
        saving_index_scan_executor_(nullptr),
        saving_update_index_scan_executor_(nullptr),
        saving_update_executor_(nullptr),
        context_(nullptr),
        start_time_(std::chrono::system_clock::now()),
        exe_start_time_(std::chrono::system_clock::now()),
        first_pop_(true),
        first_pop_exe_time_(true),
        custid_(0),
        queue_(-1) {}

  ~TransactSaving() {}

  void SetContext(executor::ExecutorContext* context) {
    accounts_index_scan_executor_->SetContext(context);
    saving_index_scan_executor_->SetContext(context);
    saving_update_index_scan_executor_->SetContext(context);
    saving_update_executor_->SetContext(context);

    context_ = context;
  }

  virtual void Cleanup() {

    // Note: context is set in RunNewOrder, and it is unique_prt
    // delete context_;
    // context_ = nullptr;

    delete accounts_index_scan_executor_;
    accounts_index_scan_executor_ = nullptr;

    delete saving_index_scan_executor_;
    saving_index_scan_executor_ = nullptr;

    delete saving_update_index_scan_executor_;
    saving_update_index_scan_executor_ = nullptr;

    delete saving_update_executor_;
    saving_update_executor_ = nullptr;
  }

  void SetValue(ZipfDistribution& zipf);

  // Run txn
  virtual bool Run();

  virtual void SetStartTime(
      std::chrono::system_clock::time_point& delay_start_time) {
    if (first_pop_ == true) {
      start_time_ = delay_start_time;
      first_pop_ = false;
    }
  }

  std::chrono::system_clock::time_point& GetStartTime() {
    return start_time_;
  };

  virtual void SetExeStartTime(
      std::chrono::system_clock::time_point& delay_start_time) {
    if (first_pop_exe_time_ == true) {
      exe_start_time_ = delay_start_time;
      first_pop_exe_time_ = false;
    }
  }

  std::chrono::system_clock::time_point& GetExeStartTime() {
    return exe_start_time_;
  };

  // TODO: just passing the compile
  virtual const std::vector<Value>& GetCompareKeys() const {
    return accounts_index_scan_executor_->GetValues();
  }

  // Common method
  virtual TxnType GetTxnType() {
    return TXN_TYPE_TRANSACT_SAVING;
  };

  virtual std::vector<uint64_t>& GetPrimaryKeysByint() { return primary_keys_; }
  virtual int GetPrimaryKey() { return custid_; }

  // Common method
  virtual peloton::PlanNodeType GetPlanType() {
    return peloton::PLAN_NODE_TYPE_UPDATE;
  };

  // According the New-Order predicate, transform them into a region
  // New-Order predicate have two UPDATE types. In this experiment
  // we only consider one UPDATE (STOCK table update). It contains
  // two columns W_ID and I_ID. W_ID's range is from [1, state.warehouse_count]
  // and I_ID's range is from [1, state.item_count].
  // Note: this is new a Region which is different from the GetRegion();
  virtual SingleRegion* RegionTransform() {
    // Generate region and return
    // std::shared_ptr<Region> region(new Region(cover));
    return new SingleRegion();
  }

  // According predicate (WID AND IID), set the region cover(vector) for this
  // txn
  void SetRegionCover() {
    // Set region
  }

  virtual SingleRegion& GetRegion() { return region_; }

  /*
     "Transact Saving": {
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?" # id 0

        "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
        " WHERE custid = ?" # id 0

        "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS +
        "   SET bal = bal + ? " +
        " WHERE custid = ?" # id 0
      }
   */

  // Increase the counter when conflict
  virtual void UpdateLogTableSingleRef(bool canonical __attribute__((unused))) {

    std::string key;

    // update it in Log Table
    key = std::string("CUSTID") + "-" + std::to_string(custid_);
    for (int i = 0; i < 3; i++) {
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
    }
  }

  virtual void UpdateLogTable(bool single_ref, bool canonical) {
    if (single_ref) {
      UpdateLogTableSingleRef(canonical);
      return;
    }
  }

  // Find out the max conflict condition and return the thread executing this
  // condition. If there are multiple threads executing this condition, choose
  // the thread who has the most of this condition
  virtual int LookupRunTableMaxSingleRef(bool canonical
                                         __attribute__((unused))) {
    int max_conflict = CONFLICT_THRESHHOLD;
    std::string max_conflict_key;
    std::string key;
    std::map<std::string, int> key_counter;

    //////////////////////////////////////////////////////////////////////
    // id 0
    //////////////////////////////////////////////////////////////////////
    key = std::string("CUSTID") + "-" + std::to_string(custid_);

    // Get conflict from Log Table for the given condition
    int conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    for (int i = 0; i < 3; i++) {
      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }
    }

    // If there is no conflict, return -1;
    if (max_conflict == CONFLICT_THRESHHOLD) {
      // std::cout << "Not find any conflict in Log Table" << std::endl;
      // return -1;
      max_conflict_key =
          std::string("CUSTID") + "-" + std::to_string(GetPrimaryKey());
    }

    // Now we get the key with max conflict, such as S_W_ID
    // Then we should lookup Run Table to get the thread who has this key
    // Each queue: <queueNo. reference>
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(
            max_conflict_key);

    int max_reference = RUNNING_REF_THRESHOLD;
    int queue_no = -1;

    // select max reference
    if (queue_info != nullptr) {
      std::vector<int> queues;

      for (auto queue : (*queue_info)) {

        // reference = 0 means there is txn (of this condition) executing
        if (queue.second > max_reference) {
          // Get the queue No.
          queue_no = queue.first;
          max_reference = queue.second;

          // Once find out new max, clear vector
          queues.clear();

          // Put the new number in the queues
          queues.push_back(queue.first);
        } else if (queue.second != 0 && queue.second == max_reference) {
          queues.push_back(queue.first);
        }
      }

      if (queues.size() > 0) {
        std::srand(unsigned(std::time(0)));
        int random_variable = std::rand() % queues.size();
        queue_no = queues.at(random_variable);
      }
    }

    return queue_no;
  }

  virtual int LookupRunTableMax(bool single_ref __attribute__((unused)),
                                bool canonical) {
    return LookupRunTableMaxSingleRef(canonical);
  }

  // Return a queue to schedule
  virtual int LookupRunTableSingleRef(bool canonical __attribute__((unused))) {
    int queue_count =
        concurrency::TransactionScheduler::GetInstance().GetQueueCount();

    std::vector<int> queue_map(queue_count, 0);
    int max_conflict = CONFLICT_THRESHHOLD;
    int return_queue = -1;
    std::string key;

    //////////////////////////////////////////////////////////////////////
    // id 0
    //////////////////////////////////////////////////////////////////////
    key = std::string("CUSTID") + "-" + std::to_string(custid_);

    // Get conflict from Log Table for the given condition
    int conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    // wid-3-->(3,100)(5,99)
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);

    for (int i = 0; i < 3; i++) {
      if (queue_info != nullptr) {
        for (auto queue : (*queue_info)) {

          // reference = 0 means there is txn (of this condition) executing
          if (queue.second > 0) {
            // Get the queue No.
            int queue_no = queue.first;

            // accumulate the conflict for this queue
            queue_map[queue_no] += conflict;

            // Get the latest conflict
            int queue_conflict = queue_map[queue_no];

            // Compare with the max, if current queue has larger conflict
            if (queue_conflict >= max_conflict) {
              return_queue = queue_no;
              max_conflict = queue_conflict;
            }
          }
        }
      }
    }
    return return_queue;
  }

  virtual int LookupRunTable(bool single_ref __attribute__((unused)),
                             bool canonical) {
    return LookupRunTableSingleRef(canonical);
  }

  // Increase each condition with the queue/thread. When a txn completes, it
  // will decrease the reference
  virtual void UpdateRunTableSingleRef(int queue_no,
                                       bool canonical __attribute__((unused))) {
    //////////////////////////////////////////////////////////////////////
    // id 0
    //////////////////////////////////////////////////////////////////////
    std::string key = std::string("CUSTID") + "-" + std::to_string(custid_);

    // update run table
    for (int i = 0; i < 3; i++) {
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
    }
  }

  virtual void UpdateRunTable(int queue_no,
                              bool single_ref __attribute__((unused)),
                              bool canonical) {
    UpdateRunTableSingleRef(queue_no, canonical);
    return;
  }

  // Increase each condition with the queue/thread. When a txn completes, it
  // will decrease the reference
  virtual void DecreaseRunTableSingleRef(bool canonical
                                         __attribute__((unused))) {
    int queue_no = GetQueueNo();

    //////////////////////////////////////////////////////////////////////
    // id 0
    //////////////////////////////////////////////////////////////////////
    std::string key = std::string("CUSTID") + "-" + std::to_string(custid_);

    // update run table
    for (int i = 0; i < 3; i++) {
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
    }
  }

  virtual void DecreaseRunTable(bool single_ref __attribute__((unused)),
                                bool canonical) {

    DecreaseRunTableSingleRef(canonical);
    return;
  }

  virtual bool ExistInRunTable(int queue) {

    //////////////////////////////////////////////////////////////////////
    // D_W_ID
    //////////////////////////////////////////////////////////////////////
    std::string key = std::string("CUSTID") + "-" + std::to_string(custid_);

    return concurrency::TransactionScheduler::GetInstance().ExistInRunTable(
        key, queue);
  }

  // For queue No.
  virtual void SetQueueNo(int queue_no) { queue_ = queue_no; }
  virtual int GetQueueNo() { return queue_; }

  // For fraction run
  virtual void UpdateLogTableFullConflict(bool single_ref
                                          __attribute__((unused)),
                                          bool canonical
                                          __attribute__((unused))) {}
  virtual void UpdateLogTableFullSuccess(bool single_ref
                                         __attribute__((unused)),
                                         bool canonical
                                         __attribute__((unused))) {}
  virtual int LookupRunTableFull(bool single_ref __attribute__((unused)),
                                 bool canonical __attribute__((unused))) {
    return 0;
  }
  virtual int LookupRunTableMaxFull(bool single_ref __attribute__((unused)),
                                    bool canonical __attribute__((unused))) {
    return 0;
  }

  // Make them public for convenience
 public:
  executor::IndexScanExecutor* accounts_index_scan_executor_;

  executor::IndexScanExecutor* saving_index_scan_executor_;
  executor::IndexScanExecutor* saving_update_index_scan_executor_;
  executor::UpdateExecutor* saving_update_executor_;

  executor::ExecutorContext* context_;

  std::chrono::system_clock::time_point start_time_;
  std::chrono::system_clock::time_point exe_start_time_;

  // Flag to compute the execution time
  bool first_pop_;
  bool first_pop_exe_time_;

  // uint64_t primary_key_;
  std::vector<uint64_t> primary_keys_;

  // For execute
  int custid_;

  SingleRegion region_;

  // For queue No.
  int queue_;
};

TransactSaving* GenerateTransactSaving(ZipfDistribution& zipf);
}
}
}  // end namespace peloton
