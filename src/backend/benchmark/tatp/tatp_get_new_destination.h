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
#include "backend/benchmark/tatp/tatp_loader.h"
#include "backend/benchmark/tatp/tatp_configuration.h"
#include "backend/executor/abstract_executor.h"
#include "backend/storage/data_table.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/concurrency/transaction_scheduler.h"

namespace peloton {
namespace benchmark {
namespace tatp {

extern configuration state;
extern int RUNNING_REF_THRESHOLD;

class GetNewDestination : public concurrency::TransactionQuery {
 public:
  GetNewDestination()
      : spe_index_scan_executor_(nullptr),
        call_index_scan_executor_(nullptr),
        context_(nullptr),
        start_time_(std::chrono::system_clock::now()),
        exe_start_time_(std::chrono::system_clock::now()),
        first_pop_(true),
        first_pop_exe_time_(true),
        sid_(0),
        sf_type_(-1),
        start_time_1_(-1),
        end_time_1_(-1),
        is_active_(-1),
        queue_(-1) {}

  ~GetNewDestination() {}

  void SetContext(executor::ExecutorContext* context) {
    spe_index_scan_executor_->SetContext(context);
    call_index_scan_executor_->SetContext(context);

    context_ = context;
  }

  virtual void Cleanup() {

    // Note: context is set in RunNewOrder, and it is unique_prt
    // delete context_;
    // context_ = nullptr;

    delete spe_index_scan_executor_;
    spe_index_scan_executor_ = nullptr;

    delete call_index_scan_executor_;
    call_index_scan_executor_ = nullptr;
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

  virtual std::chrono::system_clock::time_point& GetExeStartTime() {
    return exe_start_time_;
  };

  virtual void SetExeStartTime(
      std::chrono::system_clock::time_point& delay_start_time) {
    if (first_pop_exe_time_ == true) {
      exe_start_time_ = delay_start_time;
      first_pop_exe_time_ = false;
    }
  }

  // TODO: just passing the compile
  virtual const std::vector<Value>& GetCompareKeys() const {
    return spe_index_scan_executor_->GetValues();
  }

  // Common method
  virtual TxnType GetTxnType() {
    return TXN_TYPE_GET_NEW_DESTINATION;
  };

  virtual std::vector<uint64_t>& GetPrimaryKeysByint() { return primary_keys_; }
  virtual int GetPrimaryKey() { return sid_; }

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
         "SELECT cf.numberx " +
         "  FROM " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " sf, " +
         "       " + TATPConstants.TABLENAME_CALL_FORWARDING + " cf " +
         " WHERE sf.s_id = ? " +
         "   AND sf.sf_type = ? " +
         "   AND sf.is_active = 1 " +
         "   AND cf.s_id = sf.s_id " +
         "   AND cf.sf_type = sf.sf_type " +
         "   AND cf.start_time <= ? " +
         "   AND cf.end_time > ?"
  */

  // Increase the counter when conflict
  virtual void UpdateLogTableSingleRef(bool canonical __attribute__((unused))) {

    std::string key;

    // update it in Log Table
    for (int i = 0; i < 2; i++) {
      key = std::string("S_ID") + "-" + std::to_string(sid_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
    }

    for (int i = 0; i < 2; i++) {
      key = std::string("SF_TYPE") + "-" + std::to_string(sf_type_);
      concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
    }

    key = std::string("START_TIME") + "-" + std::to_string(start_time_1_);
    concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

    key = std::string("END_TIME") + "-" + std::to_string(end_time_1_);
    concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);

    key = std::string("IS_ACTIVE") + "-" + std::to_string(is_active_);
    concurrency::TransactionScheduler::GetInstance().LogTableIncrease(key);
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
    // IS_ACTIVE
    //////////////////////////////////////////////////////////////////////
    key = std::string("IS_ACTIVE") + "-" + std::to_string(is_active_);

    // Get conflict from Log Table for the given condition
    int conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    key_counter[key] += conflict;

    if (key_counter[key] > max_conflict) {
      max_conflict = key_counter[key];
      max_conflict_key = key;
    }

    //////////////////////////////////////////////////////////////////////
    // S_ID
    //////////////////////////////////////////////////////////////////////
    key = std::string("S_ID") + "-" + std::to_string(sid_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    for (int i = 0; i < 2; i++) {
      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }
    }

    //////////////////////////////////////////////////////////////////////
    // SF_TYPE
    //////////////////////////////////////////////////////////////////////
    key = std::string("SF_TYPE") + "-" + std::to_string(sf_type_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    for (int i = 0; i < 2; i++) {
      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }
    }

    //////////////////////////////////////////////////////////////////////
    // START_TIME
    //////////////////////////////////////////////////////////////////////
    key = std::string("START_TIME") + "-" + std::to_string(start_time_1_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    key_counter[key] += conflict;

    if (key_counter[key] > max_conflict) {
      max_conflict = key_counter[key];
      max_conflict_key = key;
    }

    //////////////////////////////////////////////////////////////////////
    // END_TIME
    //////////////////////////////////////////////////////////////////////
    key = std::string("END_TIME") + "-" + std::to_string(end_time_1_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    key_counter[key] += conflict;

    if (key_counter[key] > max_conflict) {
      max_conflict = key_counter[key];
      max_conflict_key = key;
    }

    // If there is no conflict, return -1;
    if (max_conflict == CONFLICT_THRESHHOLD) {
      // std::cout << "Not find any conflict in Log Table" << std::endl;
      // return -1;
      max_conflict_key =
          std::string("S_ID") + "-" + std::to_string(GetPrimaryKey());
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
    // IS_ACTIVE
    //////////////////////////////////////////////////////////////////////
    key = std::string("IS_ACTIVE") + "-" + std::to_string(is_active_);

    // Get conflict from Log Table for the given condition
    int conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    // wid-3-->(3,100)(5,99)
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);

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

    //////////////////////////////////////////////////////////////////////
    // S_ID
    //////////////////////////////////////////////////////////////////////
    key = std::string("S_ID") + "-" + std::to_string(sid_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    // wid-3-->(3,100)(5,99)
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    for (int i = 0; i < 2; i++) {
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

    //////////////////////////////////////////////////////////////////////
    // SF_TYPE
    //////////////////////////////////////////////////////////////////////
    key = std::string("SF_TYPE") + "-" + std::to_string(sf_type_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    // wid-3-->(3,100)(5,99)
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);

    for (int i = 0; i < 2; i++) {
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

    //////////////////////////////////////////////////////////////////////
    // START_TIME
    //////////////////////////////////////////////////////////////////////
    key = std::string("START_TIME") + "-" + std::to_string(start_time_1_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    // wid-3-->(3,100)(5,99)
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);

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
    // IS_ACTIVE
    //////////////////////////////////////////////////////////////////////
    std::string key =
        std::string("IS_ACTIVE") + "-" + std::to_string(is_active_);

    // update run table
    concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
        key, queue_no);

    //////////////////////////////////////////////////////////////////////
    // S_ID
    //////////////////////////////////////////////////////////////////////
    key = std::string("S_ID") + "-" + std::to_string(sid_);

    // update run table
    for (int i = 0; i < 2; i++) {
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
    }

    //////////////////////////////////////////////////////////////////////
    // SF_TYPE
    //////////////////////////////////////////////////////////////////////
    key = std::string("SF_TYPE") + "-" + std::to_string(sf_type_);

    // update run table
    for (int i = 0; i < 2; i++) {
      concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
          key, queue_no);
    }

    //////////////////////////////////////////////////////////////////////
    // START_TIME
    //////////////////////////////////////////////////////////////////////
    key = std::string("START_TIME") + "-" + std::to_string(start_time_1_);

    // update run table
    concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
        key, queue_no);

    //////////////////////////////////////////////////////////////////////
    // END_TIME
    //////////////////////////////////////////////////////////////////////
    key = std::string("END_TIME") + "-" + std::to_string(end_time_1_);

    // update run table
    concurrency::TransactionScheduler::GetInstance().RunTableIncreaseNoLock(
        key, queue_no);
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
    // IS_ACTIVE
    //////////////////////////////////////////////////////////////////////
    std::string key =
        std::string("IS_ACTIVE") + "-" + std::to_string(is_active_);

    // update run table
    concurrency::TransactionScheduler::GetInstance().RunTableDecrease(key,
                                                                      queue_no);

    //////////////////////////////////////////////////////////////////////
    // S_ID
    //////////////////////////////////////////////////////////////////////
    key = std::string("S_ID") + "-" + std::to_string(sid_);

    // update run table
    for (int i = 0; i < 2; i++) {
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
    }

    //////////////////////////////////////////////////////////////////////
    // SF_TYPE
    //////////////////////////////////////////////////////////////////////
    key = std::string("SF_TYPE") + "-" + std::to_string(sf_type_);

    // update run table
    for (int i = 0; i < 2; i++) {
      concurrency::TransactionScheduler::GetInstance().RunTableDecrease(
          key, queue_no);
    }

    //////////////////////////////////////////////////////////////////////
    // START_TIME
    //////////////////////////////////////////////////////////////////////
    key = std::string("START_TIME") + "-" + std::to_string(start_time_1_);

    // update run table
    concurrency::TransactionScheduler::GetInstance().RunTableDecrease(key,
                                                                      queue_no);

    //////////////////////////////////////////////////////////////////////
    // END_TIME
    //////////////////////////////////////////////////////////////////////
    key = std::string("END_TIME") + "-" + std::to_string(end_time_1_);

    // update run table
    concurrency::TransactionScheduler::GetInstance().RunTableDecrease(key,
                                                                      queue_no);
  }

  virtual void DecreaseRunTable(bool single_ref __attribute__((unused)),
                                bool canonical) {

    DecreaseRunTableSingleRef(canonical);
    return;
  }

  virtual bool ExistInRunTable(int queue) {

    //////////////////////////////////////////////////////////////////////
    // S_ID
    //////////////////////////////////////////////////////////////////////
    std::string key = std::string("S_ID") + "-" + std::to_string(sid_);

    return concurrency::TransactionScheduler::GetInstance().ExistInRunTable(
        key, queue);
  }

  // For queue No.
  virtual void SetQueueNo(int queue_no) { queue_ = queue_no; }
  virtual int GetQueueNo() { return queue_; }

  // For fraction run
  virtual void UpdateLogTableSingleRefFullConflict(bool canonical
                                                   __attribute__((unused))) {

    std::string key;

    // update it in Log Table
    for (int i = 0; i < 2; i++) {
      key = std::string("S_ID") + "-" + std::to_string(sid_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
    }

    for (int i = 0; i < 2; i++) {
      key = std::string("SF_TYPE") + "-" + std::to_string(sf_type_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullConflictIncrease(key);
    }

    key = std::string("START_TIME") + "-" + std::to_string(start_time_1_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullConflictIncrease(key);

    key = std::string("END_TIME") + "-" + std::to_string(end_time_1_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullConflictIncrease(key);

    key = std::string("IS_ACTIVE") + "-" + std::to_string(is_active_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullConflictIncrease(key);
  }

  virtual void UpdateLogTableFullConflict(bool single_ref, bool canonical) {
    if (single_ref) {
      UpdateLogTableSingleRefFullConflict(canonical);
      return;
    }
  }

  virtual void UpdateLogTableSingleRefFullSuccess(bool canonical
                                                  __attribute__((unused))) {

    std::string key;

    // update it in Log Table
    for (int i = 0; i < 2; i++) {
      key = std::string("S_ID") + "-" + std::to_string(sid_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
    }

    for (int i = 0; i < 2; i++) {
      key = std::string("SF_TYPE") + "-" + std::to_string(sf_type_);
      concurrency::TransactionScheduler::GetInstance()
          .LogTableFullSuccessIncrease(key);
    }

    key = std::string("START_TIME") + "-" + std::to_string(start_time_1_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullSuccessIncrease(key);

    key = std::string("END_TIME") + "-" + std::to_string(end_time_1_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullSuccessIncrease(key);

    key = std::string("IS_ACTIVE") + "-" + std::to_string(is_active_);
    concurrency::TransactionScheduler::GetInstance()
        .LogTableFullSuccessIncrease(key);
  }

  virtual void UpdateLogTableFullSuccess(bool single_ref, bool canonical) {
    if (single_ref) {
      UpdateLogTableSingleRefFullSuccess(canonical);
      return;
    }
  }

  virtual int LookupRunTableMaxSingleRefFull(bool canonical
                                             __attribute__((unused))) {
    int max_conflict = CONFLICT_THRESHHOLD;
    std::string max_conflict_key;
    std::string key;
    std::map<std::string, int> key_counter;

    //////////////////////////////////////////////////////////////////////
    // IS_ACTIVE
    //////////////////////////////////////////////////////////////////////
    key = std::string("IS_ACTIVE") + "-" + std::to_string(is_active_);

    // Get conflict from Log Table for the given condition
    int conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    key_counter[key] += conflict;

    if (key_counter[key] > max_conflict) {
      max_conflict = key_counter[key];
      max_conflict_key = key;
    }

    //////////////////////////////////////////////////////////////////////
    // S_ID
    //////////////////////////////////////////////////////////////////////
    key = std::string("S_ID") + "-" + std::to_string(sid_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    for (int i = 0; i < 2; i++) {
      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }
    }

    //////////////////////////////////////////////////////////////////////
    // SF_TYPE
    //////////////////////////////////////////////////////////////////////
    key = std::string("SF_TYPE") + "-" + std::to_string(sf_type_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    for (int i = 0; i < 2; i++) {
      key_counter[key] += conflict;

      if (key_counter[key] > max_conflict) {
        max_conflict = key_counter[key];
        max_conflict_key = key;
      }
    }

    //////////////////////////////////////////////////////////////////////
    // START_TIME
    //////////////////////////////////////////////////////////////////////
    key = std::string("START_TIME") + "-" + std::to_string(start_time_1_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    key_counter[key] += conflict;

    if (key_counter[key] > max_conflict) {
      max_conflict = key_counter[key];
      max_conflict_key = key;
    }

    //////////////////////////////////////////////////////////////////////
    // END_TIME
    //////////////////////////////////////////////////////////////////////
    key = std::string("END_TIME") + "-" + std::to_string(end_time_1_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    key_counter[key] += conflict;

    if (key_counter[key] > max_conflict) {
      max_conflict = key_counter[key];
      max_conflict_key = key;
    }

    // If there is no conflict, return -1;
    if (max_conflict == CONFLICT_THRESHHOLD) {
      // std::cout << "Not find any conflict in Log Table" << std::endl;
      // return -1;
      max_conflict_key =
          std::string("S_ID") + "-" + std::to_string(GetPrimaryKey());
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
        int random_variable = std::rand() % queues.size();
        queue_no = queues.at(random_variable);
      }
    }

    return queue_no;
  }

  virtual int LookupRunTableMaxFull(bool single_ref __attribute__((unused)),
                                    bool canonical) {
    return LookupRunTableMaxSingleRefFull(canonical);
  }

  virtual int LookupRunTableSingleRefFull(bool canonical
                                          __attribute__((unused))) {
    int queue_count =
        concurrency::TransactionScheduler::GetInstance().GetQueueCount();

    std::vector<int> queue_map(queue_count, 0);
    int max_conflict = CONFLICT_THRESHHOLD;
    int return_queue = -1;
    std::string key;

    //////////////////////////////////////////////////////////////////////
    // IS_ACTIVE
    //////////////////////////////////////////////////////////////////////
    key = std::string("IS_ACTIVE") + "-" + std::to_string(is_active_);

    // Get conflict from Log Table for the given condition
    int conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    // wid-3-->(3,100)(5,99)
    std::unordered_map<int, int>* queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);

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

    //////////////////////////////////////////////////////////////////////
    // S_ID
    //////////////////////////////////////////////////////////////////////
    key = std::string("S_ID") + "-" + std::to_string(sid_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    // wid-3-->(3,100)(5,99)
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);
    for (int i = 0; i < 2; i++) {
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

    //////////////////////////////////////////////////////////////////////
    // SF_TYPE
    //////////////////////////////////////////////////////////////////////
    key = std::string("SF_TYPE") + "-" + std::to_string(sf_type_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    // wid-3-->(3,100)(5,99)
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);

    for (int i = 0; i < 2; i++) {
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

    //////////////////////////////////////////////////////////////////////
    // START_TIME
    //////////////////////////////////////////////////////////////////////
    key = std::string("START_TIME") + "-" + std::to_string(start_time_1_);

    // Get conflict from Log Table for the given condition
    conflict =
        concurrency::TransactionScheduler::GetInstance().LogTableFullGet(key);

    // Get the queues from Run Table for the given condition.
    // Each queue: <queueNo. reference>
    // wid-3-->(3,100)(5,99)
    queue_info =
        concurrency::TransactionScheduler::GetInstance().RunTableGetNoLock(key);

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

    return return_queue;
  }

  virtual int LookupRunTableFull(bool single_ref __attribute__((unused)),
                                 bool canonical) {
    return LookupRunTableSingleRefFull(canonical);
  }

  // Make them public for convenience
 public:
  executor::IndexScanExecutor* spe_index_scan_executor_;
  executor::IndexScanExecutor* call_index_scan_executor_;

  executor::ExecutorContext* context_;

  std::chrono::system_clock::time_point start_time_;
  std::chrono::system_clock::time_point exe_start_time_;

  // Flag to compute the execution time
  bool first_pop_;
  bool first_pop_exe_time_;

  // uint64_t primary_key_;
  std::vector<uint64_t> primary_keys_;

  // For execute
  int sid_;
  int sf_type_;
  int start_time_1_;
  int end_time_1_;
  int is_active_;

  SingleRegion region_;

  // For queue No.
  int queue_;
};

GetNewDestination* GenerateGetNewDestination(ZipfDistribution& zipf);
}
}
}  // end namespace peloton
