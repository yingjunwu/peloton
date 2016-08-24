//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_executor.cpp
//
// Identification: src/backend/executor/insert_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/insert_executor.h"

#include "backend/catalog/manager.h"
#include "backend/common/logger.h"
#include "backend/common/pool.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/executor_context.h"
#include "backend/expression/container_tuple.h"
#include "backend/planner/insert_plan.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple_iterator.h"
#include "backend/index/index_factory.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for insert executor.
 * @param node Insert node corresponding to this executor.
 */
InsertExecutor::InsertExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DInit() {
  PL_ASSERT(children_.size() == 0 || children_.size() == 1);
  PL_ASSERT(executor_context_);

  done_ = false;
  return true;
}

/**
 * @brief Adds a column to the logical tile, using the position lists.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DExecute() {
  if (done_) return false;

  PL_ASSERT(!done_);
  PL_ASSERT(executor_context_ != nullptr);

  const planner::InsertPlan &node = GetPlanNode<planner::InsertPlan>();
  storage::DataTable *target_table = node.GetTable();
  oid_t bulk_insert_count = node.GetBulkInsertCount();
  PL_ASSERT(target_table);

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();
  auto executor_pool = executor_context_->GetExecutorContextPool();

  // Inserting a logical tile.
  if (children_.size() == 1) {
    LOG_TRACE("Insert executor :: 1 child ");

    if (!children_[0]->Execute()) {
      return false;
    }

    std::unique_ptr<LogicalTile> logical_tile(children_[0]->GetOutput());
    PL_ASSERT(logical_tile.get() != nullptr);
    auto target_table_schema = target_table->GetSchema();
    auto column_count = target_table_schema->GetColumnCount();

    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(target_table_schema, true));

    // Go over the logical tile
    for (oid_t tuple_id : *logical_tile) {
      expression::ContainerTuple<LogicalTile> cur_tuple(logical_tile.get(),
                                                        tuple_id);

      // Materialize the logical tile tuple
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++)
        tuple->SetValue(column_itr, cur_tuple.GetValue(column_itr),
                        executor_pool);

      ItemPointer *itemptr_ptr = nullptr;
      index::RBItemPointer *rb_itemptr_ptr = nullptr;
      peloton::ItemPointer location;
      
      if (concurrency::TransactionManagerFactory::IsRB()
         && index::IndexFactory::GetSecondaryIndexType() == SECONDARY_INDEX_TYPE_VERSION) {
        location = target_table->InsertTuple(tuple.get(), &rb_itemptr_ptr);
        assert(rb_itemptr_ptr != nullptr);
      } else {
        location = target_table->InsertTuple(tuple.get(), &itemptr_ptr);
      }
      if (location.block == INVALID_OID) {
        transaction_manager.SetTransactionResult(
            peloton::Result::RESULT_FAILURE);
        return false;
      }
      bool res;

      if (concurrency::TransactionManagerFactory::IsN2O() == true) {
        // If we are using OCC N2O txn manager, use another form of perform insert
        res = transaction_manager.PerformInsert(location, itemptr_ptr);
      } else if (concurrency::TransactionManagerFactory::IsRB()
        && index::IndexFactory::GetSecondaryIndexType() == SECONDARY_INDEX_TYPE_VERSION) {
        res = ((concurrency::RBTxnManager*)&transaction_manager)->PerformInsert(location, rb_itemptr_ptr);
      }  else {
        res = transaction_manager.PerformInsert(location);
      }

      if (!res) {
        transaction_manager.SetTransactionResult(RESULT_FAILURE);
        return res;
      }

      executor_context_->num_processed += 1;  // insert one
    }

    return true;
  }
  // Inserting a collection of tuples from plan node
  else if (children_.size() == 0) {
    LOG_TRACE("Insert executor :: 0 child ");

    // Extract expressions from plan node and construct the tuple.
    // For now we just handle a single tuple
    auto schema = target_table->GetSchema();
    auto project_info = node.GetProjectInfo();
    auto tuple = node.GetTuple();
    std::unique_ptr<storage::Tuple> project_tuple;

    // Check if this is not a raw tuple
    if(tuple == nullptr) {
      // Otherwise, there must exist a project info
      PL_ASSERT(project_info);
      // There should be no direct maps
      PL_ASSERT(project_info->GetDirectMapList().size() == 0);

      project_tuple.reset(new storage::Tuple(schema, true));

      for (auto target : project_info->GetTargetList()) {
        peloton::Value value =
            target.second->Evaluate(nullptr, nullptr, executor_context_);
        project_tuple->SetValue(target.first, value, executor_pool);
      }

      // Set tuple to point to temporary project tuple
      tuple = project_tuple.get();
    }

    // Bulk Insert Mode
    for (oid_t insert_itr = 0; insert_itr < bulk_insert_count; insert_itr++) {

      // Carry out insertion
      ItemPointer *itemptr_ptr = nullptr;
      index::RBItemPointer *rb_itemptr_ptr = nullptr;
      peloton::ItemPointer location;
      
      if (concurrency::TransactionManagerFactory::IsRB()
         && index::IndexFactory::GetSecondaryIndexType() == SECONDARY_INDEX_TYPE_VERSION) {
        location = target_table->InsertTuple(tuple, &rb_itemptr_ptr);
      } else {
        location = target_table->InsertTuple(tuple, &itemptr_ptr);
      }
      if (location.block == INVALID_OID) {
        transaction_manager.SetTransactionResult(
            peloton::Result::RESULT_FAILURE);
        return false;
      }
      bool res;

      if (concurrency::TransactionManagerFactory::GetProtocol() == CONCURRENCY_TYPE_OCC_N2O) {      
        // If we are using OCC N2O txn manager, use another form of perform insert
        res = ((concurrency::OptimisticN2OTxnManager*)&transaction_manager)->PerformInsert(location, itemptr_ptr);
      } else if (concurrency::TransactionManagerFactory::GetProtocol() == CONCURRENCY_TYPE_TO_N2O) {
        // If we are using TO N2O txn manager, use another form of perform insert
        res = ((concurrency::TsOrderN2OTxnManager*)&transaction_manager)->PerformInsert(location, itemptr_ptr);
      } else if (concurrency::TransactionManagerFactory::GetProtocol() == CONCURRENCY_TYPE_TO_OPT_N2O) {
        // If we are using TO OPT N2O txn manager, use another form of perform insert
        res = ((concurrency::TsOrderOptN2OTxnManager*)&transaction_manager)->PerformInsert(location, itemptr_ptr);
      } else if (concurrency::TransactionManagerFactory::IsRB()) {
        if (index::IndexFactory::GetSecondaryIndexType() == SECONDARY_INDEX_TYPE_VERSION) {
          res = ((concurrency::RBTxnManager*)&transaction_manager)->PerformInsert(location, rb_itemptr_ptr);
        } else {
          res = ((concurrency::RBTxnManager*)&transaction_manager)->PerformInsert(location, itemptr_ptr);
        }
      } else {
        res = transaction_manager.PerformInsert(location);
      }

      if (!res) {
        transaction_manager.SetTransactionResult(RESULT_FAILURE);
        return res;
      }

      executor_context_->num_processed += 1;  // insert one

    }

    done_ = true;
    return true;
  }

  return true;
}

}  // namespace executor
}  // namespace peloton
