//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_executor.cpp
//
// Identification: src/backend/executor/update_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/update_executor.h"
#include "backend/planner/update_plan.h"
#include "backend/common/logger.h"
#include "backend/catalog/manager.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/executor_context.h"
#include "backend/expression/container_tuple.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tile.h"
#include "backend/storage/rollback_segment.h"
#include "backend/index/index_factory.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for update executor.
 * @param node Update node corresponding to this executor.
 */
UpdateExecutor::UpdateExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context){
	}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DInit() {
  assert(children_.size() == 1);
  assert(target_table_ == nullptr);
  assert(project_info_ == nullptr);

  // Grab settings from node
  const planner::UpdatePlan &node = GetPlanNode<planner::UpdatePlan>();
  target_table_ = node.GetTable();
  project_info_.reset(new planner::ProjectInfo(*(node.GetProjectInfo())));
  
  assert(target_table_);
  assert(project_info_);

  return true;
}

/**
 * @brief updates a set of columns
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DExecute() {
  assert(children_.size() == 1);
  assert(executor_context_);

  // We are scanning over a logical tile.
  LOG_TRACE("Update executor :: 1 child ");
  if (!children_[0]->Execute()) {
    return false;
  }

  if (concurrency::TransactionManagerFactory::GetProtocol() == CONCURRENCY_TYPE_TO_SV) {
    // single version concurrency control
    return DExecuteSV();
  } else {
    // multi version concurrency control
    return DExecuteMV();    
  }

}


bool UpdateExecutor::DExecuteSV() {
  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

  auto &pos_lists = source_tile.get()->GetPositionLists();
  storage::Tile *tile = source_tile->GetBaseTile(0);
  storage::TileGroup *tile_group = tile->GetTileGroup();
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();
  auto tile_group_id = tile_group->GetTileGroupId();

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  // Update tuples in given table
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    ItemPointer old_location(tile_group_id, physical_tuple_id);

    LOG_TRACE("Visible Tuple id : %u, Physical Tuple id : %u ",
              visible_tuple_id, physical_tuple_id);

    if (transaction_manager.IsOwner(tile_group_header, physical_tuple_id) == true) {
      
      // Make a copy of the original tuple and allocate a new tuple
      expression::ContainerTuple<storage::TileGroup> old_tuple(
          tile_group, physical_tuple_id);
      
      // Execute the projections
      project_info_->Evaluate(&old_tuple, &old_tuple, nullptr,
                              executor_context_);

      transaction_manager.PerformUpdate(old_location);

    } else if (transaction_manager.IsOwnable(tile_group_header, physical_tuple_id) == true) {
      // if the tuple is not owned by any transaction and is visible to current
      // transaction.
      if (transaction_manager.AcquireOwnership(tile_group_header, tile_group_id,
                                               physical_tuple_id) == false) {
        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
        transaction_manager.AddOneAcquireOwnerAbort();
        return false;
      }

      ItemPointer new_location = target_table_->AcquireVersion();

      auto &manager = catalog::Manager::GetInstance();
      auto new_tile_group = manager.GetTileGroup(new_location.block);

      expression::ContainerTuple<storage::TileGroup> new_tuple(
          new_tile_group.get(), new_location.offset);

      expression::ContainerTuple<storage::TileGroup> old_tuple(
          tile_group, physical_tuple_id);

      // perform projection from old version to new version.
      // this triggers in-place update, and we do not need to allocate another version.
      project_info_->Evaluate(&new_tuple, &old_tuple, nullptr,
                              executor_context_);
      
      bool ret = target_table_->InstallVersion(&new_tuple, new_location);
      
      // PerformUpdate() will not be executed if the insertion failed,
      // There is a write lock acquired, but it is not in the write set.
      // the acquired lock can't be released when the txn is aborted.
      if (ret == false) {
        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        // First yield ownership
        transaction_manager.YieldOwnership(tile_group_id, physical_tuple_id);
        transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
        transaction_manager.AddOneNoSpaceAbort();
        return false;
      }

      LOG_TRACE("perform update old location: %u, %u", old_location.block, old_location.offset);
      LOG_TRACE("perform update new location: %u, %u", new_location.block, new_location.offset);
      transaction_manager.PerformUpdate(old_location, new_location);

    } else {
      // transaction should be aborted as we cannot update the latest version.
      LOG_TRACE("Fail to update tuple. Set txn failure.");
      transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
      transaction_manager.AddOneCannotOwnAbort();
      return false;
    }
  }
  return true;
}


bool UpdateExecutor::DExecuteMV() {
  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

  auto &pos_lists = source_tile.get()->GetPositionLists();
  storage::Tile *tile = source_tile->GetBaseTile(0);
  storage::TileGroup *tile_group = tile->GetTileGroup();
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();
  auto tile_group_id = tile_group->GetTileGroupId();

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  auto concurrency_protocol = concurrency::TransactionManagerFactory::GetProtocol();
  auto schema = target_table_->GetSchema();

  // Update tuples in given table
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    ItemPointer old_location(tile_group_id, physical_tuple_id);

    LOG_TRACE("Visible Tuple id : %u, Physical Tuple id : %u ",
              visible_tuple_id, physical_tuple_id);

    if (transaction_manager.IsOwner(tile_group_header, physical_tuple_id) == true) {

      // Check if we are using rollback segment
      if (concurrency::TransactionManagerFactory::IsRB()) {

        // Make a copy of the original tuple and allocate a new tuple
        expression::ContainerTuple<storage::TileGroup> old_tuple(
            tile_group, physical_tuple_id);


        if ((concurrency_protocol == CONCURRENCY_TYPE_TO_FULL_RB
             || concurrency_protocol == CONCURRENCY_TYPE_TO_FULL_CENTRAL_RB)) {
          auto rb_txn_manager = (concurrency::TsOrderFullRbTxnManager*)&transaction_manager;

          // This is an inserted tuple, create a full rb and perform update
          if (rb_txn_manager->IsInserted(tile_group_header, physical_tuple_id)) {
            expression::ContainerTuple<storage::TileGroup> old_tuple(
              tile_group, physical_tuple_id);
            char *rb_seg = rb_txn_manager->GetSegmentPool()->CreateSegmentFromTuple(
              schema, &old_tuple);
            rb_txn_manager->PerformUpdateWithRb(old_location, rb_seg);
          }
          
          // Overwrite the master copy
          project_info_->Evaluate(&old_tuple, &old_tuple, nullptr,
                                  executor_context_);

          // Insert into secondary index
          rb_txn_manager->RBInsertVersion(target_table_, old_location, &old_tuple);
        } else {
          auto rb_txn_manager = (concurrency::RBTxnManager*)&transaction_manager;

          if (rb_txn_manager->IsInserted(tile_group_header, physical_tuple_id) == false) {
            // If it's not an inserted tuple,
            // create a new rollback segment based on the old one and the old tuple
            auto rb_seg = rb_txn_manager->GetSegmentPool()->CreateSegmentFromTuple(
              schema, project_info_->GetTargetList(), &old_tuple);

            // TODO: rb_seg == nullptr may be resulted from an optimization to be done
            // when creating rollback segment
            if (rb_seg != nullptr) {
              // Ask the txn manager to add the a new rollback segment
              rb_txn_manager->PerformUpdateWithRb(old_location, rb_seg);
            }
          }

          // Overwrite the master copy
          project_info_->Evaluate(&old_tuple, &old_tuple, nullptr,
                                  executor_context_);

          // Insert into secondary index
          rb_txn_manager->RBInsertVersion(target_table_, old_location, &old_tuple);
        }
      } else {
        // if not RB.

        // Make a copy of the original tuple and allocate a new tuple
        expression::ContainerTuple<storage::TileGroup> old_tuple(
            tile_group, physical_tuple_id);
        // Execute the projections
        project_info_->Evaluate(&old_tuple, &old_tuple, nullptr,
                                executor_context_);

        transaction_manager.PerformUpdate(old_location);
      }
    } else if (transaction_manager.IsOwnable(tile_group_header,
                                             physical_tuple_id) == true) {
      // if the tuple is not owned by any transaction and is visible to current
      // transaction.
      if (transaction_manager.AcquireOwnership(tile_group_header, tile_group_id,
                                               physical_tuple_id) == false) {
        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
        transaction_manager.AddOneAcquireOwnerAbort();
        return false;
      }

      if (concurrency::TransactionManagerFactory::IsRB()) {
        // Make a copy of the original tuple and allocate a new tuple
        expression::ContainerTuple<storage::TileGroup> old_tuple(
            tile_group, physical_tuple_id);

        // For rollback segment implementation
        auto rb_txn_manager = (concurrency::RBTxnManager*)&transaction_manager;

        // Create a rollback segment based on the old tuple
        char *rb_seg = nullptr;
        if (concurrency_protocol == CONCURRENCY_TYPE_TO_FULL_RB ||
            concurrency_protocol == CONCURRENCY_TYPE_TO_FULL_CENTRAL_RB) {
          rb_seg = rb_txn_manager->GetSegmentPool()->CreateSegmentFromTuple(
            schema, &old_tuple);
        } else {
          rb_seg = rb_txn_manager->GetSegmentPool()->CreateSegmentFromTuple(
            schema, project_info_->GetTargetList(), &old_tuple);  
        }

        // Ask the txn manager to append the rollback segment
        rb_txn_manager->PerformUpdateWithRb(old_location, rb_seg);
        
        // Overwrite the master copy
        // Execute the projections
        project_info_->Evaluate(&old_tuple, &old_tuple, nullptr,
                                executor_context_);

        // Insert into secondary index. Maybe we can insert index before
        // CopyTuple? -- RX
        rb_txn_manager->RBInsertVersion(target_table_, old_location, &old_tuple);
        
      } else {

        ItemPointer new_location = target_table_->AcquireVersion();

        auto &manager = catalog::Manager::GetInstance();
        auto new_tile_group = manager.GetTileGroup(new_location.block);

        expression::ContainerTuple<storage::TileGroup> new_tuple(
            new_tile_group.get(), new_location.offset);

        expression::ContainerTuple<storage::TileGroup> old_tuple(
            tile_group, physical_tuple_id);

        // perform projection from old version to new version.
        // this triggers in-place update, and we do not need to allocate another version.
        project_info_->Evaluate(&new_tuple, &old_tuple, nullptr,
                                executor_context_);

        bool ret = true;
        // finally insert updated tuple into the table
        if (index::IndexFactory::GetSecondaryIndexType() == SECONDARY_INDEX_TYPE_TUPLE) {
          
          ItemPointer *master_ptr = tile_group_header->GetMasterPointer(old_location.offset);
          
          ret = target_table_->InstallVersion(&new_tuple, new_location, &(project_info_->GetTargetList()), master_ptr);
        
        } else {
          
          ret = target_table_->InstallVersion(&new_tuple, new_location);
        }
        // FIXME: PerformUpdate() will not be executed if the insertion failed,
        // There is a write lock acquired, but it is not in the write set.
        // the acquired lock can't be released when the txn is aborted.
        if (ret == false) {
          LOG_TRACE("Fail to insert new tuple. Set txn failure.");
          // First yield ownership
          transaction_manager.YieldOwnership(tile_group_id, physical_tuple_id);
          transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
          transaction_manager.AddOneNoSpaceAbort();
          return false;
        }

        LOG_TRACE("perform update old location: %u, %u", old_location.block, old_location.offset);
        LOG_TRACE("perform update new location: %u, %u", new_location.block, new_location.offset);
        transaction_manager.PerformUpdate(old_location, new_location);
      }

      // TODO: Why don't we also do this in the if branch above?
      executor_context_->num_processed += 1;  // updated one
    } else {
      // transaction should be aborted as we cannot update the latest version.
      LOG_TRACE("Fail to update tuple. Set txn failure.");
      transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
      transaction_manager.AddOneCannotOwnAbort();
      return false;
    }
  }
  return true;

}

}  // namespace executor
}  // namespace peloton
