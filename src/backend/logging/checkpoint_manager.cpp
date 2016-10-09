//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpoint_manager.cpp
//
// Identification: src/backend/logging/checkpoint_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/logging/checkpoint_manager.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/epoch_manager_factory.h"

namespace peloton {
namespace logging {

  void CheckpointManager::StartCheckpointing() {
    is_running_ = true;
    checkpoint_thread_.reset(new std::thread(&CheckpointManager::Running, this));
  }

  void CheckpointManager::StopCheckpointing() {
    is_running_ = false;
    checkpoint_thread_->join();
  }

  void CheckpointManager::Running() {
    int count = 0;
    while (1) {
      if (is_running_ == false) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
      ++count;
      if (count == CHECKPOINT_INTERVAL) {
        PerformCheckpoint();
        count = 0;
      }
    }
  }

  void CheckpointManager::PerformCheckpoint() {

    // prepare files
    auto &catalog_manager = catalog::Manager::GetInstance();
    auto database_count = catalog_manager.GetDatabaseCount();
    
    // first of all, we should get a snapshot txn id.
    auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
    cid_t begin_cid = epoch_manager.EnterReadOnlyEpoch();
    size_t epoch_id = begin_cid >> 32;

    // creating files
    FileHandle **file_handles = new FileHandle*[database_count];
    for (oid_t database_idx = 0; database_idx < database_count; database_idx++) {
      auto database = catalog_manager.GetDatabase(database_idx);
      auto table_count = database->GetTableCount();
      
      file_handles[database_idx] = new FileHandle[table_count];
      for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
        std::string file_name = GetCheckpointFileFullPath(0, database_idx, table_idx, epoch_id);
       
        bool success =
            LoggingUtil::OpenFile(file_name.c_str(), "wb", file_handles[database_idx][table_idx]);
        
        PL_ASSERT(success == true);
        if (success != true) {
          LOG_ERROR("create file failed!");
          exit(-1);
        }

      }
    }

    // loop all databases
    for (oid_t database_idx = 0; database_idx < database_count; database_idx++) {
      auto database = catalog_manager.GetDatabase(database_idx);
      auto table_count = database->GetTableCount();
      
      // loop all tables
      for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
        // Get the target table
        storage::DataTable *target_table = database->GetTable(table_idx);
        PL_ASSERT(target_table);
        LOG_TRACE("SeqScan: database idx %u table idx %u: %s", database_idx,
                 table_idx, target_table->GetName().c_str());
        CheckpointTable(target_table, begin_cid, file_handles[database_idx][table_idx]);
      }
    }

    // exit epoch.
    epoch_manager.ExitReadOnlyEpoch(concurrency::current_txn->GetEpochId());


    // close files
    for (oid_t database_idx = 0; database_idx < database_count; database_idx++) {
      auto database = catalog_manager.GetDatabase(database_idx);
      auto table_count = database->GetTableCount();
      
      file_handles[database_idx] = new FileHandle[table_count];
      for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
        
        fclose(file_handles[database_idx][table_idx].file);

      }
      delete[] file_handles[table_count];
    }
    delete[] file_handles;
  }

  void CheckpointManager::CheckpointTable(storage::DataTable *target_table, const cid_t &begin_cid, FileHandle &file_handle) {

    LOG_TRACE("perform checkpoint cid = %lu", begin_cid);

    oid_t tile_group_count = target_table->GetTileGroupCount();
    oid_t current_tile_group_offset = 0;
    
    CopySerializeOutput output_buffer;

    while (current_tile_group_offset < tile_group_count) {
      auto tile_group =
          target_table->GetTileGroup(current_tile_group_offset++);
      auto tile_group_header = tile_group->GetHeader();

      oid_t active_tuple_count = tile_group->GetNextTupleSlot();

      for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {

        // check tuple version visibility
        if (IsVisible(tile_group_header, tuple_id, begin_cid) == true) {
          // persist this version.
          //PersistTuple(tile_group_header, tuple_id);

          expression::ContainerTuple<storage::TileGroup> container_tuple(
            tile_group.get(), tuple_id);
          
          output_buffer.Reset();

          container_tuple.SerializeTo(output_buffer);

          fwrite((const void *) (output_buffer.Data()), output_buffer.Size(), 1, file_handle.file);

        } // end if isvisible
      }   // end for
    }     // end while

    // Call fsync
    LoggingUtil::FFlushFsync(file_handle);

  }


  // Visibility check
  bool CheckpointManager::IsVisible(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id, const cid_t &begin_cid) {
    txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
    cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
    cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

    bool activated = (begin_cid >= tuple_begin_cid);
    bool invalidated = (begin_cid >= tuple_end_cid);

    if (tuple_txn_id == INVALID_TXN_ID) {
      // the tuple is not available.
      return false;
    }

    // there are exactly two versions that can be owned by a transaction.
    // unless it is an insertion.

    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // never read an uncommitted version.
        return false;
      } else {
        // the older version may be visible.
        if (activated && !invalidated) {
          return true;
        } else {
          return false;
        }
      }
    } else {
      // if the tuple is not owned by any transaction.
      if (activated && !invalidated) {
        return true;
      } else {
        return false;
      }
    }
  }

}
}