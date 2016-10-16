//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// phylog_checkpoint_manager.cpp
//
// Identification: src/backend/logging/phylog_checkpoint_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/logging/phylog_checkpoint_manager.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/epoch_manager_factory.h"

namespace peloton {
namespace logging {

  void PhyLogCheckpointManager::StartCheckpointing() {
    is_running_ = true;
    central_checkpoint_thread_.reset(new std::thread(&PhyLogCheckpointManager::Running, this));
  }

  void PhyLogCheckpointManager::StopCheckpointing() {
    is_running_ = false;
    central_checkpoint_thread_->join();
  }

  void PhyLogCheckpointManager::Running() {
    FileHandle file_handle;
    std::string filename = ckpt_pepoch_dir_ + "/" + ckpt_pepoch_filename_;
    // Create a new file
    if (LoggingUtil::OpenFile(filename.c_str(), "wb", file_handle) == false) {
      LOG_ERROR("Unable to create ckpt pepoch file %s\n", filename.c_str());
      exit(EXIT_FAILURE);
    }

    int count = 0;
    while (1) {
      if (is_running_ == false) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
      ++count;
      if (count == checkpoint_interval_) {
        // first of all, we should get a snapshot txn id.
        auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
        cid_t begin_cid = epoch_manager.EnterReadOnlyEpoch();

        PerformCheckpoint(begin_cid);
        size_t epoch_id = begin_cid >> 32;
        // exit epoch.
        epoch_manager.ExitReadOnlyEpoch(epoch_id);

        fwrite((const void *) (&epoch_id), sizeof(epoch_id), 1, file_handle.file);
        
        LoggingUtil::FFlushFsync(file_handle);
    
        count = 0;
      }
    }
  }

  void PhyLogCheckpointManager::PerformCheckpoint(const cid_t &begin_cid) {

    // prepare database structures.
    // each checkpoint thread must observe the same database structures.
    // this guarantees that the workload can be partitioned consistently.
    std::vector<std::vector<size_t>> database_structures;

    auto &catalog_manager = catalog::Manager::GetInstance();
    auto database_count = catalog_manager.GetDatabaseCount();

    database_structures.resize(database_count);

    for (oid_t database_idx = 0; database_idx < database_count; ++database_idx) {
      auto database = catalog_manager.GetDatabase(database_idx);
      auto table_count = database->GetTableCount();

      database_structures[database_idx].resize(table_count);

      for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
  
        storage::DataTable *target_table = database->GetTable(table_idx);

        database_structures[database_idx][table_idx] = target_table->GetTileGroupCount();
      }
    }
    
    // after obtaining the database structures, we can start performing checkpointing in parallel.
    std::vector<std::unique_ptr<std::thread>> checkpoint_threads;
    checkpoint_threads.resize(checkpointer_count_);
    for (size_t i = 0; i < checkpointer_count_; ++i) {
      checkpoint_threads[i].reset(new std::thread(&PhyLogCheckpointManager::PerformCheckpointThread, this, i, begin_cid, std::ref(database_structures)));
    }
    
    for (size_t i = 0; i < checkpointer_count_; ++i) {
      checkpoint_threads[i]->join();
    }

  }

  void PhyLogCheckpointManager::PerformCheckpointThread(const size_t &thread_id, const cid_t &begin_cid, const std::vector<std::vector<size_t>> &database_structures) {
    
    size_t epoch_id = begin_cid >> 32;

    // creating files
    FileHandle **file_handles = new FileHandle*[database_structures.size()];
    
    for (oid_t database_idx = 0; database_idx < database_structures.size(); database_idx++) {
    
      file_handles[database_idx] = new FileHandle[database_structures[database_idx].size()];

      for (oid_t table_idx = 0; table_idx < database_structures[database_idx].size(); table_idx++) {
        std::string file_name = GetCheckpointFileFullPath(thread_id, database_idx, table_idx, epoch_id);
       
        bool success =
            LoggingUtil::OpenFile(file_name.c_str(), "wb", file_handles[database_idx][table_idx]);
        
        PL_ASSERT(success == true);
        if (success != true) {
          LOG_ERROR("create file failed!");
          exit(-1);
        }
      }
    }

    auto &catalog_manager = catalog::Manager::GetInstance();

    // loop all databases
    for (oid_t database_idx = 0; database_idx < database_structures.size(); database_idx++) {
      auto database = catalog_manager.GetDatabase(database_idx);
      
      // loop all tables
      for (oid_t table_idx = 0; table_idx < database_structures[database_idx].size(); table_idx++) {
        // Get the target table
        storage::DataTable *target_table = database->GetTable(table_idx);
        PL_ASSERT(target_table);

        size_t tile_group_count = database_structures.at(database_idx).at(table_idx);

        CheckpointTable(target_table, tile_group_count, thread_id, begin_cid, file_handles[database_idx][table_idx]);
      }
    }


    // close files
    for (oid_t database_idx = 0; database_idx < database_structures.size(); database_idx++) {
      
      for (oid_t table_idx = 0; table_idx < database_structures[database_idx].size(); table_idx++) {
    
        fclose(file_handles[database_idx][table_idx].file);

      }
    
      delete[] file_handles[database_idx];
    }
    delete[] file_handles;

  }


  void PhyLogCheckpointManager::CheckpointTable(storage::DataTable *target_table, const size_t &tile_group_count, const size_t &thread_id, const cid_t &begin_cid, FileHandle &file_handle) {
    
    CopySerializeOutput output_buffer;

    for (size_t current_tile_group_offset = 0; current_tile_group_offset < tile_group_count; ++current_tile_group_offset) {
    
      // shuffle workloads
      if (current_tile_group_offset % checkpointer_count_ != thread_id) {
        continue;
      }
    

      auto tile_group =
          target_table->GetTileGroup(current_tile_group_offset);
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
  bool PhyLogCheckpointManager::IsVisible(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id, const cid_t &begin_cid) {
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