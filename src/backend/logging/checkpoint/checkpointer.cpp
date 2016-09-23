//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpointer.cpp
//
// Identification: src/backend/logging/checkpoint/checkpointer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/logging/chekcpoint/checkpointer.h"

namespace peloton {
namespace logging {

class Checkpointer {

  void Checkpointer::StartCheckpointing() {
    bool res = true;
    res = LoggingUtil::RemoveDirectory(checkpoint_dir_prefix.c_str(), false);
    PL_ASSERT(res == true);
    if (res != true) {
      LOG_ERROR("remove directory failed!");
      exit(-1);
    }
    res = LoggingUtil::CreateDirectory(checkpoint_dir_prefix.c_str(), 0700);
    PL_ASSERT(res == true);
    if (res != true) {
      LOG_ERROR("create directory failed!");
      exit(-1);
    }
    is_running_ = true;
    checkpoint_thread_.reset(new std::thread(&Checkpointer::Running, this));
  }

  void Checkpointer::StopCheckpointing() {
    is_running_ = false;
    checkpoint_thread_->join();
  }

  void Checkpointer::Running() {
    int count = 0;
    while (1) {
      if (is_working_ == false) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
      ++count;
      if (count == 30) {
        PerformCheckpoint();
        count = 0;
      }
    }
  }

  void Checkpointer::PerformCheckpoint() {
    // first of all, we should get a snapshot txn id.
    auto &epoch_manager = EpochManagerFactory::GetInstance();
    cid_t begin_cid = epoch_manager.EnterReadOnlyEpoch();

    auto &catalog_manager = catalog::Manager::GetInstance();
    auto database_count = catalog_manager.GetDatabaseCount();
    
    // creating files
    for (oid_t database_idx = 0; database_idx < database_count; database_idx++) {
      auto database = catalog_manager.GetDatabase(database_idx);
      auto table_count = database->GetTableCount();
      
      for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
        std::string filename = GetCheckpointFileFullPath(database_idx, table_idx, begin_cid);
        Filehandle file_handle;
        CreateFile
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
        CheckpointTable(begin_cid, target_table);
      }
    }

    // exit epoch.
    epoch_manager.ExitReadOnlyEpoch(current_txn->GetEpochId());

  }

  void Checkpointer::CheckpointTable(cid_t begin_cid, storage::DataTable *target_table) {
    
    // TODO split checkpoint file into multiple files in the future
    // Create a new file for checkpoint
    


    LOG_TRACE("DoCheckpoint cid = %lu", start_commit_id_);

    // Add txn begin record
    std::shared_ptr<LogRecord> begin_record(new TransactionRecord(
        LOGRECORD_TYPE_TRANSACTION_BEGIN, start_commit_id_));




    CopySerializeOutput begin_output_buffer;
    begin_record->Serialize(begin_output_buffer);
    records_.push_back(begin_record);

    auto catalog = catalog::Catalog::GetInstance();
    auto database_count = catalog->GetDatabaseCount();

      
    // loop all tables
    for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
      // Get the target table
      storage::DataTable *target_table = database->GetTable(table_idx);
      PL_ASSERT(target_table);
      LOG_TRACE("SeqScan: database idx %u table idx %u: %s", database_idx,
                table_idx, target_table->GetName().c_str());
      Scan(target_table, database_oid);
    }
    
    // Add txn commit record
    std::shared_ptr<LogRecord> commit_record(new TransactionRecord(
        LOGRECORD_TYPE_TRANSACTION_COMMIT, start_commit_id_));
    CopySerializeOutput commit_output_buffer;
    commit_record->Serialize(commit_output_buffer);
    records_.push_back(commit_record);

    // TODO Add delimiter record for checkpoint recovery as well
    Persist();



    Cleanup();

    most_recent_checkpoint_cid = start_commit_id_;
  }

  

  void Scan(storage::DataTable *target_table,
                              oid_t database_oid) {
    auto schema = target_table->GetSchema();
    PL_ASSERT(schema);
    std::vector<oid_t> column_ids;
    column_ids.resize(schema->GetColumnCount());
    std::iota(column_ids.begin(), column_ids.end(), 0);

    oid_t current_tile_group_offset = START_OID;
    auto table_tile_group_count = target_table->GetTileGroupCount();
    CheckpointTileScanner scanner;

    // TODO scan assigned tile in multi-thread checkpoint
    while (current_tile_group_offset < table_tile_group_count) {
      // Retrieve a tile group
      auto tile_group = target_table->GetTileGroup(current_tile_group_offset);

      // Retrieve a logical tile
      std::unique_ptr<executor::LogicalTile> logical_tile(
          scanner.Scan(tile_group, column_ids, start_commit_id_));

      // Empty result
      if (!logical_tile) {
        current_tile_group_offset++;
        continue;
      }

      auto tile_group_id = logical_tile->GetColumnInfo(0)
                               .base_tile->GetTileGroup()
                               ->GetTileGroupId();

      // Go over the logical tile
      for (oid_t tuple_id : *logical_tile) {
        expression::ContainerTuple<executor::LogicalTile> cur_tuple(
            logical_tile.get(), tuple_id);

        // Logging
        {
          // construct a physical tuple from the logical tuple
          std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
          for (auto column_id : column_ids) {
            std::unique_ptr<common::Value> val(cur_tuple.GetValue(column_id));
            tuple->SetValue(column_id, *val, this->pool.get());
          }
          ItemPointer location(tile_group_id, tuple_id);
          // TODO is it possible to avoid `new` for checkpoint?
          std::shared_ptr<LogRecord> record(logger_->GetTupleRecord(
              LOGRECORD_TYPE_TUPLE_INSERT, INITIAL_TXN_ID, target_table->GetOid(),
              database_oid, location, INVALID_ITEMPOINTER, tuple.get()));
          PL_ASSERT(record);
          CopySerializeOutput output_buffer;
          record->Serialize(output_buffer);
          LOG_TRACE("Insert a new record for checkpoint (%u, %u)", tile_group_id,
                    tuple_id);
          records_.push_back(record);
        }
      }
      // persist to file once per tile
      Persist();
      current_tile_group_offset++;
    }
  }


  // Visibility check
  bool Checkpointer::IsVisible(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id) {
    txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
    cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
    cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

    bool activated = (current_txn->GetBeginCommitId() >= tuple_begin_cid);
    bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);

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
}