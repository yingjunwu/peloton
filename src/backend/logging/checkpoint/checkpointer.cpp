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
    is_working_ = true;
    checkpoint_thread_.reset(new std::thread(&Checkpointer::Running, this));
  }

  void Checkpointer::StopCheckpointing() {
    is_working_ = false;
    checkpoint_thread_->join();
  }

  void Checkpointer::RegisterTable(oid_t table_id) {
    table_set_.insert(table_id);
  }

  void Checkpointer::Running() {
    int count = 0;
    while (1) {
      if (is_working_ == false) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
      if (count != 30) {
        ++count;
      } else {
        count = 0;
        DoCheckpoint(timestamp);
      }
    }
  }


  void Checkpointer::DoCheckpoint() {

    // get a previous id.
    // scan. if qualified, then dump.
    txn_id_t txn_id = READONLY_TXN_ID;
    auto &epoch_manager = EpochManagerFactory::GetInstance();
    cid_t begin_cid = epoch_manager.EnterReadOnlyEpoch();
  
    // scan all the tile groups
    Scan();

    epoch_manager.ExitReadOnlyEpoch(current_txn->GetEpochId());


    // TODO split checkpoint file into multiple files in the future
    // Create a new file for checkpoint
    CreateFile();


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


}

}
}