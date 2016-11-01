//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// physical_checkpoint_manager.cpp
//
// Identification: src/backend/logging/physical_checkpoint_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/logging/physical_checkpoint_manager.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/epoch_manager_factory.h"

namespace peloton {
namespace logging {


  void PhysicalCheckpointManager::PrepareTables(const std::vector<std::vector<size_t>> &database_structures) {

    auto &catalog_manager = catalog::Manager::GetInstance();
    auto database_count = catalog_manager.GetDatabaseCount();

    if (database_count != database_structures.size()) {
      LOG_ERROR("incorrect database state!");
      exit(EXIT_FAILURE);
    }

    for (oid_t database_idx = 0; database_idx < database_count; ++database_idx) {
      auto database = catalog_manager.GetDatabase(database_idx);

      auto table_count = database->GetTableCount();
      if (table_count != database_structures.at(database_idx).size()) {
        LOG_ERROR("incorrect database state!");
        exit(EXIT_FAILURE);
      }

      for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
  
        storage::DataTable *target_table = database->GetTable(table_idx);

        size_t tile_group_count = database_structures.at(database_idx).at(table_idx);
        printf("recover tile group: <%lu, %lu>\n", target_table->GetTileGroupCount(), tile_group_count);
        for (size_t tg_id = target_table->GetTileGroupCount(); tg_id < tile_group_count; ++tg_id) {
          target_table->AddDefaultTileGroup(tg_id % NUM_PREALLOCATION);
        }
        printf("final table tile group count = %lu\n", target_table->GetTileGroupCount());
      }
    }
  }

  void PhysicalCheckpointManager::RecoverTable(storage::DataTable *target_table, const size_t &thread_id, const cid_t &current_cid, FileHandle *file_handles) {

    auto schema = target_table->GetSchema();

    for (size_t virtual_checkpointer_id = 0; virtual_checkpointer_id < max_checkpointer_count_; virtual_checkpointer_id++) {

      if (virtual_checkpointer_id % recovery_thread_count_ != thread_id) {
        continue;
      }

      FileHandle &file_handle = file_handles[virtual_checkpointer_id];
      
      char *buffer = new char[4096];
      size_t tuple_size = 0;


      while (true) {
        
        ItemPointer item_pointer;
        
        if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) &(item_pointer.block), sizeof(oid_t)) == false) {
          LOG_TRACE("Reach the end of the log file");
          break;
        }
        if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) &(item_pointer.offset), sizeof(oid_t)) == false) {
          LOG_TRACE("Reach the end of the log file");
          break;
        }

        // printf("item_pointer : (%d, %d)", (int)item_pointer.block, (int)item_pointer.offset);

        // Read the frame length
        if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) &tuple_size, sizeof(size_t)) == false) {
          LOG_TRACE("Reach the end of the log file");
          break;
        }
        // printf("tuple size = %lu\n", tuple_size);

        if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) buffer, tuple_size) == false) {
          LOG_ERROR("Unexpected file eof");
          // TODO: How to handle damaged log file?
        }
        
        CopySerializeInputBE record_decode((const void *) buffer, tuple_size);

        std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
        tuple->DeserializeFrom(record_decode, this->recovery_pool_.get());
        
        auto &manager = catalog::Manager::GetInstance();
        auto tile_group = manager.GetTileGroup(item_pointer.block);
        auto tile_group_header = tile_group->GetHeader();

        tile_group->InsertTupleFromCheckpoint(item_pointer.offset, tuple.get());

        ItemPointer *itemptr_ptr = nullptr;
        target_table->InsertTupleFromCheckpoint(item_pointer, tuple.get(), &itemptr_ptr);
        tile_group_header->SetBeginCommitId(item_pointer.offset, current_cid);
        tile_group_header->SetEndCommitId(item_pointer.offset, MAX_CID);
        tile_group_header->SetTransactionId(item_pointer.offset, INITIAL_TXN_ID);

        // FILE *fp = fopen("in_file.txt", "a");
        // for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
        //   int value = ValuePeeker::PeekAsInteger(tuple->GetValue(i));
        //   fprintf(stdout, "%d, ", value);
        // }
        // fprintf(stdout, "\n");
        // fclose(fp);

      } // end while

      delete[] buffer;
      buffer = nullptr;

    } // end for
  }


  void PhysicalCheckpointManager::CheckpointTable(storage::DataTable *target_table, const size_t &tile_group_count, const size_t &thread_id, const cid_t &begin_cid, FileHandle *file_handles) {

    // auto schema = target_table->GetSchema();

    CopySerializeOutput output_buffer;

    for (size_t current_tile_group_offset = 0; current_tile_group_offset < tile_group_count; ++current_tile_group_offset) {
    
      // shuffle workloads
      if (current_tile_group_offset % max_checkpointer_count_ % checkpointer_count_ != thread_id) {
        continue;
      }

      size_t virtual_checkpointer_id = current_tile_group_offset % max_checkpointer_count_;
    
      FileHandle &file_handle = file_handles[virtual_checkpointer_id];

      auto tile_group =
          target_table->GetTileGroup(current_tile_group_offset);

      oid_t tile_group_id = tile_group->GetTileGroupId();

      auto tile_group_header = tile_group->GetHeader();

      oid_t active_tuple_count = tile_group->GetNextTupleSlot();
    
      for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {

        // check tuple version visibility
        if (IsVisible(tile_group_header, tuple_id, begin_cid) == true) {
          
          // persist this version.
          expression::ContainerTuple<storage::TileGroup> container_tuple(
            tile_group.get(), tuple_id);
          
          // FILE *fp = fopen("out_file.txt", "a");
          // for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
          //   int value = ValuePeeker::PeekAsInteger(container_tuple.GetValue(i));
          //   fprintf(fp, "%d, ", value);
          // }
          // fprintf(fp, "\n");
          // fclose(fp);

          fwrite((const void *) (&tile_group_id), sizeof(tile_group_id), 1, file_handle.file);
          fwrite((const void *) (&tuple_id), sizeof(tuple_id), 1, file_handle.file);

          output_buffer.Reset();

          container_tuple.SerializeTo(output_buffer);

          size_t output_buffer_size = output_buffer.Size();
          fwrite((const void *) (&output_buffer_size), sizeof(output_buffer_size), 1, file_handle.file);
          fwrite((const void *) (output_buffer.Data()), output_buffer_size, 1, file_handle.file);

        } // end if isvisible
      }   // end for
    }     // end while
    
  }

}
}