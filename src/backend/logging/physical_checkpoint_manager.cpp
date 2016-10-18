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

  void PhysicalCheckpointManager::RecoverCheckpointThread(const size_t &thread_id, const size_t &epoch_id, const std::vector<size_t> &database_structures, FileHandle ***file_handles) {
    cid_t current_cid = (epoch_id << 32) | 0x0;

    concurrency::current_txn = new concurrency::Transaction(thread_id, current_cid);

    auto &catalog_manager = catalog::Manager::GetInstance();

    // loop all databases
    for (oid_t database_idx = 0; database_idx < database_structures.size(); database_idx++) {
      auto database = catalog_manager.GetDatabase(database_idx);
      
      // loop all tables
      for (oid_t table_idx = 0; table_idx < database_structures[database_idx]; table_idx++) {
        // Get the target table
        storage::DataTable *target_table = database->GetTable(table_idx);
        PL_ASSERT(target_table);

        auto schema = target_table->GetSchema();

        for (size_t virtual_checkpointer_id = 0; virtual_checkpointer_id < max_checkpointer_count_; virtual_checkpointer_id++) {
          if (virtual_checkpointer_id % recovery_thread_count_ != thread_id) {
            continue;
          }

          FileHandle &file_handle = file_handles[database_idx][table_idx][virtual_checkpointer_id];
          
          char *buffer = new char[4096];
          size_t tuple_size = 0;
          while (true) {
            // Read the frame length
            if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) &tuple_size, sizeof(size_t)) == false) {
              LOG_TRACE("Reach the end of the log file");
              break;
            }

            if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) buffer, tuple_size) == false) {
              LOG_ERROR("Unexpected file eof");
              // TODO: How to handle damaged log file?
              return false;
            }
            
            CopySerializeInputBE record_decode((const void *) buffer, tuple_size);

            std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
            tuple->DeserializeFrom(record_decode, this->recovery_pool_.get());
            
            // FILE *fp = fopen("in_file.txt", "a");
            // for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
            //   int value = ValuePeeker::PeekAsInteger(tuple->GetValue(i));
            //   fprintf(stdout, "%d, ", value);
            // }
            // fprintf(stdout, "\n");
            // fclose(fp);

            ItemPointer *itemptr_ptr = nullptr;
            ItemPointer location;
            location = target_table->InsertTuple(tuple.get(), &itemptr_ptr);

            if (location.block == INVALID_OID) {
              LOG_ERROR("insertion failed!");
            }
            auto tile_group_header = catalog::Manager::GetInstance().GetTileGroup(location.block)->GetHeader();

            tile_group_header->SetBeginCommitId(location.offset, current_cid);
            tile_group_header->SetEndCommitId(location.offset, MAX_CID);
            tile_group_header->SetTransactionId(location.offset, INITIAL_TXN_ID);
          } // end while

          delete[] buffer;
          buffer = nullptr;

        } // end for

        size_t num_tuples = (size_t)target_table->GetNumberOfTuples();
        printf("num tuples = %lu\n", num_tuples);

      } // end table looping
    } // end database looping

    delete concurrency::current_txn;
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
    

      auto tile_group =
          target_table->GetTileGroup(current_tile_group_offset);
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

          output_buffer.Reset();

          container_tuple.SerializeTo(output_buffer);

          size_t output_buffer_size = output_buffer.Size();
          fwrite((const void *) (&output_buffer_size), sizeof(output_buffer_size), 1, file_handles[virtual_checkpointer_id].file);
          fwrite((const void *) (output_buffer.Data()), output_buffer_size, 1, file_handles[virtual_checkpointer_id].file);

        } // end if isvisible
      }   // end for
    }     // end while
    
  }

}
}