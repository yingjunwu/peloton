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

#include <backend/concurrency/transaction_manager_factory.h>
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

  void PhyLogCheckpointManager::RecoverTable(storage::DataTable *target_table, const size_t &thread_id, const cid_t &current_cid, FileHandle *file_handles) {

    auto schema = target_table->GetSchema();

    for (size_t virtual_checkpointer_id = 0; virtual_checkpointer_id < max_checkpointer_count_; virtual_checkpointer_id++) {

      if (virtual_checkpointer_id % recovery_thread_count_ != thread_id) {
        continue;
      }

      FileHandle &file_handle = file_handles[virtual_checkpointer_id];
      
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
        }
        
        CopySerializeInputBE record_decode((const void *) buffer, tuple_size);

        std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
        tuple->DeserializeFrom(record_decode, this->recovery_pools_[thread_id].get());
        
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

        concurrency::TransactionManagerFactory::GetInstance().InitInsertedTupleForRecovery(tile_group_header, location.offset, itemptr_ptr);

        tile_group_header->SetBeginCommitId(location.offset, current_cid);
        tile_group_header->SetEndCommitId(location.offset, MAX_CID);
        tile_group_header->SetTransactionId(location.offset, INITIAL_TXN_ID);

      } // end while

      delete[] buffer;
      buffer = nullptr;

    } // end for
  }


  void PhyLogCheckpointManager::CheckpointTable(storage::DataTable *target_table, const size_t &tile_group_count, const size_t &thread_id, const cid_t &begin_cid, FileHandle *file_handles) {

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
          fwrite((const void *) (&output_buffer_size), sizeof(output_buffer_size), 1, file_handle.file);
          fwrite((const void *) (output_buffer.Data()), output_buffer_size, 1, file_handle.file);

        } // end if isvisible
      }   // end for

      // Call fsync
      LoggingUtil::FFlushFsync(file_handle);

    }     // end while
    
  }

}
}