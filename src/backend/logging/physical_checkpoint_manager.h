//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// physical_checkpoint_manager.h
//
// Identification: src/backend/logging/checkpoint/physical_checkpoint_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/logging/checkpoint_manager.h"

namespace peloton {
namespace logging {

/**
 * checkpoint file name layout :
 * 
 * dir_name + "/" + prefix + "_" + checkpointer_id + "_" + database_id + "_" + table_id + "_" + epoch_id
 *
 *
 * checkpoint file layout :
 *
 *  -----------------------------------------------------------------------------
 *  | tuple_1 | tuple_2 | tuple_3 | ...
 *  -----------------------------------------------------------------------------
 *
 * NOTE: tuple length can be obtained from the table schema.
 *
 */

class PhysicalCheckpointManager : public CheckpointManager {
  // Deleted functions
  PhysicalCheckpointManager(const PhysicalCheckpointManager &) = delete;
  PhysicalCheckpointManager &operator=(const PhysicalCheckpointManager &) = delete;
  PhysicalCheckpointManager(PhysicalCheckpointManager &&) = delete;
  PhysicalCheckpointManager &operator=(const PhysicalCheckpointManager &&) = delete;


public:
  PhysicalCheckpointManager() {}
  virtual ~PhysicalCheckpointManager() {}

  static PhysicalCheckpointManager& GetInstance() {
    static PhysicalCheckpointManager checkpoint_manager;
    return checkpoint_manager;
  }

private:

  virtual void PrepareTables(const std::vector<std::vector<size_t>> &database_structures);  
  virtual void RecoverTable(storage::DataTable *, const size_t &thread_id, const cid_t &begin_cid, FileHandle *file_handles) final;

  virtual void CheckpointTable(storage::DataTable *, const size_t &tile_group_count, const size_t &thread_id, const cid_t &begin_cid, FileHandle *file_handles) final;

};

}
}
