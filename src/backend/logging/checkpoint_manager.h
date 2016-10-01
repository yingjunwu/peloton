//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpoint_manager.h
//
// Identification: src/backend/logging/checkpoint/checkpoint_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "backend/common/types.h"
#include "backend/common/logger.h"

#define CHECKPOINT_INTERVAL 30

namespace peloton {

namespace storage {
  class Database;
  class DataTable;
  class TileGroup;
  class TileGroupHeader;
}

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

class CheckpointManager {
  // Deleted functions
  CheckpointManager(const CheckpointManager &) = delete;
  CheckpointManager &operator=(const CheckpointManager &) = delete;
  CheckpointManager(CheckpointManager &&) = delete;
  CheckpointManager &operator=(const CheckpointManager &&) = delete;


public:
  CheckpointManager(size_t thread_count) 
    : is_running_(true),
      checkpoint_thread_count_(thread_count),
      checkpoint_dirs_(thread_count, TMP_DIR) {}
  ~CheckpointManager() {}

  static CheckpointManager& GetInstance(size_t thread_count) {
    static CheckpointManager checkpoint_manager(thread_count);
    return checkpoint_manager;
  }

  void SetDirectories(const std::vector<std::string> &checkpoint_dirs) {
    checkpoint_dirs_ = checkpoint_dirs;
  }

  void StartCheckpointing();
  
  void StopCheckpointing();

private:
  void Running();

  void PerformCheckpoint();

  void CheckpointTable(cid_t begin_cid, storage::DataTable *);

  std::string GetCheckpointFileFullPath(size_t checkpointer_id, oid_t database_idx, oid_t table_idx, cid_t begin_cid) {
    return checkpoint_dirs_.at(checkpointer_id) + "/" + checkpoint_filename_prefix_ + "_" + std::to_string(checkpointer_id) + "_" + std::to_string(database_idx) + "_" + std::to_string(table_idx) + "_" + std::to_string(begin_cid);
  }

  // Visibility check
  bool IsVisible(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id);


private:
  bool is_running_;
  int checkpoint_interval_;
  
  size_t checkpoint_thread_count_;
  std::vector<std::string> checkpoint_dirs_;

  const std::string checkpoint_filename_prefix_ = "checkpoint";
  
  std::unique_ptr<std::thread> checkpoint_thread_;
};

}
}
