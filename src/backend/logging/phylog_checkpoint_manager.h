//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// phylog_checkpoint_manager.h
//
// Identification: src/backend/logging/checkpoint/phylog_checkpoint_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <thread>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/logging/logging_util.h"

#include "backend/logging/checkpoint_manager.h"

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

class PhyLogCheckpointManager : public CheckpointManager {
  // Deleted functions
  PhyLogCheckpointManager(const PhyLogCheckpointManager &) = delete;
  PhyLogCheckpointManager &operator=(const PhyLogCheckpointManager &) = delete;
  PhyLogCheckpointManager(PhyLogCheckpointManager &&) = delete;
  PhyLogCheckpointManager &operator=(const PhyLogCheckpointManager &&) = delete;


public:
  PhyLogCheckpointManager() : is_running_(false), checkpoint_interval_(DEFAULT_CHECKPOINT_INTERVAL) {
    max_checkpointer_count_ = std::thread::hardware_concurrency() / 2;
    printf("max checkpointer count = %d\n", (int)max_checkpointer_count_);
  }
  virtual ~PhyLogCheckpointManager() {}

  static PhyLogCheckpointManager& GetInstance() {
    static PhyLogCheckpointManager checkpoint_manager;
    return checkpoint_manager;
  }

  virtual void SetDirectories(const std::vector<std::string> &checkpoint_dirs) override {
    if (checkpoint_dirs.size() > 0) {
      ckpt_pepoch_dir_ = checkpoint_dirs.at(0);
    }
    // check the existence of checkpoint directories.
    // if not exists, then create the directory.
    for (auto checkpoint_dir : checkpoint_dirs) {
      if (LoggingUtil::CheckDirectoryExistence(checkpoint_dir.c_str()) == false) {
        LOG_INFO("Checkpoint directory %s is not accessible or does not exist", checkpoint_dir.c_str());
        bool res = LoggingUtil::CreateDirectory(checkpoint_dir.c_str(), 0700);
        if (res == false) {
          LOG_ERROR("Cannot create directory: %s", checkpoint_dir.c_str());
        }
      }
    }

    checkpoint_dirs_ = checkpoint_dirs;
    checkpointer_count_ = checkpoint_dirs_.size();
  }

  virtual void SetCheckpointInterval(const int &checkpoint_interval) override {
    checkpoint_interval_ = checkpoint_interval;
  }

  virtual void StartCheckpointing() override;
  
  virtual void StopCheckpointing() override;

private:
  void Running();

  void PerformCheckpoint(const cid_t &begin_cid);

  void PerformCheckpointThread(const size_t &thread_id, const cid_t &begin_cid, const std::vector<std::vector<size_t>> &database_structures, FileHandle ***file_handles);

  void CheckpointTable(storage::DataTable *, const size_t &tile_group_count, const size_t &thread_id, const cid_t &begin_cid, FileHandle *file_handles);

  // Visibility check
  bool IsVisible(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id, const cid_t &begin_cid);

  std::string GetCheckpointFileFullPath(size_t checkpointer_id, size_t virtual_checkpointer_id, oid_t database_idx, oid_t table_idx, size_t epoch_id) {
    return checkpoint_dirs_.at(checkpointer_id) + "/" + checkpoint_filename_prefix_ + "_" + std::to_string(virtual_checkpointer_id) + "_" + std::to_string(database_idx) + "_" + std::to_string(table_idx) + "_" + std::to_string(epoch_id);
  }


private:
  bool is_running_;
  int checkpoint_interval_;
  const int DEFAULT_CHECKPOINT_INTERVAL = 30;
  
  size_t checkpointer_count_;
  std::vector<std::string> checkpoint_dirs_;

  size_t max_checkpointer_count_;

  const std::string checkpoint_filename_prefix_ = "checkpoint";
  
  std::unique_ptr<std::thread> central_checkpoint_thread_;

  std::string ckpt_pepoch_dir_;

  const std::string ckpt_pepoch_filename_ = "checkpoint_pepoch";
};

}
}
