//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpointer.h
//
// Identification: src/backend/logging/checkpoint/checkpointer.h
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
#define CHECKPOINT_DIR "/tmp/"

namespace peloton {
namespace logging {

class Checkpointer {
  // Deleted functions
  Checkpointer(const Checkpointer &) = delete;
  Checkpointer &operator=(const Checkpointer &) = delete;
  Checkpointer(Checkpointer &&) = delete;
  Checkpointer &operator=(const Checkpointer &&) = delete;


public:
  CheckpointManager(int thread_count) 
    : is_running_(true),
      checkpoint_thread_count_(thread_count) {}
  ~CheckpointManager() {}

  static CheckpointManager& GetInstance(int thread_count = 1) {
    static CheckpointManager checkpoint_manager(thread_count);
    return checkpoint_manager;
  }

  void SetDirectory(const std::string &dir_prefix) {
    checkpoint_dir_prefix_ = dir_prefix;
  }

  void StartCheckpointing();
  
  void StopCheckpointing();


private:
  void Running();

  void PerformCheckpoint();

  void CheckpointTable(cid_t begin_cid, storage::Database *);

  std::string GetCheckpointFileFullPath(oid_t database_idx, oid_t table_idx, cid_t begin_cid) {
    return checkpoint_dir_ + "/" + checkpoint_filename_prefix_ + std::to_string(database_idx) + "_" + std::to_string(table_idx) + "_" + std::to_string(begin_cid);
  }

  std::unique_ptr<executor::LogicalTile> Scan(
      std::shared_ptr<storage::TileGroup> tile_group,
      const std::vector<oid_t> &column_ids, cid_t start_cid) {
    // Retrieve next tile group.
    auto tile_group_header = tile_group->GetHeader();

    oid_t active_tuple_count = tile_group->GetNextTupleSlot();

    LOG_TRACE("Active tuple count in tile group: %d", active_tuple_count);

    // Construct position list by looping through tile group
    // and applying the predicate.
    std::vector<oid_t> position_list;
    for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
      // check transaction visibility
      if (IsVisible(tile_group_header, tuple_id, start_cid)) {
        position_list.push_back(tuple_id);
      }
    }

    // Construct logical tile.
    std::unique_ptr<executor::LogicalTile> logical_tile(
        executor::LogicalTileFactory::GetTile());
    logical_tile->AddColumns(tile_group, column_ids);
    logical_tile->AddPositionList(std::move(position_list));

    return std::move(logical_tile);
  }


  // Visibility check
  bool IsVisible(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id);


private:
  bool is_running_;
  int checkpoint_thread_count_;
  int checkpoint_interval_;

  std::string checkpoint_dir_ = CHECKPOINT_DIR;
  const std::string checkpoint_filename_prefix_ = "checkpoint_";

  std::unique_ptr<std::thread> checkpoint_thread_;
};

}
}
