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
 * dir_name + "/" + prefix + "_" + database_id + "_" + table_id + "_" + epoch_id
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

class Checkpointer {
  // Deleted functions
  Checkpointer(const Checkpointer &) = delete;
  Checkpointer &operator=(const Checkpointer &) = delete;
  Checkpointer(Checkpointer &&) = delete;
  Checkpointer &operator=(const Checkpointer &&) = delete;


public:
  Checkpointer(size_t thread_count) 
    : is_running_(true),
      checkpoint_thread_count_(thread_count) {}
  ~Checkpointer() {}

  static Checkpointer& GetInstance(size_t thread_count) {
    static Checkpointer checkpointer(thread_count);
    return checkpointer;
  }

  void SetDirectory(const std::string &checkpoint_dir) {
    checkpoint_dir_ = checkpoint_dir;
  }

  void StartCheckpointing();
  
  void StopCheckpointing();

private:
  void Running();

  void PerformCheckpoint();

  void CheckpointTable(cid_t begin_cid, storage::DataTable *);

  std::string GetCheckpointFileFullPath(oid_t database_idx, oid_t table_idx, cid_t begin_cid) {
    return checkpoint_dir_ + "/" + checkpoint_filename_prefix_ + "_" + std::to_string(database_idx) + "_" + std::to_string(table_idx) + "_" + std::to_string(begin_cid);
  }

  // Visibility check
  bool IsVisible(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id);


private:
  bool is_running_;
  int checkpoint_interval_;
  
  size_t checkpoint_thread_count_;
  std::string checkpoint_dir_ = TMP_DIR;

  const std::string checkpoint_filename_prefix_ = "checkpoint";
  
  std::unique_ptr<std::thread> checkpoint_thread_;
};

}
}
