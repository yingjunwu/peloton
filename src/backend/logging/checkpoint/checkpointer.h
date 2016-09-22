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

namespace peloton {
namespace logging {

class Checkpointer {
  // Deleted functions
  Checkpointer(const Checkpointer &) = delete;
  Checkpointer &operator=(const Checkpointer &) = delete;
  Checkpointer(Checkpointer &&) = delete;
  Checkpointer &operator=(const Checkpointer &&) = delete;


public:
  CheckpointManager() {}
  ~CheckpointManager() {}

  static CheckpointManager& GetInstance() {
    static CheckpointManager checkpoint_manager();
    return checkpoint_manager;
  }

  void StartCheckpointing();
  
  void StopCheckpointing();

  void RegisterTable(oid_t table_id);

  void Running();

  void DoCheckpoint();


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
  bool IsVisible(const storage::TileGroupHeader *const tile_group_header, const oid_t &tuple_id) {
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



private:
  // Private Functions
  void CreateFile() {
    // open checkpoint file and file descriptor
    std::string file_name = ConcatFileName(checkpoint_dir, ++checkpoint_version);
    bool success =
        LoggingUtil::InitFileHandle(file_name.c_str(), file_handle_, "ab");
    if (!success) {
      PL_ASSERT(false);
      return;
    }
    LOG_TRACE("Created a new checkpoint file: %s", file_name.c_str());
  }



private:
  bool is_working_ = false;
  size_t current_eid_;
  std::unique_ptr<std::thread> checkpoint_thread_;
};

}
}
