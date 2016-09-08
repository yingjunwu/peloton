//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table.h
//
// Identification: src/backend/storage/data_table.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <map>
#include <mutex>

#include "backend/brain/sample.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/catalog/foreign_key.h"
#include "backend/storage/abstract_table.h"
#include "backend/common/platform.h"
#include "backend/index/index.h"


//===--------------------------------------------------------------------===//
// Configuration Variables
//===--------------------------------------------------------------------===//



namespace peloton {


namespace index {
  class Index;
}

namespace storage {

class Tuple;
class TileGroup;

//===--------------------------------------------------------------------===//
// EpochDataTable
//===--------------------------------------------------------------------===//

/**
 * Represents a group of tile groups logically vertically contiguous.
 *
 * <Tile Group 1>
 * <Tile Group 2>
 * ...
 * <Tile Group n>
 *
 */
class EpochDataTable : public AbstractTable {
  friend class TileGroup;
  friend class TileGroupFactory;
  friend class TableFactory;

  EpochDataTable() = delete;
  EpochDataTable(EpochDataTable const &) = delete;

 public:
  // Table constructor
  EpochDataTable(catalog::Schema *schema, const std::string &table_name,
            const oid_t &database_oid, const oid_t &table_oid,
            const size_t &tuples_per_tilegroup, const bool own_schema,
            const bool adapt_table);

  ~EpochDataTable();

  //===--------------------------------------------------------------------===//
  // TUPLE OPERATIONS
  //===--------------------------------------------------------------------===//
  // insert version in table
  ItemPointer InsertEmptyVersion();
  
  // these two functions are designed for reducing memory allocation by performing in-place update.
  // in the update executor, we first acquire a version slot from the data table, and then
  // copy the content into the version. after that, we need to check constraints and then install the version
  // into all the corresponding indexes.
  ItemPointer AcquireVersion();
  bool InstallVersion(const AbstractTuple *tuple, 
    const ItemPointer &location, 
    const TargetList *targets_ptr = nullptr, 
    ItemPointer *index_entry_ptr = nullptr);

  
  // insert tuple in table
  ItemPointer InsertTuple(const Tuple *tuple, ItemPointer **itemptr_ptr = nullptr);

  //===--------------------------------------------------------------------===//
  // TILE GROUP
  //===--------------------------------------------------------------------===//

  // coerce into adding a new tile group with a tile group id
  void AddTileGroupWithOidForRecovery(const oid_t &tile_group_id);

  // add a tile group to table
  void AddTileGroup(const std::shared_ptr<TileGroup> &tile_group);

  // Offset is a 0-based number local to the table.
  std::shared_ptr<storage::TileGroup> GetTileGroup(
      const oid_t &tile_group_offset) const;

  // ID is the global identifier in the entire DBMS
  std::shared_ptr<storage::TileGroup> GetTileGroupById(
      const oid_t &tile_group_id) const;

  size_t GetTileGroupCount() const;

  oid_t GetAllCurrentTupleCount();

  //===--------------------------------------------------------------------===//
  // INDEX
  //===--------------------------------------------------------------------===//

  void AddIndex(index::Index *index);

  index::Index *GetIndexWithOid(const oid_t &index_oid) const;

  void DropIndexWithOid(const oid_t &index_oid);

  index::Index *GetIndex(const oid_t &index_offset) const;

  oid_t GetIndexCount() const;

  //===--------------------------------------------------------------------===//
  // FOREIGN KEYS
  //===--------------------------------------------------------------------===//

  void AddForeignKey(catalog::ForeignKey *key);

  catalog::ForeignKey *GetForeignKey(const oid_t &key_offset) const;

  void DropForeignKey(const oid_t &key_offset);

  oid_t GetForeignKeyCount() const;

  //===--------------------------------------------------------------------===//
  // STATS
  //===--------------------------------------------------------------------===//

  void IncreaseNumberOfTuplesBy(const float &amount);

  void DecreaseNumberOfTuplesBy(const float &amount);

  void SetNumberOfTuples(const float &num_tuples);

  float GetNumberOfTuples() const;

  bool IsDirty() const;

  void ResetDirty();


  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  bool HasPrimaryKey() { return has_primary_key_; }

  bool HasUniqueConstraints() { return (unique_constraint_count_ > 0); }

  bool HasForeignKeys() { return (GetForeignKeyCount() > 0); }

  // Get a string representation for debugging
  const std::string GetInfo() const;

 protected:
  //===--------------------------------------------------------------------===//
  // INTEGRITY CHECKS
  //===--------------------------------------------------------------------===//

  bool CheckNulls(const storage::Tuple *tuple) const;

  bool CheckConstraints(const storage::Tuple *tuple) const;

  // Claim a tuple slot in a tile group
  ItemPointer FillInEmptyTupleSlot(const storage::Tuple *tuple);

  // add a default unpartitioned tile group to table
  oid_t AddDefaultTileGroup(const size_t &tg_seq_id);


  TileGroup *GetTileGroupWithLayout(const column_map_type &partitioning);

  // get a partitioning with given layout type
  column_map_type GetTileGroupLayout();

  //===--------------------------------------------------------------------===//
  // INDEX HELPERS
  //===--------------------------------------------------------------------===//

  // try to insert into the indices
  // the forth argument return the itempointer ptr inserted into the primary index
  bool InsertInIndexes(const storage::Tuple *tuple, ItemPointer location, ItemPointer **itemptr_ptr);


  bool InsertInSecondaryIndexes(const AbstractTuple *tuple,
                                ItemPointer location);

  bool InsertInSecondaryTupleIndexes(const AbstractTuple *tuple,
          const TargetList *targetes_ptr, ItemPointer *masterPtr);

  // check the foreign key constraints
  bool CheckForeignKeyConstraints(const storage::Tuple *tuple);

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // TODO need some policy ?
  // number of tuples allocated per tilegroup
  size_t tuples_per_tilegroup_;

  // TILE GROUPS
  // set of tile groups
  RWLock tile_group_lock_;

  std::shared_ptr<storage::TileGroup> last_tile_groups_[NUM_PREALLOCATION];

  std::vector<oid_t> tile_groups_;

  std::atomic<size_t> tile_group_count_ = ATOMIC_VAR_INIT(0);

  // epoch tile groups
  std::shared_ptr<storage::TileGroup> epoch_tile_groups_[10];
  
  // tile group mutex
  // TODO: don't know why need this mutex --Yingjun
  std::mutex tile_group_mutex_;

  // INDEXES
  std::vector<index::Index *> indexes_;

  // CONSTRAINTS
  std::vector<catalog::ForeignKey *> foreign_keys_;

  // has a primary key ?
  std::atomic<bool> has_primary_key_ = ATOMIC_VAR_INIT(false);

  // # of unique constraints
  std::atomic<oid_t> unique_constraint_count_ = ATOMIC_VAR_INIT(START_OID);

  // # of tuples
  float number_of_tuples_ = 0.0;

  // dirty flag
  bool dirty_ = false;

  // clustering mutex
  std::mutex clustering_mutex_;

  // adapt table
  bool adapt_table_ = true;

  // default partition map for table
  column_map_type default_partition_;

  // samples for clustering
  std::vector<brain::Sample> samples_;
};

}  // End storage namespace
}  // End peloton namespace