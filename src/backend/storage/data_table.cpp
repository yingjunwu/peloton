//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table.cpp
//
// Identification: src/backend/storage/data_table.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <mutex>
#include <utility>
#include <thread>

#include "backend/brain/clusterer.h"
#include "backend/storage/data_table.h"

#include "backend/storage/database.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/index/index.h"
#include "backend/catalog/foreign_key.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tile_group_factory.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/index/index_factory.h"

namespace peloton {
namespace storage {

DataTable::DataTable(catalog::Schema *schema, const std::string &table_name,
                     const oid_t &database_oid, const oid_t &table_oid,
                     const size_t &tuples_per_tilegroup, const bool own_schema,
                     const bool adapt_table)
    : AbstractTable(database_oid, table_oid, table_name, schema, own_schema),
      tuples_per_tilegroup_(tuples_per_tilegroup),
      adapt_table_(adapt_table) {

  LOG_TRACE("Data table %u created", table_oid);

  // Create a tile group.
  for (size_t i = 0; i < NUM_PREALLOCATION; ++i) {
    AddDefaultTileGroup(i);
  }
}

DataTable::~DataTable() {
  // clean up tile groups by dropping the references in the catalog
  oid_t tile_group_count = GetTileGroupCount();
  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
       tile_group_itr++) {
    
    tile_group_lock_.ReadLock();
    auto tile_group_id = tile_groups_.at(tile_group_itr);
    tile_group_lock_.Unlock();

    catalog::Manager::GetInstance().DropTileGroup(tile_group_id);
  }

  // clean up indices
  for (auto index : indexes_) {
    delete index;
  }

  // clean up foreign keys
  for (auto foreign_key : foreign_keys_) {
    delete foreign_key;
  }
  // AbstractTable cleans up the schema
  LOG_TRACE("Data table %u destroyed", table_oid);

}

//===--------------------------------------------------------------------===//
// TUPLE HELPER OPERATIONS
//===--------------------------------------------------------------------===//

// this function is called when update/delete/insert is performed.
// this function first checks whether there's available slot.
// if yes, then directly return the available slot.
// in particular, if this is the last slot, a new tile group is created.
// if there's no available slot, then some other threads must be allocating a
// new tile group.
// we just wait until a new tuple slot in the newly allocated tile group is
// available.
ItemPointer DataTable::FillInEmptyTupleSlot(const storage::Tuple *tuple) {
  // assert(tuple);
  // if (check_constraint == true && CheckConstraints(tuple) == false) {
  //   return INVALID_ITEMPOINTER;
  // }
  //=============== garbage collection==================
  // check if there are recycled tuple slots
  auto &gc_manager = gc::GCManagerFactory::GetInstance();
  auto free_item_pointer = gc_manager.ReturnFreeSlot(this->table_oid);
  if (free_item_pointer.IsNull() == false) {
    // auto tg = catalog::Manager::GetInstance().GetTileGroup(free_item_pointer.block);
    // tg->CopyTuple(tuple, free_item_pointer.offset);
    return free_item_pointer;
  }
  //====================================================
  size_t tg_seq_id = concurrency::current_txn->GetTransactionId() % NUM_PREALLOCATION;
  // std::hash<std::thread::id>()(std::this_thread::get_id()) % NUM_PREALLOCATION;
  // size_t tg_seq_id = 0;
  std::shared_ptr<storage::TileGroup> tile_group;
  oid_t tuple_slot = INVALID_OID;
  oid_t tile_group_id = INVALID_OID;

  // get valid tuple.
  while (true) {
    // get the last tile group.
    tile_group = last_tile_groups_[tg_seq_id];
//    tile_group = GetTileGroup(tile_group_count_ - 1);

    tuple_slot = tile_group->InsertTuple(tuple);

    // now we have already obtained a new tuple slot.
    if (tuple_slot != INVALID_OID) {
      tile_group_id = tile_group->GetTileGroupId();
      break;
    }
  }
  // if this is the last tuple slot we can get
  // then create a new tile group
  if (tuple_slot == tile_group->GetAllocatedTupleCount() - 1) {
    AddDefaultTileGroup(tg_seq_id);
  }

  LOG_TRACE("tile group count: %lu, tile group id: %u, address: %p",
            tile_group_count_.load(), tile_group->GetTileGroupId(), tile_group.get());

  // Set tuple location
  ItemPointer location(tile_group_id, tuple_slot);

  return location;
}

//===--------------------------------------------------------------------===//
// INSERT
//===--------------------------------------------------------------------===//
ItemPointer DataTable::InsertEmptyVersion() {
  // First, do integrity checks and claim a slot
  ItemPointer location = FillInEmptyTupleSlot(nullptr);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  // Index checks and updates
  // if (InsertInSecondaryIndexes(tuple, location) == false) {
  //   LOG_TRACE("Index constraint violated when inserting secondary index");
  //   return INVALID_ITEMPOINTER;
  // }

  // ForeignKey checks
  // if (CheckForeignKeyConstraints(tuple) == false) {
  //   LOG_TRACE("ForeignKey constraint violated");
  //   return INVALID_ITEMPOINTER;
  // }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseNumberOfTuplesBy(1);
  return location;
}

ItemPointer DataTable::AcquireVersion() {
  // First, claim a slot
  ItemPointer location = FillInEmptyTupleSlot(nullptr);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseNumberOfTuplesBy(1);
  return location;
}

bool DataTable::InstallVersion(const AbstractTuple *tuple, const ItemPointer &location,
                               const TargetList *targets_ptr, 
                               ItemPointer *master_ptr) {
  // Index checks and updates
  if ( concurrency::TransactionManagerFactory::IsRB() == false
      && index::IndexFactory::GetSecondaryIndexType() == SECONDARY_INDEX_TYPE_TUPLE) {
    if (InsertInSecondaryTupleIndexes(tuple, targets_ptr, master_ptr) == false) {
      LOG_TRACE("Index constraint violated when inserting secondary index");
      return false;
    }
  } else {
    if (InsertInSecondaryIndexes(tuple, location) == false) {
      LOG_TRACE("Index constraint violated when inserting secondary index");
      return false;
    }
  }

  // ForeignKey checks
  // if (CheckForeignKeyConstraints(tuple) == false) {
  //   LOG_TRACE("ForeignKey constraint violated");
  //   return false;
  // }

  // Write down the master version's pointer into tile group header
  auto tg_hdr = catalog::Manager::GetInstance().GetTileGroup(location.block)->GetHeader();
  tg_hdr->SetMasterPointer(location.offset, master_ptr);

  return true;
}


ItemPointer DataTable::InsertTuple(const storage::Tuple *tuple, ItemPointer **itemptr_ptr) {
  // First, do integrity checks and claim a slot
  ItemPointer *temp_ptr = nullptr;

  // Upper layer don't want to know information about index
  if (itemptr_ptr == nullptr) {
    itemptr_ptr = &temp_ptr;
  }

  ItemPointer location = FillInEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  // Index checks and updates
  if (InsertInIndexes(tuple, location, itemptr_ptr) == false) {
    LOG_TRACE("Index constraint violated");
    gc::GCManagerFactory::GetInstance().DirectRecycleTuple(table_oid, location);
    return INVALID_ITEMPOINTER;
  }

  // ForeignKey checks
  // if (CheckForeignKeyConstraints(tuple) == false) {
  //   LOG_TRACE("ForeignKey constraint violated");
  //   return INVALID_ITEMPOINTER;
  // }

  // Write down the master version's pointer into tile group header
  auto tg_hdr = catalog::Manager::GetInstance().GetTileGroup(location.block)->GetHeader();
  tg_hdr->SetMasterPointer(location.offset, *itemptr_ptr);

  if (indexes_.size() != 0) {
    PL_ASSERT((*itemptr_ptr)->block == location.block && (*itemptr_ptr)->offset == location.offset);
  }

  // Increase the table's number of tuples by 1
  IncreaseNumberOfTuplesBy(1);
  // Increase the indexes' number of tuples by 1 as well
  for (auto index : indexes_)
    index->IncreaseNumberOfTuplesBy(1);

  return location;
}

// For RB
ItemPointer DataTable::InsertTuple(const storage::Tuple *tuple, index::RBItemPointer **rb_itemptr_ptr) {
  assert(rb_itemptr_ptr != nullptr);

  ItemPointer location = FillInEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  // Index checks and updates
  if (InsertInIndexes(tuple, location, rb_itemptr_ptr) == false) {
    LOG_TRACE("Index constraint violated");
    return INVALID_ITEMPOINTER;
  }

  // ForeignKey checks
  // if (CheckForeignKeyConstraints(tuple) == false) {
  //   LOG_TRACE("ForeignKey constraint violated");
  //   return INVALID_ITEMPOINTER;
  // }

  // Increase the table's number of tuples by 1
  IncreaseNumberOfTuplesBy(1);
  // Increase the indexes' number of tuples by 1 as well
  for (auto index : indexes_)
    index->IncreaseNumberOfTuplesBy(1);

  return location;
}

// For RB
bool DataTable::InsertInIndexes(const storage::Tuple *tuple,
                                ItemPointer location, index::RBItemPointer **rb_itempointer_ptr) {
  *rb_itempointer_ptr = nullptr;

  ItemPointer *temp_itemptr = nullptr;

  int index_count = GetIndexCount();
  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
      std::bind(&concurrency::TransactionManager::IsOccupied,
                &transaction_manager, std::placeholders::_1);

  // (A) Check existence for primary/unique indexes
  // FIXME Since this is NOT protected by a lock, concurrent insert may happen.
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
      case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY: {
        // TODO: get unique tuple from primary index.
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
        if (index->CondInsertEntry(key.get(), location, fn, &temp_itemptr) == false) {
          return false;
        }
      } break;
      case INDEX_CONSTRAINT_TYPE_UNIQUE: {
        // TODO: get unique tuple from primary index.
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
        if (index->CondInsertEntry(key.get(), location, fn, rb_itempointer_ptr) == false) {
          return false;
        }

      } break;

      case INDEX_CONSTRAINT_TYPE_DEFAULT:
      default:
        index->InsertEntry(key.get(), location, rb_itempointer_ptr);
        break;
    }
    LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }

  return true;
}



/**
 * @brief Insert a tuple into all indexes. If index is primary/unique,
 * check visibility of existing
 * index entries.
 * @warning This still doesn't guarantee serializability.
 *
 * @returns True on success, false if a visible entry exists (in case of
 *primary/unique).
 */
bool DataTable::InsertInIndexes(const storage::Tuple *tuple,
                                ItemPointer location, ItemPointer ** itempointer_ptr) {
  *itempointer_ptr = nullptr;
  ItemPointer *temp_ptr = nullptr;

  int index_count = GetIndexCount();
  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
      std::bind(&concurrency::TransactionManager::IsOccupied,
                &transaction_manager, std::placeholders::_1);

  // (A) Check existence for primary/unique indexes
  // FIXME Since this is NOT protected by a lock, concurrent insert may happen.
  if (index::IndexFactory::GetSecondaryIndexType() == SECONDARY_INDEX_TYPE_TUPLE) {
    *itempointer_ptr = new ItemPointer(location);

    bool res = true;
    int success_count = 0;

    for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
      auto index = GetIndex(index_itr);
      auto index_schema = index->GetKeySchema();
      auto indexed_columns = index_schema->GetIndexedColumns();
      std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
      key->SetFromTuple(tuple, indexed_columns, index->GetPool());

      switch (index->GetIndexType()) {
        case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY: {
          // TODO: get unique tuple from primary index.
          // if in this index there has been a visible or uncommitted
          // <key, location> pair, this constraint is violated
          res = index->CondInsertEntryInTupleIndex(key.get(), *itempointer_ptr, fn);
        }
          break;
        case INDEX_CONSTRAINT_TYPE_UNIQUE: {
          // TODO: get unique tuple from primary index.
          // if in this index there has been a visible or uncommitted
          // <key, location> pair, this constraint is violated
          res = index->CondInsertEntryInTupleIndex(key.get(), *itempointer_ptr, fn);
        }
          break;

        case INDEX_CONSTRAINT_TYPE_DEFAULT:
        default:
          index->InsertEntryInTupleIndex(key.get(), *itempointer_ptr);
          break;
      }

      // Handle failure
      if (res == false) {
        // If some of the indexes have been inserted,
        // the pointer has a chance to be dereferenced by readers and it can not be deleted
        if (success_count == 0) {
          delete itempointer_ptr;
        }
        *itempointer_ptr = nullptr;
        return false;
      } else {
        success_count += 1;
      }
      LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
    }

  } else {
    for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
      auto index = GetIndex(index_itr);
      auto index_schema = index->GetKeySchema();
      auto indexed_columns = index_schema->GetIndexedColumns();
      std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
      key->SetFromTuple(tuple, indexed_columns, index->GetPool());

      switch (index->GetIndexType()) {
        case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY: {
          // TODO: get unique tuple from primary index.
          // if in this index there has been a visible or uncommitted
          // <key, location> pair, this constraint is violated
          if (index->CondInsertEntry(key.get(), location, fn, itempointer_ptr) == false) {
            return false;
          }
        }
          break;
        case INDEX_CONSTRAINT_TYPE_UNIQUE: {
          // TODO: get unique tuple from primary index.
          // if in this index there has been a visible or uncommitted
          // <key, location> pair, this constraint is violated
          if (index->CondInsertEntry(key.get(), location, fn, &temp_ptr) == false) {
            return false;
          }

        }
          break;

        case INDEX_CONSTRAINT_TYPE_DEFAULT:
        default:
          index->InsertEntry(key.get(), location);
          break;
      }
      LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
    }
  }

  return true;
}

bool DataTable::InsertInSecondaryTupleIndexes(const AbstractTuple *tuple, const TargetList *targets_ptr, ItemPointer *master_ptr) {
  int index_count = GetIndexCount();
  auto &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
    std::bind(&concurrency::TransactionManager::IsOccupied,
              &transaction_manager, std::placeholders::_1);

  // Transaform the target list into a hash set
  std::unordered_set<oid_t> targets_set;
  for (auto target : *targets_ptr) {
    targets_set.insert(target.first);
  }


  // (A) Check existence for primary/unique indexes
  // FIXME Since this is NOT protected by a lock, concurrent insert may happen.
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    if (index->GetIndexType() == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY) {
//      fprintf(stdout, "skip primary index\n");
      continue;
    }

    // Check if we need to update the secondary index
    bool updated = false;
    for (auto col : indexed_columns) {
      if (targets_set.find(col) != targets_set.end()) {
        updated = true;
        break;
      }
    }

    // If attributes on key are not updated, skip the index update
    if (updated == false) {
//      fprintf(stdout, "No need to update sindex\n");
      continue;
    }

    // Key attributes are updated, insert a new entry in all secondary index
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));

    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
      case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY:
        break;
      case INDEX_CONSTRAINT_TYPE_UNIQUE: {
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
//        fprintf(stdout, "Haha update uniq sindex\n");
        if (index->CondInsertEntryInTupleIndex(key.get(), master_ptr, fn) == false) {
          return false;
        }
      } break;

      case INDEX_CONSTRAINT_TYPE_DEFAULT:
      default:
//        fprintf(stdout, "Haha update sindex\n");
        index->InsertEntryInTupleIndex(key.get(), master_ptr);
        break;
    }
    LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }
  return true;
}


bool DataTable::InsertInSecondaryIndexes(const AbstractTuple *tuple,
                                         ItemPointer location) {
  int index_count = GetIndexCount();
  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
      std::bind(&concurrency::TransactionManager::IsOccupied,
                &transaction_manager, std::placeholders::_1);

  // (A) Check existence for primary/unique indexes
  // FIXME Since this is NOT protected by a lock, concurrent insert may happen.
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));

    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    ItemPointer *itempointer_ptr = nullptr;

    switch (index->GetIndexType()) {
      case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY:
        break;
      case INDEX_CONSTRAINT_TYPE_UNIQUE: {
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
        if (index->CondInsertEntry(key.get(), location, fn, &itempointer_ptr) == false) {
          return false;
        }
      } break;

      case INDEX_CONSTRAINT_TYPE_DEFAULT:
      default:
        index->InsertEntry(key.get(), location);
        break;
    }
    LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }
  return true;
}

//===--------------------------------------------------------------------===//
// STATS
//===--------------------------------------------------------------------===//

/**
 * @brief Increase the number of tuples in this table
 * @param amount amount to increase
 */
void DataTable::IncreaseNumberOfTuplesBy(const float &amount) {
  number_of_tuples_ += amount;
  dirty_ = true;
}

/**
 * @brief Decrease the number of tuples in this table
 * @param amount amount to decrease
 */
void DataTable::DecreaseNumberOfTuplesBy(const float &amount) {
  number_of_tuples_ -= amount;
  dirty_ = true;
}

/**
 * @brief Set the number of tuples in this table
 * @param num_tuples number of tuples
 */
void DataTable::SetNumberOfTuples(const float &num_tuples) {
  number_of_tuples_ = num_tuples;
  dirty_ = true;
}

/**
 * @brief Get the number of tuples in this table
 * @return number of tuples
 */
float DataTable::GetNumberOfTuples() const { return number_of_tuples_; }

/**
 * @brief return dirty flag
 * @return dirty flag
 */
bool DataTable::IsDirty() const { return dirty_; }

/**
 * @brief Reset dirty flag
 */
void DataTable::ResetDirty() { dirty_ = false; }

//===--------------------------------------------------------------------===//
// TILE GROUP
//===--------------------------------------------------------------------===//

TileGroup *DataTable::GetTileGroupWithLayout(
    const column_map_type &partitioning) {
  std::vector<catalog::Schema> schemas;
  oid_t tile_group_id = INVALID_OID;

  tile_group_id = catalog::Manager::GetInstance().GetNextOid();

  // Figure out the columns in each tile in new layout
  std::map<std::pair<oid_t, oid_t>, oid_t> tile_column_map;
  for (auto entry : partitioning) {
    tile_column_map[entry.second] = entry.first;
  }

  // Build the schema tile at a time
  std::map<oid_t, std::vector<catalog::Column>> tile_schemas;
  for (auto entry : tile_column_map) {
    tile_schemas[entry.first.first].push_back(schema->GetColumn(entry.second));
  }
  for (auto entry : tile_schemas) {
    catalog::Schema tile_schema(entry.second);
    schemas.push_back(tile_schema);
  }

  TileGroup *tile_group = TileGroupFactory::GetTileGroup(
      database_oid, table_oid, tile_group_id, this, schemas, partitioning,
      tuples_per_tilegroup_);

  return tile_group;
}

column_map_type DataTable::GetTileGroupLayout() {
  column_map_type column_map;

  auto col_count = schema->GetColumnCount();

  for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
    column_map[col_itr] = std::make_pair(0, col_itr);
  }

  return column_map;
}

oid_t DataTable::AddDefaultTileGroup(const size_t &tg_seq_id) {
  column_map_type column_map;
  oid_t tile_group_id = INVALID_OID;

  // Figure out the partitioning for given tile group layout
  column_map = GetTileGroupLayout();

  // Create a tile group with that partitioning
  std::shared_ptr<TileGroup> tile_group(GetTileGroupWithLayout(column_map));
  assert(tile_group.get());
  tile_group_id = tile_group->GetTileGroupId();

  LOG_TRACE("Added a tile group ");

  tile_group_lock_.WriteLock();

  // add tile group metadata in locator
  catalog::Manager::GetInstance().AddTileGroup(tile_group_id, tile_group);

  last_tile_groups_[tg_seq_id] = tile_group;

  tile_groups_.push_back(tile_group_id);
  
  tile_group_lock_.Unlock();

  // we must guarantee that the compiler always add tile group before adding
  // tile_group_count_.
  COMPILER_MEMORY_FENCE;

  tile_group_count_++;

  LOG_TRACE("Recording tile group : %u ", tile_group_id);

  return tile_group_id;
}

void DataTable::AddTileGroup(const std::shared_ptr<TileGroup> &tile_group) {
  oid_t tile_group_id = tile_group->GetTileGroupId();

  tile_group_lock_.WriteLock();
  tile_groups_.push_back(tile_group_id);
  tile_group_lock_.Unlock();

  // add tile group in catalog
  catalog::Manager::GetInstance().AddTileGroup(tile_group_id, tile_group);

  // we must guarantee that the compiler always add tile group before adding
  // tile_group_count_.
  COMPILER_MEMORY_FENCE;

  tile_group_count_++;

  LOG_TRACE("Recording tile group : %u ", tile_group_id);
}

size_t DataTable::GetTileGroupCount() const {
  return tile_group_count_;
}

std::shared_ptr<storage::TileGroup> DataTable::GetTileGroup(
    const oid_t &tile_group_offset) const {
  assert(tile_group_offset < GetTileGroupCount());

  tile_group_lock_.ReadLock();
  auto tile_group_id = tile_groups_.at(tile_group_offset);
  tile_group_lock_.Unlock();

  return GetTileGroupById(tile_group_id);
}


std::shared_ptr<storage::TileGroup> DataTable::GetTileGroupById(
    const oid_t &tile_group_id) const {
  auto &manager = catalog::Manager::GetInstance();
  return manager.GetTileGroup(tile_group_id);
}


oid_t DataTable::GetAllCurrentTupleCount() {
  // for (auto index : indexes_) {
  //   LOG_TRACE("index size = %lu", index->GetIndexSize());
  // }

  oid_t count = 0;
  for (auto tile_group_id : tile_groups_) {
    auto tile_group = GetTileGroupById(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    count += tile_group_header->GetCurrentTupleCount();
  }
  return count;
}

const std::string DataTable::GetInfo() const {
  std::ostringstream os;

  //os << "=====================================================\n";
  //os << "TABLE :\n";

  oid_t tile_group_count = GetTileGroupCount();
  //os << "Tile Group Count : " << tile_group_count << "\n";

  oid_t tuple_count = 0;
  oid_t table_id = 0;
  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
       tile_group_itr++) {
    auto tile_group = GetTileGroup(tile_group_itr);
    table_id = tile_group->GetTableId();
    auto tile_tuple_count = tile_group->GetNextTupleSlot();

    //os << "Tile Group Id  : " << tile_group_itr
    //    << " Tuple Count : " << tile_tuple_count << "\n";
    //os << (*tile_group);

    tuple_count += tile_tuple_count;
  }

  os << "Table " << table_id << " Tuple Count :: " << tuple_count << "\n";

  //os << "=====================================================\n";

  return os.str();
}

//===--------------------------------------------------------------------===//
// INDEX
//===--------------------------------------------------------------------===//

void DataTable::AddIndex(index::Index *index) {
  {
    std::lock_guard<std::mutex> lock(tile_group_mutex_);
    indexes_.push_back(index);
  }

  // Update index stats
  auto index_type = index->GetIndexType();
  if (index_type == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY) {
    has_primary_key_ = true;
    primary_idx_oid_ = index->GetOid();
  } else if (index_type == INDEX_CONSTRAINT_TYPE_UNIQUE) {
    unique_constraint_count_++;
  }
}

index::Index *DataTable::GetIndexWithOid(const oid_t &index_oid) const {
  for (auto index : indexes_)
    if (index->GetOid() == index_oid) return index;

  return nullptr;
}

void DataTable::DropIndexWithOid(const oid_t &index_id) {
  {
    std::lock_guard<std::mutex> lock(tile_group_mutex_);

    oid_t index_offset = 0;
    for (auto index : indexes_) {
      if (index->GetOid() == index_id) break;
      index_offset++;
    }
    assert(index_offset < indexes_.size());

    // Drop the index
    indexes_.erase(indexes_.begin() + index_offset);
  }
}

index::Index *DataTable::GetIndex(const oid_t &index_offset) const {
  assert(index_offset < indexes_.size());
  auto index = indexes_.at(index_offset);
  return index;
}

oid_t DataTable::GetIndexCount() const { return indexes_.size(); }

//===--------------------------------------------------------------------===//
// FOREIGN KEYS
//===--------------------------------------------------------------------===//

void DataTable::AddForeignKey(catalog::ForeignKey *key) {
  {
    std::lock_guard<std::mutex> lock(tile_group_mutex_);
    catalog::Schema *schema = this->GetSchema();
    catalog::Constraint constraint(CONSTRAINT_TYPE_FOREIGN,
                                   key->GetConstraintName());
    constraint.SetForeignKeyListOffset(GetForeignKeyCount());
    for (auto fk_column : key->GetFKColumnNames()) {
      schema->AddConstraint(fk_column, constraint);
    }
    // TODO :: We need this one..
    catalog::ForeignKey *fk = new catalog::ForeignKey(*key);
    foreign_keys_.push_back(fk);
  }
}

catalog::ForeignKey *DataTable::GetForeignKey(const oid_t &key_offset) const {
  catalog::ForeignKey *key = nullptr;
  key = foreign_keys_.at(key_offset);
  return key;
}

void DataTable::DropForeignKey(const oid_t &key_offset) {
  {
    std::lock_guard<std::mutex> lock(tile_group_mutex_);
    assert(key_offset < foreign_keys_.size());
    foreign_keys_.erase(foreign_keys_.begin() + key_offset);
  }
}

oid_t DataTable::GetForeignKeyCount() const { return foreign_keys_.size(); }


}  // End storage namespace
}  // End peloton namespace
