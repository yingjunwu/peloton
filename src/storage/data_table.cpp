/*-------------------------------------------------------------------------
 *
 * table.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/table.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "storage/abstract_table.h"

#include "common/exception.h"
#include "index/index.h"
#include "common/logger.h"
#include "catalog/manager.h"

#include <mutex>

namespace nstore {
namespace storage {

DataTable::DataTable(catalog::Schema *schema,
             Backend *backend,
             std::string table_name)
: AbstractTable(schema, backend),
  table_name(table_name) {

    // Sweat these rocks bitch!
}

DataTable::~DataTable() {

    // clean up indices
    for (auto index : indexes) {
        delete index;
    }
}

void DataTable::AddIndex(index::Index *index) {
    std::lock_guard<std::mutex> lock(table_mutex);
    indexes.push_back(index);
}

ItemPointer DataTable::InsertTuple(txn_id_t transaction_id, const storage::Tuple *tuple, bool update){
    // Insert the tuples in the base table
    ItemPointer location = AbstractTable::InsertTuple(transaction_id, tuple, update);

    // Index checks and updates
    if (update == false) {
        if (TryInsertInIndexes(tuple, location) == false){
            tile_group->ReclaimTuple(tuple_slot);
            throw ConstraintException("Index constraint violated : " + tuple->GetInfo());
            return location;
        }
    }
    else {
        // just do a blind insert
        InsertInIndexes(tuple, location);
        return location;
    }

    return location;
}

void DataTable::InsertInIndexes(const storage::Tuple *tuple, ItemPointer location) {
    for (auto index : indexes) {
        if (index->InsertEntry(tuple, location) == false) {
            throw ExecutorException("Failed to insert tuple into index");
        }
    }
}

bool DataTable::TryInsertInIndexes(const storage::Tuple *tuple, ItemPointer location) {

    int index_count = GetIndexCount();
    for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {

        // No need to check if it does not have unique keys
        if(indexes[index_itr]->HasUniqueKeys() == false) {
        bool status = indexes[index_itr]->InsertEntry(tuple, location);
        if (status == true)
            continue;
        }

        // Check if key already exists
        if (indexes[index_itr]->Exists(tuple) == true) {
            LOG_ERROR("Failed to insert into index %s.%s [%s]",
                      GetName().c_str(), indexes[index_itr]->GetName().c_str(),
                      indexes[index_itr]->GetTypeName().c_str());
        }
        else {
            bool status = indexes[index_itr]->InsertEntry(tuple, location);
            if (status == true)
                continue;
        }

        // Undo insert in other indexes as well
        for (int prev_index_itr = index_itr + 1; prev_index_itr < index_count; ++prev_index_itr) {
            indexes[prev_index_itr]->DeleteEntry(tuple);
        }
        return false;
    }

    return true;
}

void DataTable::DeleteInIndexes(const storage::Tuple *tuple) {
  for(auto index : indexes){
    if (index->DeleteEntry(tuple) == false) {
      throw ExecutorException("Failed to delete tuple from index " +
                              GetName() + "." + index->GetName() + " " +index->GetTypeName());
    }
  }
}


std::ostream& operator<<(std::ostream& os, const Table& table) {

    os << "=====================================================\n";
    os << "TABLE :\n";

    oid_t tile_group_count = table.GetTileGroupCount();
    std::cout << "Tile Group Count : " << tile_group_count << "\n";

    oid_t tuple_count = 0;
    for (oid_t tile_group_itr = 0 ; tile_group_itr < tile_group_count ; tile_group_itr++) {
        auto tile_group = table.GetTileGroup(tile_group_itr);
        auto tile_tuple_count = tile_group->GetNextTupleSlot();

        std::cout << "Tile Group Id  : " << tile_group_itr << " Tuple Count : " << tile_tuple_count << "\n";
        std::cout << (*tile_group);

        tuple_count += tile_tuple_count;
    }

    std::cout << "Table Tuple Count :: " << tuple_count << "\n";

    os << "=====================================================\n";

    return os;
}

} // End storage namespace
} // End nstore namespace

