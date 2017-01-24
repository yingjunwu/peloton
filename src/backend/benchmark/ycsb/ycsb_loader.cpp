//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ycsb_loader.cpp
//
// Identification: benchmark/ycsb/ycsb_loader.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>

#include "backend/benchmark/ycsb/ycsb_loader.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/index/index_factory.h"
#include "backend/planner/insert_plan.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

storage::Database *ycsb_database = nullptr;

storage::DataTable *user_table = nullptr;

void CreateYCSBDatabase() {
  const oid_t col_count = state.column_count + 1;
  const bool is_inlined = true;

  /////////////////////////////////////////////////////////
  // Create tables
  /////////////////////////////////////////////////////////

  // Clean up
  delete ycsb_database;
  ycsb_database = nullptr;
  user_table = nullptr;

  auto &manager = catalog::Manager::GetInstance();
  ycsb_database = new storage::Database(ycsb_database_oid);
  manager.AddDatabase(ycsb_database);

  bool own_schema = true;
  bool adapt_table = false;

  // Create schema first
  std::vector<catalog::Column> columns;

  auto column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "YCSB_KEY", is_inlined);
  columns.push_back(column);

  for (oid_t col_itr = 1; col_itr < col_count; col_itr++) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, ycsb_field_length,
                        "FIELD" + std::to_string(col_itr), is_inlined);
    columns.push_back(column);
  }

  catalog::Schema *table_schema = new catalog::Schema(columns);
  std::string table_name("USERTABLE");

  user_table = storage::TableFactory::GetDataTable(
      ycsb_database_oid, user_table_oid, table_schema, table_name, DEFAULT_TUPLES_PER_TILEGROUP,
      own_schema, adapt_table);

  ycsb_database->AddTable(user_table);

  // Primary index on user key
  std::vector<oid_t> key_attrs;

  auto tuple_schema = user_table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "primary_index", user_table_pkey_index_oid, state.index,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
  user_table->AddIndex(pkey_index);


  // Secondary index
  // FIXME (Runshen Zhu): double check range
  for (int i = 1; i <= state.sindex_count; i++) {
    key_attrs.clear();
    key_attrs.push_back(i);
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    index_metadata = new index::IndexMetadata(
      "sindex_" + i, ycsb_table_sindex_begin_oid + i, state.index,
      INDEX_CONSTRAINT_TYPE_INVALID, tuple_schema, key_schema, false);

    index::Index *skey_index = index::IndexFactory::GetInstance(index_metadata);
    user_table->AddIndex(skey_index);
  }
}

void LoadYCSBRows(const int begin_rowid, const int end_rowid) {
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.RegisterTxnWorker(false);

  const oid_t col_count = state.column_count + 1;
  
  // Pick the user table
  auto table_schema = user_table->GetSchema();
  
  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));
  
  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  int rowid;
  for (rowid = begin_rowid; rowid < end_rowid; rowid++) {
    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(table_schema, allocate));
    
    auto key_value = ValueFactory::GetIntegerValue(rowid);
    tuple->SetValue(0, key_value, nullptr);
    
    for (oid_t col_itr = 1; col_itr < col_count; col_itr++) {
      tuple->SetValue(col_itr, key_value, nullptr);
    }

    planner::InsertPlan node(user_table, std::move(tuple));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }

  txn_manager.CommitTransaction();
}


void LoadYCSBDatabase() {

  std::chrono::steady_clock::time_point start_time;
  start_time = std::chrono::steady_clock::now();

  const int tuple_count = state.scale_factor * 1000;
  int row_per_thread = tuple_count / state.loader_count;
  std::vector<std::unique_ptr<std::thread>> load_threads(state.loader_count);

  for (int thread_id = 0; thread_id < state.loader_count - 1; ++thread_id) {
    int begin_rowid = row_per_thread * thread_id;
    int end_rowid = row_per_thread * (thread_id + 1);
    load_threads[thread_id].reset(new std::thread(LoadYCSBRows, begin_rowid, end_rowid));
  }
  
  int thread_id = state.loader_count - 1;
  int begin_rowid = row_per_thread * thread_id;
  int end_rowid = tuple_count;
  load_threads[thread_id].reset(new std::thread(LoadYCSBRows, begin_rowid, end_rowid));

  for (int thread_id = 0; thread_id < state.loader_count; ++thread_id) {
    load_threads[thread_id]->join();
  }

  std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
  double diff = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  LOG_INFO("time = %lf ms", diff);

  LOG_INFO("============TABLE SIZES==========");
  LOG_INFO("user count = %u", user_table->GetAllCurrentTupleCount());

  LOG_INFO("Sleep for a while to generate a read only snap shot");
  std::this_thread::sleep_for(std::chrono::seconds(1));
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
