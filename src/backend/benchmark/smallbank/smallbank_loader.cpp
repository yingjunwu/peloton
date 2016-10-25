//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// loader.cpp
//
// Identification: benchmark/tpcc/loader.cpp
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
#include <cstring>

#include "backend/benchmark/smallbank/smallbank_loader.h"
#include "backend/benchmark/smallbank/smallbank_configuration.h"

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
namespace smallbank {

/////////////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////////////
const size_t BATCH_SIZE = 5000;
const size_t BASIC_ACCOUNTS = 10000;
size_t NUM_ACCOUNTS = 0;

const size_t HOTSPOT_FIXED_SIZE = 50;  // fixed number of tuples

double accounts_name_length = 16;
bool HOTSPOT_USE_FIXED_SIZE = false;

const size_t MIN_BALANCE = 10000;
const size_t MAX_BALANCE = 50000;

NURandConstant nu_rand_const;

const int loading_thread_count = 0;

/////////////////////////////////////////////////////////
// Create the tables
/////////////////////////////////////////////////////////
storage::Database *smallbank_database;

storage::DataTable *accounts_table;
storage::DataTable *savings_table;
storage::DataTable *checking_table;

const bool own_schema = true;
const bool adapt_table = false;
const bool is_inlined = true;
const bool unique_index = false;
const bool allocate = true;
const size_t preallocate_scale = 2;

void CreateAccountsTable() {
  /*
    CREATE TABLE ACCOUNTS (
      custid      BIGINT      NOT NULL,
      name        VARCHAR(64) NOT NULL,
      CONSTRAINT pk_accounts PRIMARY KEY (custid),
    );
   */

  // Create schema first
  std::vector<catalog::Column> accounts_columns;

  // custid
  auto custid_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "CUSTID", is_inlined);
  accounts_columns.push_back(custid_column);

  // name
  auto name_column = catalog::Column(VALUE_TYPE_VARCHAR, accounts_name_length,
                                     "NAME", is_inlined);
  accounts_columns.push_back(name_column);

  // schema
  catalog::Schema *table_schema = new catalog::Schema(accounts_columns);
  std::string table_name("ACCOUNTS");

  // table
  accounts_table = storage::TableFactory::GetDataTable(
      smallbank_database_oid, accounts_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  smallbank_database->AddTable(accounts_table);

  // Primary index on CUSTID
  std::vector<oid_t> key_attrs = {0};

  auto tuple_schema = accounts_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "accounts_pkey", accounts_table_pkey_index_oid, state.index,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

  index::Index *pkey_index = nullptr;
  if (state.index == INDEX_TYPE_HASH) {
    pkey_index = index::IndexFactory::GetInstance(index_metadata, NUM_ACCOUNTS);
  } else {
    pkey_index = index::IndexFactory::GetInstance(index_metadata);
  }

  accounts_table->AddIndex(pkey_index);
}

void CreateSavingsTable() {
  /*
  CREATE TABLE SAVINGS (
    custid      BIGINT      NOT NULL,
    bal         FLOAT       NOT NULL,
    CONSTRAINT pk_savings PRIMARY KEY (custid),
    FOREIGN KEY (custid) REFERENCES ACCOUNTS (custid)
  );
   */

  // Create schema first
  std::vector<catalog::Column> savings_columns;

  // custid
  auto custid_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "CUSTID", is_inlined);
  savings_columns.push_back(custid_column);

  // balance
  auto bal_column = catalog::Column(VALUE_TYPE_DOUBLE, accounts_name_length,
                                    "BAL", is_inlined);
  savings_columns.push_back(bal_column);

  // schema
  catalog::Schema *table_schema = new catalog::Schema(savings_columns);
  std::string table_name("SAVINGS");

  // table
  savings_table = storage::TableFactory::GetDataTable(
      smallbank_database_oid, savings_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  smallbank_database->AddTable(savings_table);

  // Primary index on CUSTID
  std::vector<oid_t> key_attrs = {0};

  auto tuple_schema = savings_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "savings_pkey", savings_table_pkey_index_oid, state.index,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

  index::Index *pkey_index = nullptr;
  if (state.index == INDEX_TYPE_HASH) {
    pkey_index = index::IndexFactory::GetInstance(index_metadata, NUM_ACCOUNTS);
  } else {
    pkey_index = index::IndexFactory::GetInstance(index_metadata);
  }

  savings_table->AddIndex(pkey_index);
}

void CreateCheckingTable() {
  /*
  CREATE TABLE CHECKING (
    custid      BIGINT      NOT NULL,
    bal         FLOAT       NOT NULL,
    CONSTRAINT pk_checking PRIMARY KEY (custid),
    FOREIGN KEY (custid) REFERENCES ACCOUNTS (custid)
  );
   */

  // Create schema first
  std::vector<catalog::Column> checking_columns;

  // custid
  auto custid_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "CUSTID", is_inlined);
  checking_columns.push_back(custid_column);

  // balance
  auto bal_column = catalog::Column(VALUE_TYPE_DOUBLE, accounts_name_length,
                                    "BAL", is_inlined);
  checking_columns.push_back(bal_column);

  // schema
  catalog::Schema *table_schema = new catalog::Schema(checking_columns);
  std::string table_name("CHECKING");

  // table
  checking_table = storage::TableFactory::GetDataTable(
      smallbank_database_oid, checking_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  smallbank_database->AddTable(checking_table);

  // Primary index on CUSTID
  std::vector<oid_t> key_attrs = {0};

  auto tuple_schema = savings_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "checking_pkey", checking_table_pkey_index_oid, state.index,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

  index::Index *pkey_index = nullptr;
  if (state.index == INDEX_TYPE_HASH) {
    pkey_index = index::IndexFactory::GetInstance(index_metadata, NUM_ACCOUNTS);
  } else {
    pkey_index = index::IndexFactory::GetInstance(index_metadata);
  }

  checking_table->AddIndex(pkey_index);
}

void CreateSmallBankDatabase() {

  // Clean up
  delete smallbank_database;
  smallbank_database = nullptr;
  accounts_table = nullptr;
  savings_table = nullptr;
  checking_table = nullptr;

  auto &manager = catalog::Manager::GetInstance();
  smallbank_database = new storage::Database(smallbank_database_oid);
  manager.AddDatabase(smallbank_database);

  CreateAccountsTable();
  CreateSavingsTable();
  CreateCheckingTable();
}

/////////////////////////////////////////////////////////
// Load in the tables
/////////////////////////////////////////////////////////

std::random_device rd;
std::mt19937 rng(rd());

// Create random NURand constants, appropriate for loading the database.
NURandConstant::NURandConstant() {
  c_last = GetRandomInteger(0, 255);
  c_id = GetRandomInteger(0, 1023);
  order_line_itme_id = GetRandomInteger(0, 8191);
}

// A non-uniform random number, as defined by TPC-C 2.1.6. (page 20).
int GetNURand(int a, int x, int y) {
  assert(x <= y);
  int c = nu_rand_const.c_last;

  if (a == 255) {
    c = nu_rand_const.c_last;
  } else if (a == 1023) {
    c = nu_rand_const.c_id;
  } else if (a == 8191) {
    c = nu_rand_const.order_line_itme_id;
  } else {
    assert(false);
  }

  return (((GetRandomInteger(0, a) | GetRandomInteger(x, y)) + c) %
          (y - x + 1)) +
         x;
}

std::string GetRandomAlphaNumericString(const size_t string_length) {
  const char alphanumeric[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  std::uniform_int_distribution<> dist(0, sizeof(alphanumeric) - 1);

  char repeated_char = alphanumeric[dist(rng)];
  std::string sample(string_length, repeated_char);
  return sample;
}

double GetDoubleFromValue(const peloton::Value &value) {
  return value.GetDouble();
}

bool GetRandomBoolean(double ratio) {
  double sample = (double)rand() / RAND_MAX;
  return (sample < ratio) ? true : false;
}

int GetRandomInteger(const int lower_bound, const int upper_bound) {
  std::uniform_int_distribution<> dist(lower_bound, upper_bound);

  int sample = dist(rng);
  return sample;
}

int GetRandomIntegerExcluding(const int lower_bound, const int upper_bound,
                              const int exclude_sample) {
  int sample;
  if (lower_bound == upper_bound) return lower_bound;

  while (1) {
    sample = GetRandomInteger(lower_bound, upper_bound);
    if (sample != exclude_sample) break;
  }
  return sample;
}

double GetRandomDouble(const double lower_bound, const double upper_bound) {
  std::uniform_real_distribution<> dist(lower_bound, upper_bound);

  double sample = dist(rng);
  return sample;
}

double GetRandomFixedPoint(int decimal_places, double minimum, double maximum) {
  assert(decimal_places > 0);
  assert(minimum < maximum);

  int multiplier = 1;
  for (int i = 0; i < decimal_places; ++i) {
    multiplier *= 10;
  }

  int int_min = (int)(minimum * multiplier + 0.5);
  int int_max = (int)(maximum * multiplier + 0.5);

  return GetRandomDouble(int_min, int_max) / (double)(multiplier);
}

int GetTimeStamp() {
  auto time_stamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  return time_stamp;
}

std::unique_ptr<storage::Tuple> BuildAccountsTuple(
    const int custid, const std::unique_ptr<VarlenPool> &pool) {
  auto accounts_table_schema = accounts_table->GetSchema();
  std::unique_ptr<storage::Tuple> accounts_tuple(
      new storage::Tuple(accounts_table_schema, allocate));

  // CUSTID
  accounts_tuple->SetValue(0, ValueFactory::GetIntegerValue(custid), nullptr);

  // W_NAME
  auto name = GetRandomAlphaNumericString(accounts_name_length);
  accounts_tuple->SetValue(1, ValueFactory::GetStringValue(name), pool.get());

  return accounts_tuple;
}

std::unique_ptr<storage::Tuple> BuildSavingsTuple(
    const int custid, const std::unique_ptr<VarlenPool> &pool) {

  auto savings_table_schema = savings_table->GetSchema();

  std::unique_ptr<storage::Tuple> savings_tuple(
      new storage::Tuple(savings_table_schema, allocate));

  // CUSTID
  savings_tuple->SetValue(0, ValueFactory::GetIntegerValue(custid), nullptr);

  // BAL
  double bal = GetRandomDouble(MIN_BALANCE, MAX_BALANCE);
  savings_tuple->SetValue(1, ValueFactory::ValueFactory::GetDoubleValue(bal),
                          pool.get());

  return savings_tuple;
}

std::unique_ptr<storage::Tuple> BuildCheckingTuple(
    const int custid, const std::unique_ptr<VarlenPool> &pool) {

  auto checking_table_schema = checking_table->GetSchema();

  std::unique_ptr<storage::Tuple> checking_tuple(
      new storage::Tuple(checking_table_schema, allocate));

  // CUSTID
  checking_tuple->SetValue(0, ValueFactory::GetIntegerValue(custid), nullptr);

  // BAL
  double bal = GetRandomDouble(MIN_BALANCE, MAX_BALANCE);
  checking_tuple->SetValue(1, ValueFactory::ValueFactory::GetDoubleValue(bal),
                           pool.get());

  return checking_tuple;
}

void LoadAccounts() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  LOG_INFO("Accounts: %f", state.scale_factor);
  LOG_INFO("Accounts: %lu", NUM_ACCOUNTS);

  for (size_t accouts_itr = 0; accouts_itr < NUM_ACCOUNTS; accouts_itr++) {
    auto accouts_tuple = BuildAccountsTuple(accouts_itr, pool);
    planner::InsertPlan node(accounts_table, std::move(accouts_tuple));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }

  txn_manager.CommitTransaction();
}

void LoadSavings() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  for (size_t savings_itr = 0; savings_itr < NUM_ACCOUNTS; savings_itr++) {

    auto savings_tuple = BuildSavingsTuple(savings_itr, pool);
    planner::InsertPlan node(savings_table, std::move(savings_tuple));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }

  txn_manager.CommitTransaction();
}

void LoadChecking() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  for (size_t checking_itr = 0; checking_itr < NUM_ACCOUNTS; checking_itr++) {

    auto checking_tuple = BuildCheckingTuple(checking_itr, pool);
    planner::InsertPlan node(checking_table, std::move(checking_tuple));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }

  txn_manager.CommitTransaction();
}

void LoadSmallBankDatabase() {
  std::chrono::steady_clock::time_point start_time;
  start_time = std::chrono::steady_clock::now();

  LoadAccounts();
  LoadSavings();
  LoadChecking();

  std::chrono::steady_clock::time_point end_time =
      std::chrono::steady_clock::now();
  double diff = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time).count();
  LOG_INFO("time = %lf ms", diff);

  LOG_INFO("Accounts count = %u", accounts_table->GetAllCurrentTupleCount());
  LOG_INFO("Savings count = %u", savings_table->GetAllCurrentTupleCount());
  LOG_INFO("Checking count  = %u", checking_table->GetAllCurrentTupleCount());

  LOG_INFO("============TILEGROUP SIZES==========");
  LOG_INFO("accounts tile group = %lu", accounts_table->GetTileGroupCount());
  LOG_INFO("savings tile group  = %lu", savings_table->GetTileGroupCount());
  LOG_INFO("checking tile group = %lu", checking_table->GetTileGroupCount());
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
