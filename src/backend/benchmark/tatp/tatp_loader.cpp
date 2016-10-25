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

#include "backend/benchmark/tatp/tatp_loader.h"
#include "backend/benchmark/tatp/tatp_configuration.h"

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
namespace tatp {

/////////////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////////////
const size_t BATCH_SIZE = 5000;
const size_t AI_TYPE_SIZE = 4;
const size_t SF_TYPE_SIZE = 4;
const size_t BASIC_SUBSCRIBERS = 10000;
size_t NUM_SUBSCRIBERS = 0;

const size_t HOTSPOT_FIXED_SIZE = 20;  // 20 fixed number of tuples

double data_length_3 = 3;
double data_length_5 = 4;
double data_length_15 = 15;
double data_length_16 = 16;
double sub_nbr_padding_length = 15;

bool HOTSPOT_USE_FIXED_SIZE = false;

const int MIN_BIT = 0;
const int MAX_BIT = 1;

const int MIN_HEX = 0;
const int MAX_HEX = 15;

const int MIN_BYTE = 0;
const int MAX_BYTE = 255;

const int MIN_INT = 1;
const int MAX_INT = INT_MAX - 1;

std::vector<int> AI_TYPE = {1, 2, 3, 4};
std::vector<int> SF_TYPE = {1, 2, 3, 4};
std::vector<int> START_TIME = {0, 8, 16};

const int loading_thread_count = 0;

/////////////////////////////////////////////////////////
// Create the tables
/////////////////////////////////////////////////////////
storage::Database *tatp_database;

storage::DataTable *subscriber_table;
storage::DataTable *access_info_table;
storage::DataTable *special_facility_table;
storage::DataTable *call_forwarding_table;

const bool own_schema = true;
const bool adapt_table = false;
const bool is_inlined = true;
const bool unique_index = false;
const bool allocate = true;
const size_t preallocate_scale = 2;

static IndexType GetSKeyIndexType() {
  if (concurrency::TransactionManagerFactory::IsRB() &&
      index::IndexFactory::GetSecondaryIndexType() ==
          SECONDARY_INDEX_TYPE_VERSION) {
    // if (state.index == INDEX_TYPE_BTREE)
    //   return INDEX_TYPE_RBBTREE;
    if (state.index == INDEX_TYPE_HASH) {
      return INDEX_TYPE_RBHASH;
    } else {
      return INDEX_TYPE_RBBTREE;
    }
  } else {
    return state.index;
  }
}

void CreateSubscriberTable() {
  /*
  CREATE TABLE SUBSCRIBER (
     s_id INTEGER NOT NULL PRIMARY KEY,
     sub_nbr VARCHAR(15) NOT NULL UNIQUE,
     bit_1 TINYINT,
     bit_2 TINYINT,
     bit_3 TINYINT,
     bit_4 TINYINT,
     bit_5 TINYINT,
     bit_6 TINYINT,
     bit_7 TINYINT,
     bit_8 TINYINT,
     bit_9 TINYINT,
     bit_10 TINYINT,
     hex_1 TINYINT,
     hex_2 TINYINT,
     hex_3 TINYINT,
     hex_4 TINYINT,
     hex_5 TINYINT,
     hex_6 TINYINT,
     hex_7 TINYINT,
     hex_8 TINYINT,
     hex_9 TINYINT,
     hex_10 TINYINT,
     byte2_1 SMALLINT,
     byte2_2 SMALLINT,
     byte2_3 SMALLINT,
     byte2_4 SMALLINT,
     byte2_5 SMALLINT,
     byte2_6 SMALLINT,
     byte2_7 SMALLINT,
     byte2_8 SMALLINT,
     byte2_9 SMALLINT,
     byte2_10 SMALLINT,
     msc_location INTEGER,
     vlr_location INTEGER
  );
  */

  // Create schema first
  std::vector<catalog::Column> subscriber_columns;

  // s_id
  auto s_id_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "S_ID", is_inlined);
  subscriber_columns.push_back(s_id_column);

  // sub_nbr
  auto sub_nbr_column = catalog::Column(VALUE_TYPE_VARCHAR, data_length_15,
                                        "SUB_NBR", is_inlined);
  subscriber_columns.push_back(sub_nbr_column);

  // bit_1 - bit_10
  for (int i = 1; i <= 10; i++) {
    std::string column_name = std::string("bit_") + std::to_string(i);
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        column_name, is_inlined);
    subscriber_columns.push_back(column);
  }

  // hex_1 - hex_10
  for (int i = 1; i <= 10; i++) {
    std::string column_name = std::string("hex_") + std::to_string(i);
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        column_name, is_inlined);
    subscriber_columns.push_back(column);
  }

  // byte2_1 - byte2_10
  for (int i = 1; i <= 10; i++) {
    std::string column_name = std::string("byte2_") + std::to_string(i);
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        column_name, is_inlined);
    subscriber_columns.push_back(column);
  }

  // msc_location
  auto msc_location_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "MSC_LOCATION", is_inlined);
  subscriber_columns.push_back(msc_location_column);

  // vlr_location
  auto vlr_location_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "VLR_LOCATION", is_inlined);
  subscriber_columns.push_back(vlr_location_column);

  // schema
  catalog::Schema *table_schema = new catalog::Schema(subscriber_columns);
  std::string table_name("SUBSCRIBER");

  // table
  subscriber_table = storage::TableFactory::GetDataTable(
      tatp_database_oid, subscriber_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  tatp_database->AddTable(subscriber_table);

  // Primary index on s_id
  std::vector<oid_t> key_attrs = {0};

  auto tuple_schema = subscriber_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "subscriber_pkey", subscriber_table_pkey_index_oid, state.index,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

  index::Index *pkey_index = nullptr;
  if (state.index == INDEX_TYPE_HASH) {
    pkey_index =
        index::IndexFactory::GetInstance(index_metadata, NUM_SUBSCRIBERS);
  } else {
    pkey_index = index::IndexFactory::GetInstance(index_metadata);
  }

  subscriber_table->AddIndex(pkey_index);
}

void CreateAccessInfoTable() {
  /*
 CREATE TABLE ACCESS_INFO (
    s_id INTEGER NOT NULL,
    ai_type TINYINT NOT NULL,
    data1 SMALLINT,
    data2 SMALLINT,
    data3 VARCHAR(3),
    data4 VARCHAR(5),
    PRIMARY KEY(s_id, ai_type),
    FOREIGN KEY (s_id) REFERENCES SUBSCRIBER (s_id)
 );
  */

  // Create schema first
  std::vector<catalog::Column> access_info_columns;

  // s_id
  auto s_id_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "S_ID", is_inlined);
  access_info_columns.push_back(s_id_column);

  // ai_type
  auto ai_type_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "AI_TYPE", is_inlined);
  access_info_columns.push_back(ai_type_column);

  // data1
  auto data1_type_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "DATA1", is_inlined);
  access_info_columns.push_back(data1_type_column);

  // data2
  auto data2_type_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "DATA2", is_inlined);
  access_info_columns.push_back(data2_type_column);

  // data3
  auto data3_type_column =
      catalog::Column(VALUE_TYPE_VARCHAR, data_length_3, "DATA3", is_inlined);
  access_info_columns.push_back(data3_type_column);

  // data4
  auto data4_type_column =
      catalog::Column(VALUE_TYPE_VARCHAR, data_length_5, "DATA4", is_inlined);
  access_info_columns.push_back(data4_type_column);

  // schema
  catalog::Schema *table_schema = new catalog::Schema(access_info_columns);
  std::string table_name("ACCESS_INFO");

  // table
  access_info_table = storage::TableFactory::GetDataTable(
      tatp_database_oid, access_info_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  tatp_database->AddTable(access_info_table);

  // Primary index on s_id and ai_type
  std::vector<oid_t> key_attrs = {0, 1};

  auto tuple_schema = access_info_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "access_info_pkey", access_info_table_pkey_index_oid, state.index,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

  index::Index *pkey_index = nullptr;
  if (state.index == INDEX_TYPE_HASH) {
    pkey_index = index::IndexFactory::GetInstance(
        index_metadata, NUM_SUBSCRIBERS * AI_TYPE_SIZE);
  } else {
    pkey_index = index::IndexFactory::GetInstance(index_metadata);
  }

  access_info_table->AddIndex(pkey_index);
}

void CreateSpecialFacilityTable() {
  /*
  CREATE TABLE SPECIAL_FACILITY (
     s_id INTEGER NOT NULL,
     sf_type TINYINT NOT NULL,
     is_active TINYINT NOT NULL,
     error_cntrl SMALLINT,
     data_a SMALLINT,
     data_b VARCHAR(5),
     PRIMARY KEY (s_id, sf_type),
     FOREIGN KEY (s_id) REFERENCES SUBSCRIBER (s_id)
  );
  */

  // Create schema first
  std::vector<catalog::Column> special_facility_columns;

  // s_id
  auto s_id_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "S_ID", is_inlined);
  special_facility_columns.push_back(s_id_column);

  // sf_type
  auto sf_type_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "SF_TYPE", is_inlined);
  special_facility_columns.push_back(sf_type_column);

  // is_active
  auto is_active_type_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "IS_ACTIVE", is_inlined);
  special_facility_columns.push_back(is_active_type_column);

  // error_cntrl
  auto error_cntrl_type_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "ERROR_CNTRL", is_inlined);
  special_facility_columns.push_back(error_cntrl_type_column);

  // data_a
  auto data_a_type_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "DATA_A", is_inlined);
  special_facility_columns.push_back(data_a_type_column);

  // data_b
  auto data_b_type_column =
      catalog::Column(VALUE_TYPE_VARCHAR, data_length_5, "DATA_B", is_inlined);
  special_facility_columns.push_back(data_b_type_column);

  // schema
  catalog::Schema *table_schema = new catalog::Schema(special_facility_columns);
  std::string table_name("SPECIAL_FACILITY");

  // table
  special_facility_table = storage::TableFactory::GetDataTable(
      tatp_database_oid, special_facility_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  tatp_database->AddTable(special_facility_table);

  // Primary index on sid, sf_type
  std::vector<oid_t> key_attrs = {0, 1};

  auto tuple_schema = special_facility_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "special_facility_pkey", special_facility_table_pkey_index_oid,
      state.index, INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema,
      unique);

  index::Index *pkey_index = nullptr;
  if (state.index == INDEX_TYPE_HASH) {
    pkey_index = index::IndexFactory::GetInstance(
        index_metadata, NUM_SUBSCRIBERS * SF_TYPE_SIZE);
  } else {
    pkey_index = index::IndexFactory::GetInstance(index_metadata);
  }

  special_facility_table->AddIndex(pkey_index);

  // Secondary index on s_id
  key_attrs = {0};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  index_metadata = new index::IndexMetadata(
      "special_facility_skey", special_facility_table_skey_index_oid,
      GetSKeyIndexType(), INDEX_CONSTRAINT_TYPE_INVALID, tuple_schema,
      key_schema, false);

  index::Index *skey_index = nullptr;

  if (GetSKeyIndexType() == INDEX_TYPE_HASH) {
    // no one will update the column that is indexed in customer table.
    skey_index =
        index::IndexFactory::GetInstance(index_metadata, NUM_SUBSCRIBERS);
  } else {
    skey_index = index::IndexFactory::GetInstance(index_metadata);
  }

  special_facility_table->AddIndex(skey_index);

  // Secondary index on s_id, sf_type, is_active
  key_attrs = {0, 1, 2};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  index_metadata = new index::IndexMetadata(
      "special_facility_skey2", special_facility_table_skey_index_oid2,
      GetSKeyIndexType(), INDEX_CONSTRAINT_TYPE_INVALID, tuple_schema,
      key_schema, false);

  index::Index *skey_index2 = nullptr;

  if (GetSKeyIndexType() == INDEX_TYPE_HASH) {
    // no one will update the column that is indexed in customer table.
    skey_index2 = index::IndexFactory::GetInstance(index_metadata,
                                                   NUM_SUBSCRIBERS * 3 * 10);
  } else {
    skey_index2 = index::IndexFactory::GetInstance(index_metadata);
  }

  special_facility_table->AddIndex(skey_index2);
}

void CreateCallForwardingTable() {
  /*
  CREATE TABLE CALL_FORWARDING (
     s_id INTEGER NOT NULL,
     sf_type TINYINT NOT NULL,
     start_time TINYINT NOT NULL,
     end_time TINYINT,
     numberx VARCHAR(15),
     PRIMARY KEY (s_id, sf_type, start_time),
     FOREIGN KEY (s_id, sf_type) REFERENCES SPECIAL_FACILITY(s_id, sf_type)
  );
  CREATE INDEX IDX_CF ON CALL_FORWARDING (S_ID);
  */

  // Create schema first
  std::vector<catalog::Column> call_forwarding_columns;

  // s_id
  auto s_id_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "S_ID", is_inlined);
  call_forwarding_columns.push_back(s_id_column);

  // sf_type
  auto sf_type_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "SF_TYPE", is_inlined);
  call_forwarding_columns.push_back(sf_type_column);

  // start_time
  auto start_time_type_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "START_TIME", is_inlined);
  call_forwarding_columns.push_back(start_time_type_column);

  // end_time
  auto end_time_type_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "END_TIME", is_inlined);
  call_forwarding_columns.push_back(end_time_type_column);

  // numberx
  auto numberx_type_column = catalog::Column(VALUE_TYPE_VARCHAR, data_length_15,
                                             "NUMBERX", is_inlined);
  call_forwarding_columns.push_back(numberx_type_column);

  // schema
  catalog::Schema *table_schema = new catalog::Schema(call_forwarding_columns);
  std::string table_name("CALL_FORWARDING");

  // table
  call_forwarding_table = storage::TableFactory::GetDataTable(
      tatp_database_oid, call_forwarding_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  tatp_database->AddTable(call_forwarding_table);

  // Primary index on CUSTID
  std::vector<oid_t> key_attrs = {0, 1, 2};

  auto tuple_schema = special_facility_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "call_forwarding_pkey", call_forwarding_table_pkey_index_oid, state.index,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

  index::Index *pkey_index = nullptr;
  if (state.index == INDEX_TYPE_HASH) {
    pkey_index = index::IndexFactory::GetInstance(
        index_metadata, NUM_SUBSCRIBERS * SF_TYPE_SIZE);
  } else {
    pkey_index = index::IndexFactory::GetInstance(index_metadata);
  }

  call_forwarding_table->AddIndex(pkey_index);

  // Secondary index on s_id
  key_attrs = {0};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  index_metadata = new index::IndexMetadata(
      "call_forwarding_skey", call_forwarding_table_skey_index_oid,
      GetSKeyIndexType(), INDEX_CONSTRAINT_TYPE_INVALID, tuple_schema,
      key_schema, false);

  index::Index *skey_index = nullptr;

  if (GetSKeyIndexType() == INDEX_TYPE_HASH) {
    // no one will update the column that is indexed in customer table.
    skey_index =
        index::IndexFactory::GetInstance(index_metadata, NUM_SUBSCRIBERS);
  } else {
    skey_index = index::IndexFactory::GetInstance(index_metadata);
  }

  call_forwarding_table->AddIndex(skey_index);

  // Secondary index on s_id, sf_type, start_time, end_time
  key_attrs = {0, 1, 2, 3};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  index_metadata = new index::IndexMetadata(
      "call_forwarding_skey2", call_forwarding_table_skey_index_oid2,
      GetSKeyIndexType(), INDEX_CONSTRAINT_TYPE_INVALID, tuple_schema,
      key_schema, false);

  index::Index *skey_index2 = nullptr;

  if (GetSKeyIndexType() == INDEX_TYPE_HASH) {
    // no one will update the column that is indexed in customer table.
    skey_index2 =
        index::IndexFactory::GetInstance(index_metadata, NUM_SUBSCRIBERS);
  } else {
    skey_index2 = index::IndexFactory::GetInstance(index_metadata);
  }

  call_forwarding_table->AddIndex(skey_index2);
}

void CreateTatpDatabase() {

  // Clean up
  delete tatp_database;
  tatp_database = nullptr;
  subscriber_table = nullptr;
  access_info_table = nullptr;
  special_facility_table = nullptr;
  call_forwarding_table = nullptr;

  auto &manager = catalog::Manager::GetInstance();
  tatp_database = new storage::Database(tatp_database_oid);
  manager.AddDatabase(tatp_database);

  CreateSubscriberTable();
  CreateAccessInfoTable();
  CreateSpecialFacilityTable();
  CreateCallForwardingTable();
}

/////////////////////////////////////////////////////////
// Load in the tables
/////////////////////////////////////////////////////////

std::random_device rd;
std::mt19937 rng(rd());

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

// double GetDoubleFromValue(const peloton::Value &value) {
//  return value.GetDouble();
//}

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

/**
 * Returns sub array of arr, with length in range [min_len, max_len].
 * Each element in arr appears at most once in sub array.
 */
std::vector<int> SubArr(std::vector<int> &arr, int min_len, int max_len) {
  assert(min_len <= max_len && min_len >= 0);

  int sub_len = GetRandomInteger(min_len, max_len);
  int arr_len = arr.size();

  assert(sub_len <= arr_len);

  std::vector<int> sub;

  for (int i = 0; i < sub_len; i++) {
    int idx = GetRandomInteger(0, arr_len - 1);
    sub.push_back(arr[idx]);

    // arr[idx] put to tail
    int tmp = arr[idx];
    arr[idx] = arr[arr_len - 1];
    arr[arr_len - 1] = tmp;

    arr_len--;
  }  // FOR

  return sub;
}

int GetRandomIntegerFromArr(std::vector<int> &arr) {
  int arr_len = arr.size();
  int idx = GetRandomInteger(0, arr_len - 1);
  return arr[idx];
}

std::unique_ptr<storage::Tuple> BuildSubscriberTuple(
    const int sid, const std::unique_ptr<VarlenPool> &pool) {
  auto subscriber_table_schema = subscriber_table->GetSchema();
  std::unique_ptr<storage::Tuple> subscriber_tuple(
      new storage::Tuple(subscriber_table_schema, allocate));

  // S_ID
  subscriber_tuple->SetValue(0, ValueFactory::GetIntegerValue(sid), nullptr);

  // SUB_NBR. We didn't use this when looking up since inefficient string lookup
  auto sub_nbr = std::to_string(sid);
  subscriber_tuple->SetValue(1, ValueFactory::GetStringValue(sub_nbr),
                             pool.get());

  // bit_##
  for (int column = 2; column < 12; column++) {
    int bit = GetRandomInteger(MIN_BIT, MAX_BIT);
    subscriber_tuple->SetValue(column, ValueFactory::GetIntegerValue(bit),
                               nullptr);
  }

  // hex_##
  for (int column = 12; column < 22; column++) {
    int hex = GetRandomInteger(MIN_HEX, MAX_HEX);
    subscriber_tuple->SetValue(column, ValueFactory::GetIntegerValue(hex),
                               nullptr);
  }

  // byte2_##
  for (int column = 22; column < 32; column++) {
    int byte2 = GetRandomInteger(MIN_BYTE, MAX_BYTE);
    subscriber_tuple->SetValue(column, ValueFactory::GetIntegerValue(byte2),
                               nullptr);
  }

  int msc_location = GetRandomInteger(MIN_INT, MAX_INT);
  subscriber_tuple->SetValue(32, ValueFactory::GetIntegerValue(msc_location),
                             nullptr);

  int vlr_location = GetRandomInteger(MIN_INT, MAX_INT);
  subscriber_tuple->SetValue(33, ValueFactory::GetIntegerValue(vlr_location),
                             nullptr);

  return subscriber_tuple;
}

std::unique_ptr<storage::Tuple> BuildAccessInfoTuple(
    const int sid, const int ai_type, const std::unique_ptr<VarlenPool> &pool) {
  auto access_info_table_schema = access_info_table->GetSchema();
  std::unique_ptr<storage::Tuple> access_info_tuple(
      new storage::Tuple(access_info_table_schema, allocate));

  // S_ID
  access_info_tuple->SetValue(0, ValueFactory::GetIntegerValue(sid), nullptr);

  // ai_type
  access_info_tuple->SetValue(1, ValueFactory::GetIntegerValue(ai_type),
                              nullptr);
  // data1
  int data1 = GetRandomInteger(MIN_BYTE, MAX_BYTE);
  access_info_tuple->SetValue(2, ValueFactory::GetIntegerValue(data1), nullptr);

  // data2
  int data2 = GetRandomInteger(MIN_BYTE, MAX_BYTE);
  access_info_tuple->SetValue(3, ValueFactory::GetIntegerValue(data2), nullptr);

  // data3
  auto data3 = GetRandomAlphaNumericString(data_length_3);
  access_info_tuple->SetValue(4, ValueFactory::GetStringValue(data3),
                              pool.get());

  // data4
  auto data4 = GetRandomAlphaNumericString(data_length_5);
  access_info_tuple->SetValue(5, ValueFactory::GetStringValue(data4),
                              pool.get());

  return access_info_tuple;
}

std::unique_ptr<storage::Tuple> BuildSpecialFacilityTuple(
    const int sid, const int sf_type, const std::unique_ptr<VarlenPool> &pool) {
  auto special_facility_table_schema = special_facility_table->GetSchema();
  std::unique_ptr<storage::Tuple> special_facility_tuple(
      new storage::Tuple(special_facility_table_schema, allocate));

  // S_ID
  special_facility_tuple->SetValue(0, ValueFactory::GetIntegerValue(sid),
                                   nullptr);

  // sf_type
  special_facility_tuple->SetValue(1, ValueFactory::GetIntegerValue(sf_type),
                                   nullptr);
  // is_active
  int is_active = GetRandomInteger(MIN_BIT, MAX_BIT);
  special_facility_tuple->SetValue(2, ValueFactory::GetIntegerValue(is_active),
                                   nullptr);

  // error_cntrl
  int error_cntrl = GetRandomInteger(MIN_BYTE, MAX_BYTE);
  special_facility_tuple->SetValue(
      3, ValueFactory::GetIntegerValue(error_cntrl), nullptr);

  // data_a
  int data_a = GetRandomInteger(MIN_BYTE, MAX_BYTE);
  special_facility_tuple->SetValue(4, ValueFactory::GetIntegerValue(data_a),
                                   nullptr);

  // data_b
  auto data_b = GetRandomAlphaNumericString(data_length_5);
  special_facility_tuple->SetValue(5, ValueFactory::GetStringValue(data_b),
                                   pool.get());

  return special_facility_tuple;
}

std::unique_ptr<storage::Tuple> BuildCallForwardingTuple(
    const int sid, const int sf_type, const int start_time, const int end_time,
    const std::unique_ptr<VarlenPool> &pool) {

  auto call_forwarding_table_schema = call_forwarding_table->GetSchema();

  std::unique_ptr<storage::Tuple> call_tuple(
      new storage::Tuple(call_forwarding_table_schema, allocate));

  // CUSTID
  call_tuple->SetValue(0, ValueFactory::GetIntegerValue(sid), nullptr);

  // sf_type
  call_tuple->SetValue(1, ValueFactory::GetIntegerValue(sf_type), nullptr);

  // start_time
  call_tuple->SetValue(2, ValueFactory::GetIntegerValue(start_time), nullptr);

  // end_time
  call_tuple->SetValue(3, ValueFactory::GetIntegerValue(end_time), nullptr);

  // numberx
  auto numberx = GetRandomAlphaNumericString(data_length_15);
  call_tuple->SetValue(4, ValueFactory::GetStringValue(numberx), pool.get());

  return call_tuple;
}

void LoadSubscriber() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  LOG_INFO("SUBSCRIBERS: %f", state.scale_factor);
  LOG_INFO("SUBSCRIBERS: %lu", NUM_SUBSCRIBERS);

  for (size_t itr = 0; itr < NUM_SUBSCRIBERS; itr++) {
    auto subscriber_tuple = BuildSubscriberTuple(itr, pool);
    planner::InsertPlan node(subscriber_table, std::move(subscriber_tuple));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }

  txn_manager.CommitTransaction();
}

void LoadAccessInfo() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  for (size_t itr = 0; itr < NUM_SUBSCRIBERS; itr++) {
    // Generate ai_type
    std::vector<int> sub_ai_type = SubArr(AI_TYPE, 1, 4);
    for (auto ai_type : sub_ai_type) {
      auto access_info_tuple = BuildAccessInfoTuple(itr, ai_type, pool);
      planner::InsertPlan node(access_info_table, std::move(access_info_tuple));
      executor::InsertExecutor executor(&node, context.get());
      executor.Execute();
    }
  }

  txn_manager.CommitTransaction();
}

void LoadSpeAndCal() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  for (size_t itr = 0; itr < NUM_SUBSCRIBERS; itr++) {
    // Generate sf_type
    std::vector<int> sub_sf_type = SubArr(SF_TYPE, 1, 4);

    for (auto sf_type : sub_sf_type) {
      // Load Special Facility Table
      auto special_facility_tuple =
          BuildSpecialFacilityTuple(itr, sf_type, pool);
      planner::InsertPlan node(special_facility_table,
                               std::move(special_facility_tuple));
      executor::InsertExecutor executor(&node, context.get());
      executor.Execute();

      // Load Call Forwarding Table
      std::vector<int> sub_start_time = SubArr(START_TIME, 0, 3);
      for (auto &start_time : sub_start_time) {
        int N = GetRandomInteger(1, 8);
        auto end_time = start_time + N;
        auto call_tuple =
            BuildCallForwardingTuple(itr, sf_type, start_time, end_time, pool);
        planner::InsertPlan node(call_forwarding_table, std::move(call_tuple));
        executor::InsertExecutor executor(&node, context.get());
        executor.Execute();
      }
    }
  }

  txn_manager.CommitTransaction();
}

void LoadTatpDatabase() {
  std::chrono::steady_clock::time_point start_time;
  start_time = std::chrono::steady_clock::now();

  LoadSubscriber();
  LoadAccessInfo();
  LoadSpeAndCal();

  std::chrono::steady_clock::time_point end_time =
      std::chrono::steady_clock::now();
  double diff = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time).count();
  LOG_INFO("time = %lf ms", diff);

  LOG_INFO("============TABLE SIZES==========");
  LOG_INFO("subscriber count = %u",
           subscriber_table->GetAllCurrentTupleCount());
  LOG_INFO("access_info count = %u",
           access_info_table->GetAllCurrentTupleCount());
  LOG_INFO("special_facility count  = %u",
           special_facility_table->GetAllCurrentTupleCount());
  LOG_INFO("call_forwarding count = %u",
           call_forwarding_table->GetAllCurrentTupleCount());
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
