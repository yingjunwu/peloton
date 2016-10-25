//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/tpcc/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "backend/benchmark/smallbank/smallbank_configuration.h"
#include "backend/common/value.h"

namespace peloton {
namespace storage {
class Database;
class DataTable;
class Tuple;
}

class VarlenPool;

namespace benchmark {
namespace smallbank {

extern configuration state;

void CreateSmallBankDatabase();

void LoadSmallBankDatabase();

/////////////////////////////////////////////////////////
// Database
/////////////////////////////////////////////////////////

extern storage::Database* smallbank_database;

/////////////////////////////////////////////////////////
// Table
/////////////////////////////////////////////////////////

extern storage::DataTable* accounts_table;
extern storage::DataTable* savings_table;
extern storage::DataTable* checking_table;

/////////////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////////////

extern const size_t FREQUENCY_AMALGAMATE;
extern const size_t FREQUENCY_BALANCE;
extern const size_t FREQUENCY_DEPOSIT_CHECKING;
extern const size_t FREQUENCY_SEND_PAYMENT;
extern const size_t FREQUENCY_TRANSACT_SAVINGS;
extern const size_t FREQUENCY_WRITE_CHECK;

extern const size_t BATCH_SIZE;
extern const size_t BASIC_ACCOUNTS;
extern size_t NUM_ACCOUNTS;
extern const size_t HOTSPOT_FIXED_SIZE;  // fixed number of tuples
extern double HOTSPOT_PERCENTAGE;        // [0% - 100%]

extern double accounts_name_length;
extern bool HOTSPOT_USE_FIXED_SIZE;

extern const size_t MIN_BALANCE;
extern const size_t MAX_BALANCE;

struct NURandConstant {
  int c_last;
  int c_id;
  int order_line_itme_id;

  NURandConstant();
};

extern NURandConstant nu_rand_const;

/////////////////////////////////////////////////////////
// Tuple Constructors
/////////////////////////////////////////////////////////
std::unique_ptr<storage::Tuple> BuildAccountsTuple(
    const int custid, const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple> BuildSavingsTuple(
    const int custid, const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple> BuildCheckingTuple(
    const int custid, const std::unique_ptr<VarlenPool>& pool);

/////////////////////////////////////////////////////////
// Utils
/////////////////////////////////////////////////////////
void GetStringFromValue(const peloton::Value& value, std::string& str);

std::string GetRandomAlphaNumericString(const size_t string_length);

int GetNURand(int a, int x, int y);

std::string GetLastName(int number);

std::string GetRandomLastName(int max_cid);

bool GetRandomBoolean(double ratio);

int GetRandomInteger(const int lower_bound, const int upper_bound);

int GetRandomIntegerExcluding(const int lower_bound, const int upper_bound,
                              const int exclude_sample);

double GetRandomDouble(const double lower_bound, const double upper_bound);

double GetRandomFixedPoint(int decimal_places, double minimum, double maximum);

int GetTimeStamp();

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
