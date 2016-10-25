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

#include "backend/benchmark/tatp/tatp_configuration.h"
#include "backend/common/value.h"

namespace peloton {
namespace storage {
class Database;
class DataTable;
class Tuple;
}

class VarlenPool;

namespace benchmark {
namespace tatp {

extern configuration state;

void CreateTatpDatabase();

void LoadTatpDatabase();

/////////////////////////////////////////////////////////
// Database
/////////////////////////////////////////////////////////

extern storage::Database* tatp_database;

/////////////////////////////////////////////////////////
// Table
/////////////////////////////////////////////////////////

extern storage::DataTable* subscriber_table;
extern storage::DataTable* access_info_table;
extern storage::DataTable* special_facility_table;
extern storage::DataTable* call_forwarding_table;

/////////////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////////////
extern const size_t FREQUENCY_GET_ACCESS_DATA;
extern const size_t FREQUENCY_GET_NEW_DESTINATION;
extern const size_t FREQUENCY_GET_SUBSCRIBER_DATA;
extern const size_t FREQUENCY_INSERT_CALL_FORWARDING;
extern const size_t FREQUENCY_DELETE_CALL_FORWARDING;
extern const size_t FREQUENCY_UPDATE_SUBSCRIBER_DATA;
extern const size_t FREQUENCY_UPDATE_LOCATION;

extern const size_t BATCH_SIZE;
extern const size_t BASIC_SUBSCRIBERS;
extern size_t NUM_SUBSCRIBERS;

extern const size_t HOTSPOT_FIXED_SIZE;  // fixed number of tuples
extern double HOTSPOT_PERCENTAGE;        // [0% - 100%]

extern double accounts_name_length;
extern double sub_nbr_padding_length;

extern bool HOTSPOT_USE_FIXED_SIZE;

extern const int MIN_BIT;
extern const int MAX_BIT;

extern const int MIN_HEX;
extern const int MAX_HEX;

extern const int MIN_BYTE;
extern const int MAX_BYTE;

extern const int MIN_INT;
extern const int MAX_INT;

extern std::vector<int> AI_TYPE;
extern std::vector<int> SF_TYPE;
extern std::vector<int> START_TIME;

/////////////////////////////////////////////////////////
// Tuple Constructors
/////////////////////////////////////////////////////////
std::unique_ptr<storage::Tuple> BuildSubscriberTuple(
    const int sid, const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple> BuildAccessInfoTuple(
    const int sid, const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple> BuildSpecialFacilityTuple(
    const int sid, const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple> BuildCallForwardingTuple(
    const int sid, const int sf_type, const int start_time, const int end_time,
    const std::unique_ptr<VarlenPool>& pool);

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

std::vector<int> SubArr(std::vector<int>& arr, int min_len, int max_len);
int GetRandomIntegerFromArr(std::vector<int>& arr);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
