//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table_test.cpp
//
// Identification: tests/storage/data_table_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"

#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "executor/executor_tests_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Data Table Tests
//===--------------------------------------------------------------------===//

class DataTableTests : public PelotonTest {};


}  // End test namespace
}  // End peloton namespace
