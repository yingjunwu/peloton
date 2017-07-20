//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// visibility_test.cpp
//
// Identification: test/concurrency/visibility_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/testing_transaction_util.h"
#include "common/harness.h"

namespace peloton {
namespace test {

class VisibilityTests : public PelotonTest {};

static std::vector<ProtocolType> PROTOCOL_TYPES = {
 ProtocolType::TIMESTAMP_ORDERING
};


TEST_F(VisibilityTests, InsertTest) {
 for (auto protocol_type : PROTOCOL_TYPES) {
   concurrency::TransactionManagerFactory::Configure(protocol_type);
   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
   storage::DataTable *table = TestingTransactionUtil::CreateTable();

   // update, update, update, update, read
   {
     TransactionScheduler scheduler(1, table, &txn_manager);
     scheduler.Txn(0).Insert(0, false);
     scheduler.Txn(0).Insert(1, false);
     scheduler.Txn(0).Commit();

     scheduler.Run();

     EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
   }
  }
}


}
}
