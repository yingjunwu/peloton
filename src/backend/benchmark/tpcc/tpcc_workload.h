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

#include "backend/benchmark/benchmark_common.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/executor/abstract_executor.h"
#include "backend/storage/data_table.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"

namespace peloton {

namespace storage{
class DataTable;
}

namespace benchmark {
namespace tpcc {

//===========
// Column ids
//===========
// NEW_ORDER
#define COL_IDX_NO_O_ID       0
#define COL_IDX_NO_D_ID       1
#define COL_IDX_NO_W_ID       2
// ORDERS
#define COL_IDX_O_ID          0
#define COL_IDX_O_C_ID        1
#define COL_IDX_O_D_ID        2
#define COL_IDX_O_W_ID        3
#define COL_IDX_O_ENTRY_D     4
#define COL_IDX_O_CARRIER_ID  5
#define COL_IDX_O_OL_CNT      6
#define COL_IDX_O_ALL_LOCAL   7
// ORDER_LINE
#define COL_IDX_OL_O_ID       0
#define COL_IDX_OL_D_ID       1
#define COL_IDX_OL_W_ID       2
#define COL_IDX_OL_NUMBER     3
#define COL_IDX_OL_I_ID       4
#define COL_IDX_OL_SUPPLY_W_ID      5
#define COL_IDX_OL_DELIVERY_D 6
#define COL_IDX_OL_QUANTITY   7
#define COL_IDX_OL_AMOUNT     8
#define COL_IDX_OL_DIST_INFO  9
// Customer
#define COL_IDX_C_ID              0
#define COL_IDX_C_D_ID            1
#define COL_IDX_C_W_ID            2
#define COL_IDX_C_FIRST           3
#define COL_IDX_C_MIDDLE          4
#define COL_IDX_C_LAST            5
#define COL_IDX_C_STREET_1        6
#define COL_IDX_C_STREET_2        7
#define COL_IDX_C_CITY            8
#define COL_IDX_C_STATE           9
#define COL_IDX_C_ZIP             10
#define COL_IDX_C_PHONE           11
#define COL_IDX_C_SINCE           12
#define COL_IDX_C_CREDIT          13
#define COL_IDX_C_CREDIT_LIM      14
#define COL_IDX_C_DISCOUNT        15
#define COL_IDX_C_BALANCE         16
#define COL_IDX_C_YTD_PAYMENT     17
#define COL_IDX_C_PAYMENT_CNT     18
#define COL_IDX_C_DELIVERY_CNT    19
#define COL_IDX_C_DATA            20
// District
#define COL_IDX_D_ID              0
#define COL_IDX_D_W_ID            1
#define COL_IDX_D_NAME            2
#define COL_IDX_D_STREET_1        3
#define COL_IDX_D_STREET_2        4
#define COL_IDX_D_CITY            5
#define COL_IDX_D_STATE           6
#define COL_IDX_D_ZIP             7
#define COL_IDX_D_TAX             8
#define COL_IDX_D_YTD             9
#define COL_IDX_D_NEXT_O_ID       10
// Stock
#define COL_IDX_S_I_ID            0
#define COL_IDX_S_W_ID            1
#define COL_IDX_S_QUANTITY        2
#define COL_IDX_S_DIST_01         3
#define COL_IDX_S_DIST_02         4
#define COL_IDX_S_DIST_03         5
#define COL_IDX_S_DIST_04         6
#define COL_IDX_S_DIST_05         7
#define COL_IDX_S_DIST_06         8
#define COL_IDX_S_DIST_07         9
#define COL_IDX_S_DIST_08         10
#define COL_IDX_S_DIST_09         11
#define COL_IDX_S_DIST_10         12
#define COL_IDX_S_YTD             13
#define COL_IDX_S_ORDER_CNT       14
#define COL_IDX_S_REMOTE_CNT      15
#define COL_IDX_S_DATA            16

#define TPCC_TRANSACTION_TYPE_STOCK_LEVEL     0
#define TPCC_TRANSACTION_TYPE_ORDER_STATUS    1
#define TPCC_TRANSACTION_TYPE_DELIVERY        2
#define TPCC_TRANSACTION_TYPE_PAYMENT         3
#define TPCC_TRANSACTION_TYPE_NEW_ORDER       4


extern configuration state;


void RunWorkload();

/////////////////////////////////////////////////////////
// TRANSACTION TYPES
/////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////
struct NewOrderPlans {

  executor::IndexScanExecutor* item_index_scan_executor_;
  executor::IndexScanExecutor* warehouse_index_scan_executor_;
  executor::IndexScanExecutor* district_index_scan_executor_;
  executor::IndexScanExecutor* district_update_index_scan_executor_;
  executor::UpdateExecutor* district_update_executor_;
  executor::IndexScanExecutor* customer_index_scan_executor_;
  executor::IndexScanExecutor* stock_index_scan_executor_;
  executor::IndexScanExecutor* stock_update_index_scan_executor_;
  executor::UpdateExecutor* stock_update_executor_;


  void SetContext(executor::ExecutorContext* context) {
    item_index_scan_executor_->SetContext(context);
    
    warehouse_index_scan_executor_->SetContext(context);
    
    district_index_scan_executor_->SetContext(context);
    district_update_index_scan_executor_->SetContext(context);
    district_update_executor_->SetContext(context);
    
    customer_index_scan_executor_->SetContext(context);
    
    stock_index_scan_executor_->SetContext(context);
    stock_update_index_scan_executor_->SetContext(context);
    stock_update_executor_->SetContext(context);
  }

  void Cleanup() {
    delete item_index_scan_executor_;
    item_index_scan_executor_ = nullptr;

    delete warehouse_index_scan_executor_;
    warehouse_index_scan_executor_ = nullptr;

    delete district_index_scan_executor_;
    district_index_scan_executor_ = nullptr;

    delete district_update_index_scan_executor_;
    district_update_index_scan_executor_ = nullptr;

    delete district_update_executor_;
    district_update_executor_ = nullptr;

    delete customer_index_scan_executor_;
    customer_index_scan_executor_ = nullptr;

    delete stock_index_scan_executor_;
    stock_index_scan_executor_ = nullptr;

    delete stock_update_index_scan_executor_;
    stock_update_index_scan_executor_ = nullptr;

    delete stock_update_executor_;
    stock_update_executor_ = nullptr;
  }

};


struct PaymentPlans {

  executor::IndexScanExecutor* customer_pindex_scan_executor_;
  executor::IndexScanExecutor* customer_index_scan_executor_;
  executor::IndexScanExecutor* customer_update_bc_index_scan_executor_;
  executor::UpdateExecutor* customer_update_bc_executor_;
  executor::IndexScanExecutor* customer_update_gc_index_scan_executor_;
  executor::UpdateExecutor* customer_update_gc_executor_;

  executor::IndexScanExecutor* warehouse_index_scan_executor_;
  executor::IndexScanExecutor* warehouse_update_index_scan_executor_;
  executor::UpdateExecutor* warehouse_update_executor_;
  
  executor::IndexScanExecutor* district_index_scan_executor_;
  executor::IndexScanExecutor* district_update_index_scan_executor_;
  executor::UpdateExecutor* district_update_executor_;

  void SetContext(executor::ExecutorContext* context) {
    customer_pindex_scan_executor_->SetContext(context);
    customer_index_scan_executor_->SetContext(context);
    customer_update_bc_index_scan_executor_->SetContext(context);
    customer_update_bc_executor_->SetContext(context);
    customer_update_gc_index_scan_executor_->SetContext(context);
    customer_update_gc_executor_->SetContext(context);
    
    warehouse_index_scan_executor_->SetContext(context);
    warehouse_update_index_scan_executor_->SetContext(context);
    warehouse_update_executor_->SetContext(context);

    district_index_scan_executor_->SetContext(context);
    district_update_index_scan_executor_->SetContext(context);
    district_update_executor_->SetContext(context);
  }

  void Cleanup() {
    delete customer_pindex_scan_executor_;
    customer_pindex_scan_executor_ = nullptr;
    delete customer_index_scan_executor_;
    customer_index_scan_executor_ = nullptr;
    delete customer_update_bc_index_scan_executor_;
    customer_update_bc_index_scan_executor_ = nullptr;
    delete customer_update_bc_executor_;
    customer_update_bc_executor_ = nullptr;
    delete customer_update_gc_index_scan_executor_;
    customer_update_gc_index_scan_executor_ = nullptr;
    delete customer_update_gc_executor_;
    customer_update_gc_executor_ = nullptr;
    
    delete warehouse_index_scan_executor_;
    warehouse_index_scan_executor_ = nullptr;
    delete warehouse_update_index_scan_executor_;
    warehouse_update_index_scan_executor_ = nullptr;
    delete warehouse_update_executor_;
    warehouse_update_executor_ = nullptr;
    
    delete district_index_scan_executor_;
    district_index_scan_executor_ = nullptr;
    delete district_update_index_scan_executor_;
    district_update_index_scan_executor_ = nullptr;
    delete district_update_executor_;
    district_update_executor_ = nullptr;
  }

};

struct DeliveryPlans {

  executor::IndexScanExecutor* new_order_index_scan_executor_;
  executor::IndexScanExecutor* new_order_delete_index_scan_executor_;
  executor::DeleteExecutor* new_order_delete_executor_;
  
  executor::IndexScanExecutor* orders_index_scan_executor_;
  executor::IndexScanExecutor* orders_update_index_scan_executor_;
  executor::UpdateExecutor* orders_update_executor_;
  
  executor::IndexScanExecutor* order_line_index_scan_executor_;
  executor::IndexScanExecutor* order_line_update_index_scan_executor_;
  executor::UpdateExecutor* order_line_update_executor_;

  executor::IndexScanExecutor* customer_index_scan_executor_;
  executor::UpdateExecutor* customer_update_executor_;

  void SetContext(executor::ExecutorContext* context) {
    new_order_index_scan_executor_->SetContext(context);
    new_order_delete_index_scan_executor_->SetContext(context);
    new_order_delete_executor_->SetContext(context);

    orders_index_scan_executor_->SetContext(context);
    orders_update_index_scan_executor_->SetContext(context);
    orders_update_executor_->SetContext(context);

    order_line_index_scan_executor_->SetContext(context);
    order_line_update_index_scan_executor_->SetContext(context);
    order_line_update_executor_->SetContext(context);

    customer_index_scan_executor_->SetContext(context);
    customer_update_executor_->SetContext(context);
  }

  void Cleanup() {
    delete new_order_index_scan_executor_;
    new_order_index_scan_executor_ = nullptr;
    delete new_order_delete_index_scan_executor_;
    new_order_delete_index_scan_executor_ = nullptr;
    delete new_order_delete_executor_;
    new_order_delete_executor_ = nullptr;
    
    delete orders_index_scan_executor_;
    orders_index_scan_executor_ = nullptr;
    delete orders_update_index_scan_executor_;
    orders_update_index_scan_executor_ = nullptr;
    delete orders_update_executor_;
    orders_update_executor_ = nullptr;

    delete order_line_index_scan_executor_;
    order_line_index_scan_executor_ = nullptr;
    delete order_line_update_index_scan_executor_;
    order_line_update_index_scan_executor_ = nullptr;
    delete order_line_update_executor_;
    order_line_update_executor_ = nullptr;

    delete customer_index_scan_executor_;
    customer_index_scan_executor_ = nullptr;
    delete customer_update_executor_;
    customer_update_executor_ = nullptr;
  }

};





NewOrderPlans PrepareNewOrderPlan();

PaymentPlans PreparePaymentPlan();

DeliveryPlans PrepareDeliveryPlan();

size_t GenerateWarehouseId(const size_t &thread_id);


struct NewOrderParams {
  int warehouse_id;
  int district_id;
  int customer_id;
  int o_ol_cnt;
  std::vector<int> i_ids;
  std::vector<int> ol_w_ids;
  std::vector<int> ol_qtys;
  bool o_all_local;

  void Serialize(ParamString &param_str) {
    param_str.Allocate(4*sizeof(int) + i_ids.size()*3*sizeof(int) + sizeof(bool));
    size_t offset = 0;
    memcpy(param_str.data + offset, &warehouse_id, sizeof(warehouse_id));
    offset += sizeof(warehouse_id);
    memcpy(param_str.data + offset, &district_id, sizeof(district_id));
    offset += sizeof(district_id);
    memcpy(param_str.data + offset, &customer_id, sizeof(customer_id));
    offset += sizeof(customer_id);
    memcpy(param_str.data + offset, &o_ol_cnt, sizeof(o_ol_cnt));
    offset += sizeof(o_ol_cnt);

    size_t id_count = i_ids.size();
    memcpy(param_str.data + offset, &id_count, sizeof(id_count));

    for (size_t i = 0; i < id_count; ++i) {
      memcpy(param_str.data + offset, &(i_ids.at(i)), sizeof(int));
      offset += sizeof(int);
      memcpy(param_str.data + offset, &(ol_w_ids.at(i)), sizeof(int));
      offset += sizeof(int);
      memcpy(param_str.data + offset, &(ol_qtys.at(i)), sizeof(int));
      offset += sizeof(int);
    }

    memcpy(param_str.data + offset, &o_all_local, sizeof(o_all_local));
  }

  void Deserialize(const ParamString &param_str) {
    size_t offset = 0;
    memcpy(&warehouse_id, param_str.data + offset, sizeof(warehouse_id));
    offset += sizeof(warehouse_id);
    memcpy(&district_id, param_str.data + offset, sizeof(district_id));
    offset += sizeof(district_id);
    memcpy(&customer_id, param_str.data + offset, sizeof(customer_id));
    offset += sizeof(customer_id);
    memcpy(&o_ol_cnt, param_str.data + offset, sizeof(o_ol_cnt));
    offset += sizeof(o_ol_cnt);

    size_t id_count = 0;
    memcpy(&id_count, param_str.data + offset, sizeof(id_count));
    offset += sizeof(id_count);
    
    i_ids.resize(id_count);
    ol_w_ids.resize(id_count);
    ol_qtys.resize(id_count);
    
    for (size_t i = 0; i < id_count; ++i) {
      memcpy(&(i_ids[i]), param_str.data + offset, sizeof(int));
      offset += sizeof(int);
      memcpy(&(ol_w_ids[i]), param_str.data + offset, sizeof(int));
      offset += sizeof(int);
      memcpy(&(ol_qtys[i]), param_str.data + offset, sizeof(int));
      offset += sizeof(int);
    }

    memcpy(param_str.data + offset, &o_all_local, sizeof(o_all_local));
  }
};

void GenerateNewOrderParams(const size_t &thread_id, NewOrderParams &params);

bool RunNewOrder(NewOrderPlans &new_order_plans, const NewOrderParams &params);


struct PaymentParams {
  int warehouse_id;
  int district_id;
  int customer_warehouse_id;
  int customer_district_id;
  int customer_id;
  double h_amount;
  std::string customer_lastname;

  void Serialize(ParamString &param_str) {
    param_str.Allocate(5*sizeof(int) + sizeof(double));
    size_t offset = 0;
    memcpy(param_str.data + offset, &warehouse_id, sizeof(warehouse_id));
    offset += sizeof(warehouse_id);
    memcpy(param_str.data + offset, &district_id, sizeof(district_id));
    offset += sizeof(district_id);
    memcpy(param_str.data + offset, &customer_warehouse_id, sizeof(customer_warehouse_id));
    offset += sizeof(customer_warehouse_id);
    memcpy(param_str.data + offset, &customer_district_id, sizeof(customer_district_id));
    offset += sizeof(customer_district_id);
    memcpy(param_str.data + offset, &customer_id, sizeof(customer_id));
    offset += sizeof(customer_id);
    memcpy(param_str.data + offset, &h_amount, sizeof(h_amount));
  }

  void Deserialize(const ParamString &param_str) {
    size_t offset = 0;
    memcpy(&warehouse_id, param_str.data + offset, sizeof(warehouse_id));
    offset += sizeof(warehouse_id);
    memcpy(&district_id, param_str.data + offset, sizeof(district_id));
    offset += sizeof(district_id);
    memcpy(&customer_warehouse_id, param_str.data + offset, sizeof(customer_warehouse_id));
    offset += sizeof(customer_warehouse_id);
    memcpy(&customer_district_id, param_str.data + offset, sizeof(customer_district_id));
    offset += sizeof(customer_district_id);
    memcpy(&customer_id, param_str.data + offset, sizeof(customer_id));
    offset += sizeof(customer_id);
    memcpy(&h_amount, param_str.data + offset, sizeof(h_amount));
  }
};

void GeneratePaymentParams(const size_t &thread_id, PaymentParams &params);

bool RunPayment(PaymentPlans &payment_plans, const PaymentParams &params);


struct DeliveryParams {
  int warehouse_id;
  int o_carrier_id;

  void Serialize(ParamString &param_str) {
    param_str.Allocate(2*sizeof(int));
    size_t offset = 0;
    memcpy(param_str.data + offset, &warehouse_id, sizeof(warehouse_id));
    offset += sizeof(warehouse_id);
    memcpy(param_str.data + offset, &o_carrier_id, sizeof(o_carrier_id));
  }

  void Deserialize(const ParamString &param_str) {
    size_t offset = 0;
    memcpy(&warehouse_id, param_str.data + offset, sizeof(warehouse_id));
    offset += sizeof(warehouse_id);
    memcpy(&o_carrier_id, param_str.data + offset, sizeof(o_carrier_id));
  }
};

void GenerateDeliveryParams(const size_t &thread_id, DeliveryParams &params);

bool RunDelivery(DeliveryPlans &delivery_plans, const DeliveryParams &params);

bool RunOrderStatus(const size_t &thread_id);

bool RunStockLevel(const size_t &thread_id, const int &order_range);

bool RunScanStock();


/////////////////////////////////////////////////////////

std::vector<std::vector<Value>>
ExecuteReadTest(executor::AbstractExecutor* executor);

void ExecuteUpdateTest(executor::AbstractExecutor* executor);

void ExecuteDeleteTest(executor::AbstractExecutor* executor);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton