## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# BENCHMARK
######################################################################

benchmark_common_cppflags = -I. -I$(top_srcdir)/src -I.. $(postgres_common_INCLUDES) $(AM_CPPFLAGS)  \
            				$(third_party_INCLUDES) \
			            	-I$(srcdir)/backend/benchmark


benchmark_common_ldadd = libpelotonpg.la libpeloton.la -lpthread


######################################################################
# LOGGER
######################################################################

#logger_SOURCES =  \
#					backend/benchmark/logger/logger.cpp \
#                    backend/benchmark/logger/logger_configuration.cpp \
#                    backend/benchmark/logger/logger_workload.cpp \
#                    backend/benchmark/ycsb/ycsb_configuration.cpp \
#                    backend/benchmark/ycsb/ycsb_workload.cpp \
#                    backend/benchmark/ycsb/ycsb_loader.cpp \
#                    backend/benchmark/ycsb/ycsb_mixed.cpp \
#                    backend/benchmark/ycsb/ycsb_scan.cpp \
#                    backend/benchmark/tpcc/tpcc_configuration.cpp \
#                    backend/benchmark/tpcc/tpcc_workload.cpp \
#                    backend/benchmark/tpcc/tpcc_loader.cpp \
#                    backend/benchmark/tpcc/tpcc_new_order.cpp \
#                    backend/benchmark/tpcc/tpcc_delivery.cpp \
#                    backend/benchmark/tpcc/tpcc_payment.cpp \
#                    backend/benchmark/tpcc/tpcc_stock_level.cpp \
#                    backend/benchmark/tpcc/tpcc_order_status.cpp \
#                    backend/benchmark/tpcc/tpcc_scan_stock.cpp \
#                    backend/benchmark/benchmark_common.cpp

#logger_LDFLAGS =
#logger_CPPFLAGS = $(benchmark_common_cppflags)
#logger_LDADD = $(benchmark_common_ldadd) -lboost_system -lboost_filesystem

######################################################################
# YCSB
######################################################################

ycsb_SOURCES =  \
                         backend/benchmark/benchmark_common.cpp \
				backend/benchmark/ycsb/ycsb.cpp \
                    backend/benchmark/ycsb/ycsb_configuration.cpp \
                    backend/benchmark/ycsb/ycsb_workload.cpp \
                    backend/benchmark/ycsb/ycsb_loader.cpp \
                    backend/benchmark/ycsb/ycsb_mixed.cpp \
                    backend/benchmark/ycsb/ycsb_scan.cpp

ycsb_LDFLAGS =
ycsb_CPPFLAGS = $(benchmark_common_cppflags)
ycsb_LDADD = $(benchmark_common_ldadd)

######################################################################
# TPCC
######################################################################

tpcc_SOURCES =  \
                         backend/benchmark/benchmark_common.cpp \
				backend/benchmark/tpcc/tpcc.cpp \
                    backend/benchmark/tpcc/tpcc_configuration.cpp \
                    backend/benchmark/tpcc/tpcc_workload.cpp \
                    backend/benchmark/tpcc/tpcc_loader.cpp \
                    backend/benchmark/tpcc/tpcc_new_order.cpp \
                    backend/benchmark/tpcc/tpcc_delivery.cpp \
                    backend/benchmark/tpcc/tpcc_payment.cpp \
                    backend/benchmark/tpcc/tpcc_stock_level.cpp \
                    backend/benchmark/tpcc/tpcc_order_status.cpp \
                    backend/benchmark/tpcc/tpcc_scan_stock.cpp 

tpcc_LDFLAGS =
tpcc_CPPFLAGS = $(benchmark_common_cppflags)
tpcc_LDADD = $(benchmark_common_ldadd)


######################################################################
# SMALLBANK
######################################################################

smallbank_SOURCES =  \
                         backend/benchmark/benchmark_common.cpp \
                    backend/benchmark/smallbank/smallbank.cpp \
                    backend/benchmark/smallbank/smallbank_configuration.cpp \
                    backend/benchmark/smallbank/smallbank_loader.cpp \
                    backend/benchmark/smallbank/smallbank_workload.cpp \
                    backend/benchmark/smallbank/smallbank_amalgamate.cpp \
                    backend/benchmark/smallbank/smallbank_balance.cpp \
                    backend/benchmark/smallbank/smallbank_deposit_checking.cpp \
                    backend/benchmark/smallbank/smallbank_transact_saving.cpp \
                    backend/benchmark/smallbank/smallbank_write_check.cpp

smallbank_LDFLAGS =
smallbank_CPPFLAGS = $(benchmark_common_cppflags)
smallbank_LDADD = $(benchmark_common_ldadd)