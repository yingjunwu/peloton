#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton/tree/mvcc-dev-scale
#configuration -- ycsb workload

filename=`echo $0 | cut -d . -f 1`

cd ../build;

LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 240 ./src/tpcc -w 40 -b 39 -p ton2o -g n2otxn -k 1 -e -q tuple -l20 -P20 -n -d 120 > /dev/null;
mv outputfile.summary ${filename}"_"${gc}"_warehouse40_noscan.log"

LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 240 ./src/tpcc -w 40 -b 40 -y 1 -p ton2o -g n2otxn -k 1 -e -q tuple -l20 -P20 -n -d 120 > /dev/null;
mv outputfile.summary ${filename}"_"${gc}"_warehouse40_scan.log"