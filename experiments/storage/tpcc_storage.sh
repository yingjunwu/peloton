#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton/tree/mvcc-dev-scale
#configuration -- tpcc workload

filename=`echo $0 | cut -d . -f 1`".log"

cd ../build;


for warehouse in 40;
do
for core_cnt in {2,9,17,25,33,40};
#for core_cnt in {1,8,16,24,32,40};
do
for proto in {ton2o,to_central_rb,to_full_central_rb};
do
for gc in off;
do
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 180 ./src/tpcc -w ${warehouse} -b ${core_cnt} -p ${proto} -g ${gc} -k 1 -e -q tuple -l20 -P20 -n -y1 > /dev/null;
tail -n2 outputfile.summary | tee -a ${filename};
done; #gc
done; #proto
done; #core_cnt
done; #warehouse

