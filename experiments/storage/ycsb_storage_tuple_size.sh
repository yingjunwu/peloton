#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton/tree/mvcc-dev-scale
#configuration -- ycsb workload

filename=`echo $0 | cut -d . -f 1`".log"

cd ../build;


for column in {10,100};
do
for core_cnt in 40;
do
for skew in 0.2;
do
for update in {0,0.2,0.4,0.6,0.8,1};
do
for opt in 10;
do
# for proto in {ton2o,to_central_rb,torb,tofullrb,to_full_central_rb};
for proto in {ton2o,to_central_rb,to_full_central_rb};
do
for gc in off;
do
for preallocation in {1,20};
do
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 120 ./src/ycsb -z ${skew} -u ${update} -b ${core_cnt} -p ${proto} -g ${gc} -o ${opt} -c ${column} -k 1000 -e -P ${preallocation} > /dev/null;
tail -n2 outputfile.summary | tee -a ${filename};
done;
done; #gc
done; #proto
done; #opt
done; #update
done; #skew
done; #core_cnt
done; #column