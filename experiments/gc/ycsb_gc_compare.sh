#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton/tree/mvcc-dev-scale
#configuration -- ycsb workload

filename=`echo $0 | cut -d . -f 1`

cd ../build;


for core_cnt in 40;
do
for opt in 10;
do
for update in {0.2,0.8};
do
for skew in 0.8;
do
for proto in ton2o;
do
for gc in {n2o,n2otxn,off};
do
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 240 ./src/ycsb -z ${skew} -u ${update} -b ${core_cnt} -p ${proto} -g ${gc} -o ${opt} -k 1000 -e -d 120 > /dev/null;
mv outputfile.summary ${filename}"_"${gc}"_update"${update}"_skew"${skew}.log
done; #gc
done; #proto
done; #skew
done; #update
done; #opt
done; #core_cnt


for core_cnt in 40;
do
for opt in 10;
do
for update in {0.2,0.8};
do
for skew in 0.8;
do
for proto in to;
do
for gc in {va,co,off};
do
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 240 ./src/ycsb -z ${skew} -u ${update} -b ${core_cnt} -p ${proto} -g ${gc} -o ${opt} -k 1000 -e -d 120 > /dev/null;
mv outputfile.summary ${filename}"_tuple_"${gc}"_update"${update}"_skew"${skew}.log
done; #gc
done; #proto
done; #skew
done; #update
done; #opt
done; #core_cnt
