#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton/tree/mvcc-dev-scale
#configuration -- ycsb workload

filename=`echo $0 | cut -d . -f 1`".log"

cd ../build;


for opt in 10;
do
for update in {0.2,0.8};
do
for skew in {0.2,0.8};
do
for proto in occn2o;
do
for gc in n2otxn;
do
for core_cnt in {1,8,16,24,32,40};
do
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 120 ./src/ycsb -z ${skew} -u ${update} -b ${core_cnt} -p ${proto} -g ${gc} -o ${opt} -k 1000 -e -P20 > /dev/null;
tail -n2 outputfile.summary | tee -a ${filename};
done; #core_cnt
done; #gc
done; #proto
done; #skew
done; #update
done; #opt
