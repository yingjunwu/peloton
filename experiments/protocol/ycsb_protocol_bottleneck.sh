filename=`echo $0 | cut -d . -f 1`".log"

cd ../build;

for core_cnt in {1,8,16,24,32,40};
do
for opt in {1,100};
do
for update in 0;
do
for skew in 0.2;
do
for proto in {occn2o,ton2o};
do
for gc in off;
do
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 120 ./src/ycsb -z ${skew} -u ${update} -b ${core_cnt} -p ${proto} -g ${gc} -o ${opt} -k 1000 -e > /dev/null;
tail -n2 outputfile.summary | tee -a ${filename};
done; #gc
done; #proto
done; #skew
done; #update
done; #opt
done; #core_cnt
