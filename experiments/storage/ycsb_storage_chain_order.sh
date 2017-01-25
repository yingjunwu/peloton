#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton/tree/mvcc-dev-scale
#configuration -- ycsb workload

filename=`echo $0 | cut -d . -f 1`".log"

cd ../build;


for core_cnt in 40;
do
for skew in {0.1,0.3,0.5,0.7,0.9};
do
for update in {0.2,0.8};
do
for opt in 10;
do
proto=ton2o
gc=n2o
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 120 ./src/ycsb -z ${skew} -u ${update} -b ${core_cnt} -p ${proto} -g ${gc} -o ${opt} -k 1000 -e > /dev/null;
tail -n2 outputfile.summary | tee -a ${filename};
proto=to
gc=va
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 120 ./src/ycsb -z ${skew} -u ${update} -b ${core_cnt} -p ${proto} -g ${gc} -o ${opt} -k 1000 -e > /dev/null;
tail -n2 outputfile.summary | tee -a ${filename};
done; #opt
done; #update
done; #skew
done; #core_cnt