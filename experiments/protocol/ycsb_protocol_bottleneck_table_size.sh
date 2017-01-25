#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton/tree/mvcc-dev-scale
#configuration -- ycsb workload

filename=`echo $0 | cut -d . -f 1`".log"

cd ../build;


for core_cnt in 40;
do
for opt in 10;
do
for update in 0;
do
for skew in {0.2,0.8};
do
for proto in {occ,pcc,sread,ewrite,to};
do
for gc in off;
do
for size in {2000,4000,8000,10000,20000,40000};
do
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 120 ./src/ycsb -z ${skew} -u ${update} -b ${core_cnt} -p ${proto} -g ${gc} -o ${opt} -k ${size} -e -P20 -L20 > /dev/null;
tail -n2 outputfile.summary | tee -a ${filename};
done; #size
done; #gc
done; #proto
done; #skew
done; #update
done; #opt
done; #core_cnt
