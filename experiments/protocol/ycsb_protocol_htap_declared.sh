#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton/tree/mvcc-dev-scale
#configuration -- ycsb workload

filename=`echo $0 | cut -d . -f 1`".log"

cd ../build;


for core_cnt in 20;
do
for ro_core_cnt in {0,4,8,12,16,20};
do
for opt in 100;
do
for update in {0.2,0.8};
do
for skew in 0.8;
do
for proto in {occ,pcc,sread,ewrite,to};
do
for gc in {co,va};
do
total_core_cnt=$(expr $core_cnt + $ro_core_cnt)
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 120 ./src/ycsb -z ${skew} -u ${update} -b ${total_core_cnt} -v ${ro_core_cnt} -p ${proto} -g ${gc} -o ${opt} -k 1000 -e -a > /dev/null;
tail -n2 outputfile.summary | tee -a ${filename};
done; #gc
done; #proto
done; #skew
done; #update
done; #opt
done; #reverse_core_cnt
done; #core_cnt