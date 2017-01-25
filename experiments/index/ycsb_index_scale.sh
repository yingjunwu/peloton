#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton/tree/mvcc-dev-scale
#commit -- https://github.com/yingjunwu/peloton/commit/4db6581dbc3103be38005cfadb5928a0d4cc557f
#configuration -- ycsb mixed workload

k=1000
filename=`echo $0 | cut -d . -f 1`".log"

cd ../build;

for sindex_type in version tuple;
do
for skew in {0.2,0.8};
do
for sindex_count in 20;
do
for gc_thread in 1;
do
for column in 22;
do
for core_cnt in {1,8,16,24,32,40};
do
for update in 0.8;
do
for opt in 10;
do
for proto in ton2o;
do
for gc in n2otxn;
do
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 120 ./src/ycsb -z ${skew} -u ${update} -b ${core_cnt} -p ${proto} -g ${gc} -o ${opt} -c ${column} -t ${gc_thread} -n ${sindex_count} -q ${sindex_type} -k ${k} -e -i hash > /dev/null;
tail -n2 outputfile.summary | tee -a ${filename};
done; #gc
done; #proto
done; #opt
done; #update
done; #skew
done; #core_cnt
done; #column
done; #gc_thread
done; #sindex_count
done; #sindex_type
