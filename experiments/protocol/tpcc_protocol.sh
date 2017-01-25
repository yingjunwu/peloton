filename=`echo $0 | cut -d . -f 1`".log"

cd ../build;

for warehouse in 40;
do
for proto in {occ,pcc,to};
do
for gc in co;
do
for scalefactor in 1;
do
for core_cnt in {1,8,16,24,32,40};
do
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 180 ./src/tpcc -w ${warehouse} -b ${core_cnt} -p ${proto} -g ${gc} -k ${scalefactor} -e -q tuple -l20 -P20 -n > /dev/null;
tail -n2 outputfile.summary | tee -a ${filename};
done; #core_cnt
done;
done; #gc
done; #proto
done; #warehouse


# for core_cnt in {1,8,16,24,32,40};
# do
# for proto in {occ,pcc,sread,ewrite,to};
# do
# for gc in co;
# do
# LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so timeout 180 ./src/tpcc -w ${core_cnt} -b ${core_cnt} -p ${proto} -g ${gc} -k 1 -e -q tuple -l20 > /dev/null;
# tail -n2 outputfile.summary | tee -a ${filename};
# done; #gc
# done; #proto
# done; #core_cnt
