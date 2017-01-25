#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton-logging/tree/mvcc-epoch
#commit -- 05075fe update configuration
#configuration -- tpcc workload

for i in 0 0.2 0.4 0.6 0.8 1;
do
./cleanup_log.log;
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L command -D /data/ -C phylog -F /data/,/data/ -T dist -d 60 -I100 -goff -A $i;
mv outputfile.summary outputfile.summary.adhoc.$i;
done;
