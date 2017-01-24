#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton-logging/tree/mvcc-epoch
#commit -- 05075fe update configuration
#configuration -- tpcc workload
#requirement: need to command out install tuple in phylog logger and physical logger and transaction re-execution in command logger

./cleanup_log.log
# command logging
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L command -D /dev/shm/ -C off -F /data/,/data/ -T dist -d 60 -I100 -goff
for i in 1 4 8 12 16 20 24 28 32 36 40;do LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L command -D /data/ -C off -F /data/,/data/ -T dist -d 60 -I100 -goff -P -N$i | grep "replay log duration";done;

./cleanup_log.log
# phylog logging
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L phylog -D /dev/shm/ -C off -F /data/,/data/ -T dist -d 60 -I100 -goff
for i in 1 4 8 12 16 20 24 28 32 36 40;do LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L phylog -D /data/ -C off -F /data/,/data/ -T dist -d 60 -I100 -goff -P -N$i | grep "replay log duration";done;

./cleanup_log.log
# physical logging
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L physical -D /dev/shm/ -C off -F /data/,/data/ -T dist -d 60 -I100 -goff
for i in 1 4 8 12 16 20 24 28 32 36 40;do LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L physical -D /data/ -C off -F /data/,/data/ -T dist -d 60 -I100 -goff -P -N$i | grep "replay log duration";done;