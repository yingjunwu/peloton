#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton-logging/tree/mvcc-epoch
#commit -- 05075fe update configuration
#configuration -- tpcc workload

./cleanup_log.log
# command logging
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L command -D /data/ -C phylog -F /data/,/data/ -T dist -d 60 -I100 -goff
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L command -D /data/ -C phylog -F /data/,/data/ -T dist -d 60 -I100 -goff -R -M1 -P -N1 | grep "replay\|reload"

./cleanup_log.log
# phylog logging
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L phylog -D /data/ -C phylog -F /data/,/data/ -T dist -d 60 -I100 -goff
for i in 1 4 8 12 16 20 24 28 32 36 40;do LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L phylog -D /data/ -C phylog -F /data/,/data/ -T dist -d 60 -I100 -goff -R -M$i -P -N$i | grep "replay\|reload";done;

./cleanup_log.log
# physical logging
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L physical -D /data/ -C physical -F /data/,/data/ -T dist -d 60 -I100 -goff
for i in 1 4 8 12 16 20 24 28 32 36 40;do LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L physical -D /data/ -C physical -F /data/,/data/ -T dist -d 60 -I100 -goff -R -M$i -P -N$i | grep "replay\|reload";done;