#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton-logging/tree/mvcc-epoch
#commit -- 05075fe update configuration
#configuration -- smallbank workload
./cleanup_log.log
# command logging
echo "command logging-----------------------------------"
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/smallbank -k1 -z0 -b28 -goff -e -f10 -L command -D /data/ -C phylog -F /data/,/data/ -T dist -d 60 -I100
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/smallbank -k1 -z0 -b28 -goff -e -f10 -L command -D /data/ -C phylog -F /data/,/data/ -T dist -d 60 -I100 -R -M20 -P -N1 | grep "replay\|reload"

./cleanup_log.log
# phylog logging
echo "phylog logging-----------------------------------"
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/smallbank -k1 -z0 -b28 -goff -e -f10 -L phylog -D /data/ -C phylog -F /data/,/data/ -T dist -d 60 -I100
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/smallbank -k1 -z0 -b28 -goff -e -f10 -L phylog -D /data/ -C phylog -F /data/,/data/ -T dist -d 60 -I100 -R -M20 -P -N40 | grep "replay\|reload"

./cleanup_log.log
# physical logging
echo "physical logging-----------------------------------"
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/smallbank -k1 -z0 -b28 -goff -e -f10 -L physical -D /data/ -C physical -F /data/,/data/ -T dist -d 60 -I100
LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/smallbank -k1 -z0 -b28 -goff -e -f10 -L physical -D /data/ -C physical -F /data/,/data/ -T dist -d 60 -I100 -R -M20 -P -N40 | grep "replay\|reload"
