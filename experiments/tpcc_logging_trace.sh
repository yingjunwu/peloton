#environment -- rtm.d1.comp.nus.edu.sg hyperthreading enabled
#branch -- https://github.com/yingjunwu/peloton-logging/tree/mvcc-epoch
#commit -- 05075fe update configuration
#configuration -- tpcc workload

# off
for i in 1 2 3 4 5 6 7 8 9 10;do ./cleanup_log.log && LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L off -D /data/ -C off -F /data/,/data/ -T dist -d 60 -I100 -goff && mv outputfile.summary outputfile.summary.off.$i;done;
# command logging
for i in 1 2 3 4 5 6 7 8 9 10;do ./cleanup_log.log && LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L command -D /data/ -C phylog -F /data/,/data/ -T dist -d 60 -I100 -goff && mv outputfile.summary outputfile.summary.command.$i;done;
# phylog logging
for i in 1 2 3 4 5 6 7 8 9 10;do ./cleanup_log.log && LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L phylog -D /data/ -C phylog -F /data/,/data/ -T dist -d 60 -I100 -goff && mv outputfile.summary outputfile.summary.phylog.$i;done;
# physical logging
for i in 1 2 3 4 5 6 7 8 9 10;do ./cleanup_log.log && LD_PRELOAD=/home/yingjun/jemalloc/lib/libjemalloc.so ./src/tpcc -w40 -l20 -b32 -t1 -k1 -e -a -f10 -n -L physical -D /data/ -C physical -F /data/,/data/ -T dist -d 60 -I100 -goff && mv outputfile.summary outputfile.summary.physical.$i;done;

