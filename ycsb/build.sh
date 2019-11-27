#!/bin/bash

HOME="/proj/bg-PG0/haoyu/YCSB-C"
END=0
#make clean
#cmake .
make

scp ycsb haoyu@node-0.Nova.bg-PG0.apt.emulab.net:$HOME/
#
#for ((i=0;i<=END;i++)); do
#    echo "building server on node $i"
#    scp -r * haoyu@node-$i.Nova2.bg-PG0.apt.emulab.net:$HOME/
#    ssh haoyu@node-$i.Nova2.bg-PG0.apt.emulab.net "cd $HOME && make"
#done
