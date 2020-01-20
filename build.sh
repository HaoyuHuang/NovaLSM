#!/bin/bash

HOME="/proj/bg-PG0/haoyu/nova/Nova-LevelDB"
BIN_HOME="/proj/bg-PG0/haoyu/nova"
host="Nova.bg-PG0.apt.emulab.net"

copy_src=${1}

#make clean
#cmake .
make -j32

scp nova_main haoyu@node-0.${host}:$BIN_HOME/

if [[ ${copy_src} == "true" ]]; then
scp -r *.h* haoyu@node-0.${host}:$HOME/ &
scp -r *.cpp haoyu@node-0.${host}:$HOME/ &
scp -r *.cc haoyu@node-0.${host}:$HOME/ &
scp -r CMakeLists.txt haoyu@node-0.${host}:$HOME/ &
fi

sleep 10
