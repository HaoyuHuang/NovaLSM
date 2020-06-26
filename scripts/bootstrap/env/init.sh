#!/bin/bash

basedir="/proj/bg-PG0/haoyu"
numServers=$1
prefix="h"

for ((i=0;i<numServers;i++)); do
	echo "*******************************************"
	echo "*******************************************"
   	echo "******************* node-$i ********************"
   	echo "*******************************************"
   	echo "*******************************************"
 	ssh -oStrictHostKeyChecking=no node-$i "sudo apt-get update"
    ssh -oStrictHostKeyChecking=no node-$i "sudo apt-get --yes install screen"
    ssh -n -f -oStrictHostKeyChecking=no node-$i screen -L -S env1 -dm "$basedir/scripts/env/setup-all.sh"
done

sleep 10

sleepcount="0"

for ((i=0;i<numServers;i++)); 
do
	while ssh -oStrictHostKeyChecking=no  node-$i "screen -list | grep -q env1"
	do 
		((sleepcount++))
		sleep 10
		echo "waiting for node-$i "
	done

done

echo "init env took $((sleepcount/6)) minutes"
