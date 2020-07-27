#!/bin/bash

END=$1
REMOTE_HOME="/proj/BG"
REMOTE_HOME="/proj/bg-PG0"
HOME="/users/haoyu"
setup_script="$REMOTE_HOME/haoyu/scripts/env"
limit_dir="$REMOTE_HOME/haoyu/scripts"
LOCAL_HOME="/home/haoyuhua/Documents/nova/NovaLSM/scripts/bootstrap"


host="Nova.bg-PG0.apt.emulab.net"
scp -r $LOCAL_HOME/env/*sh haoyu@node-0.${host}:/proj/bg-PG0/haoyu/scripts/env/

for ((i=0;i<END;i++)); do
    echo "building server on node $i"
    ssh -oStrictHostKeyChecking=no haoyu@node-$i.${host} "bash $setup_script/setup-ssh.sh"
done

for ((i=0;i<END;i++)); do
    echo "building server on node $i"
    ssh -oStrictHostKeyChecking=no haoyu@node-$i.${host} "sudo cp $limit_dir/ulimit.conf /etc/systemd/user.conf"
    ssh -oStrictHostKeyChecking=no haoyu@node-$i.${host} "sudo cp $limit_dir/sys_ulimit.conf /etc/systemd/system.conf"
    ssh -oStrictHostKeyChecking=no haoyu@node-$i.${host} "sudo cp $limit_dir/limit.conf /etc/security/limits.conf"
    ssh -oStrictHostKeyChecking=no haoyu@node-$i.${host} "sudo reboot"
done
