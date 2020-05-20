#!/bin/bash
scp nova_shared_main     haoyu@node-0.Nova.bg-PG0.apt.emulab.net:~/nova_pool &
scp nova_shared_main     haoyu@node-1.Nova.bg-PG0.apt.emulab.net:~/nova_pool &
#scp nova_shared_main     haoyu@node-2.Nova.bg-PG0.apt.emulab.net:~/nova_pool &
#scp nova_shared_main     haoyu@node-3.Nova.bg-PG0.apt.emulab.net:~/nova_pool &
sleep 1000
