#!/bin/bash
# recordcount="100000"
# dryrun="false"

# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_10servers.sh $recordcount $dryrun >> lsm_10servers_ranges_$recordcount

# 
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_10servers.sh $recordcount $dryrun > lsm_10servers_ranges_$recordcount
# recordcount="100000000"
# dryrun="false"
# recordcount="1000000000"
dryrun="false"

recordcount="200000000"
ncompaction="64"
level="4"

# bash /proj/bg-PG0/haoyu/scripts/env/init.sh 2

# sleep 100

bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_backup.sh $recordcount $dryrun
bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_ranges.sh $recordcount $dryrun


# for try in "1" "2" "3" "4" "5" "6" "7" "8" "9" "10"
# do
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_10servers_backup.sh $recordcount $dryrun "no_replica"
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_10servers.sh $recordcount $dryrun "no_replica" $try
# done
# bash /proj/bg-PG0/haoyu/scripts/nova_rocksdb_1tb_10servers.sh $recordcount $dryrun $level $ncompaction
