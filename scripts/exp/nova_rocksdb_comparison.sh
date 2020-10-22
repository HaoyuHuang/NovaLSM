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

# bash /proj/bg-PG0/haoyu/scripts/nova_single_server_leveldb_exp.sh
recordcount="100000000"


# recordcount="100000000"
# # bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_10servers_backup.sh $recordcount $dryrun > lsm_10servers_backup_$recordcount
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_10servers.sh $recordcount $dryrun "both" > lsm_10servers_ranges_$recordcount

# recordcount="1000000000"
# bash /proj/bg-PG0/haoyu/scripts/nova_leveldb_10servers.sh $recordcount $dryrun > lsm_leveldb_10servers_ranges_$recordcount

# level="2"
ncompaction="128"
recordcount="100000000"
# bash /proj/bg-PG0/haoyu/scripts/nova_rocksdb_10servers.sh $recordcount $dryrun $level $ncompaction
ncompaction="64"
recordcount="1000000000"
# recordcount="100000000"
# bash /proj/bg-PG0/haoyu/scripts/nova_leveldb_10servers.sh $recordcount $dryrun
bash /proj/bg-PG0/haoyu/scripts/nova_rocksdb_1tb_10servers.sh $recordcount $dryrun $level $ncompaction
recordcount="100000000"
bash /proj/bg-PG0/haoyu/scripts/nova_rocksdb_10servers.sh $recordcount $dryrun

# level="4"
# recordcount="100000000"
# for ncompaction in "32" "64"
# do
# # ncompaction="64"
# bash /proj/bg-PG0/haoyu/scripts/nova_rocksdb_bench.sh $recordcount $dryrun $level $ncompaction
# done
# level="4"
# recordcount="100000000"
# bash /proj/bg-PG0/haoyu/scripts/nova_rocksdb_bench.sh $recordcount $dryrun $level
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_stoc_scale.sh $recordcount $dryrun