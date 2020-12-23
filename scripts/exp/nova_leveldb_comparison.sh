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

recordcount="2000000000"
ncompaction="64"
level="5"

# bash /proj/bg-PG0/haoyu/scripts/env/init.sh 2

# sleep 100

# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_backup.sh $recordcount $dryrun
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_ranges.sh $recordcount $dryrun
# bash /proj/bg-PG0/haoyu/scripts/nova_rocksdb_1tb_10servers_zipfian.sh $recordcount $dryrun $level $ncompaction

for i in {0..12}; 
do 
ssh node-$i "rm -rf /db/*"
done

bash /proj/bg-PG0/haoyu/scripts/nova_rocksdb_1tb_10servers.sh $recordcount $dryrun $level $ncompaction
for i in {0..12}; 
do 
ssh node-$i "rm -rf /db/*"
done

# bash /proj/bg-PG0/haoyu/scripts/nova_leveldb_10servers.sh $recordcount $dryrun $level $ncompaction

# for i in {0..12}; 
# do 
# ssh node-$i "rm -rf /db/*"
# done

# dist="uniform"
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_10servers_backup.sh $recordcount $dryrun $dist
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_10servers.sh $recordcount $dryrun "no_replica" $dist
# for i in {0..12}; 
# do 
# ssh node-$i "rm -rf /db/*"
# done
# dist="zipfian"
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_10servers_backup.sh $recordcount $dryrun $dist
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_10servers.sh $recordcount $dryrun "no_replica" $dist
