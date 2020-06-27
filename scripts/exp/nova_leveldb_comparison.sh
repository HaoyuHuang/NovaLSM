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

recordcount="10000000"
bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_backup.sh $recordcount $dryrun
bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_stoc_scale.sh $recordcount $dryrun


recordcount="1000000000"
bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_10servers_backup.sh $recordcount > lsm_10servers_backup_$recordcount
bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_10servers.sh $recordcount $dryrun "no_replica" > lsm_10servers_ranges_$recordcount
