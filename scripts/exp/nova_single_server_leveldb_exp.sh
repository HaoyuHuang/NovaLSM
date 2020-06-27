#!/bin/bash
recordcount="10000000"
dryrun="false"


# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_backup.sh $recordcount $dryrun > backup_out
bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_ranges.sh $recordcount $dryrun > lsm_ranges
# bash /proj/bg-PG0/haoyu/scripts/nova_leveldb_bench.sh $recordcount $dryrun > leveldb_ranges

