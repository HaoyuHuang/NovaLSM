#!/bin/bash
recordcount="200000000"
dryrun="false"

# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_backup.sh $recordcount $dryrun > backup_out

for workload in "workloadw" "workloada" "workloade"
do
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_leveldb_ranges.sh $recordcount $dryrun $workload > lsm_ranges
bash /proj/bg-PG0/haoyu/scripts/nova_leveldb_bench.sh $recordcount $dryrun $workload
done

