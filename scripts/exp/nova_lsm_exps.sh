#!/bin/bash
recordcount="10000000"
dryrun="false"

number_of_ltcs="1"
for nservers in "4" "6" "11"
do
for num_memtable_partitions in "64"
do
for dist in "zipfian" "uniform"
do
bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_backup.sh $recordcount $dryrun $number_of_ltcs $nservers $num_memtable_partitions $dist  >> lsm_backup_out
done
done
done

bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_stoc_scale.sh $recordcount $dryrun > stoc_scale_out
bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_impact.sh $recordcount $dryrun > impact_out

dist="uniform"
nservers="11"
number_of_ltcs="1"
for num_memtable_partitions in "1" "2" "4" "8" "16" "32"
do
bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_backup.sh $recordcount $dryrun $number_of_ltcs $nservers $num_memtable_partitions $dist  >> lsm_backup_out
done


bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_stoc_vertical.sh $recordcount $dryrun > stoc_v_out
bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_stoc_scatter.sh $recordcount $dryrun > stoc_scatter_out


num_memtable_partitions="64"
dist="uniform"
for number_of_ltcs in "5" "4" "3" "2"
do
nservers=$((number_of_ltcs+10))
bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_backup.sh $recordcount $dryrun $number_of_ltcs $nservers $num_memtable_partitions $dist  >> lsm_backup_out
done

bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_ltc_scale.sh $recordcount $dryrun > ltc_scale_out
