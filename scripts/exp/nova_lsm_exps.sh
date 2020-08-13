#!/bin/bash
recordcount="10000000"
dryrun="false"

number_of_ltcs="1"
num_sstable_replicas="1"
nranges_per_server="1"
# for nservers in "11"
# do
# for num_memtable_partitions in "64"
# do
# for num_sstable_replicas in "1" #"2" "3"
# do
# for dist in "zipfian"
# do
# done
# done
# done
# done

# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_replication.sh $recordcount $dryrun > stoc_scale_out

nranges_per_server="16"
number_of_ltcs="5"
num_sstable_replicas="1"
nservers="15"
num_memtable_partitions="4"
dist="zipfian"
zipfianconstant="0.99"

# for zipfianconstant in "0.99" "0.73" "0.27"
# do
# for nranges_per_server in "16" #"64" #"0.27"
# do
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_migration_backup.sh $recordcount $dryrun $number_of_ltcs $nservers $num_memtable_partitions $dist $num_sstable_replicas $nranges_per_server $zipfianconstant >> lsm_backup_out
# done
# done


dist="zipfian"
nranges_per_server="64"
zipfianconstant="0.99"
nservers="15"
number_of_ltcs="5"
cardinality="10"
# for zipfianconstant in "0.99" "0.73" "0.27"
# do
# for cardinality in "1" "10"
# do
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_migration_backup.sh $recordcount $dryrun $number_of_ltcs $nservers $num_memtable_partitions $dist $num_sstable_replicas $nranges_per_server $zipfianconstant $cardinality >> lsm_backup_out
# done
# done
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_ltc_migration.sh $recordcount $dryrun > stoc_scale_out


dist="uniform"
nservers="8"
number_of_ltcs="3"
zipfianconstant="0.00"
nranges_per_server="64"
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_elastic_backup.sh $recordcount $dryrun $number_of_ltcs $nservers $num_memtable_partitions $dist $num_sstable_replicas $nranges_per_server $zipfianconstant > lsm_backup_out
bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_elastic.sh $recordcount $dryrun > elastic_out



# dist="uniform"
# nservers="10"
# number_of_ltcs="3"
# zipfianconstant="0.99"
# nranges_per_server="64"
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_elastic_backup.sh $recordcount $dryrun $number_of_ltcs $nservers $num_memtable_partitions $dist $num_sstable_replicas $nranges_per_server $zipfianconstant > lsm_backup_out
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_elastic.sh $recordcount $dryrun > elastic_out

# number_of_ltcs="1"
# num_sstable_replicas="1"
# for nservers in "4" "6" "11"
# do
# for num_memtable_partitions in "64"
# do
# for dist in "zipfian" "uniform"
# do
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_backup.sh $recordcount $dryrun $number_of_ltcs $nservers $num_memtable_partitions $dist $num_sstable_replicas $nranges_per_server >> lsm_backup_out 
# done
# done
# done

# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_stoc_scale.sh $recordcount $dryrun > stoc_scale_out
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_impact.sh $recordcount $dryrun > impact_out

# dist="uniform"
# nservers="11"
# number_of_ltcs="1"
# for num_memtable_partitions in "1" "2" "4" "8" "16" "32"
# do
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_backup.sh $recordcount $dryrun $number_of_ltcs $nservers $num_memtable_partitions $dist $num_sstable_replicas $nranges_per_server >> lsm_backup_out
# done


# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_stoc_vertical.sh $recordcount $dryrun > stoc_v_out
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_stoc_scatter.sh $recordcount $dryrun > stoc_scatter_out


# num_memtable_partitions="64"
# dist="uniform"
# for number_of_ltcs in "5" "4" "3" "2"
# do
# nservers=$((number_of_ltcs+10))
# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_backup.sh $recordcount $dryrun $number_of_ltcs $nservers $num_memtable_partitions $dist $num_sstable_replicas $nranges_per_server >> lsm_backup_out
# done

# bash /proj/bg-PG0/haoyu/scripts/nova_lsm_subrange_ltc_scale.sh $recordcount $dryrun > ltc_scale_out
