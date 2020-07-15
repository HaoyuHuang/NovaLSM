#!/bin/bash
home_dir="/proj/bg-PG0/haoyu"
# home_dir="/proj/BG/haoyu"
config_dir="$home_dir/config"
db_dir="$home_dir/db"
script_dir="$home_dir/scripts"
cache_bin_dir="$home_dir/nova"
client_bin_dir="/tmp/YCSB-Nova"
results="/tmp/results"
recordcount="$1"
exp_results_dir="$home_dir/presentation-1h-large-nova-lsm-sr-scale-$recordcount"
dryrun="$2"

mkdir -p $results
mkdir -p $exp_results_dir

nservers="3"
nclients="6"

# YCSB
maxexecutiontime=300
workload="workloadc"
nthreads="16"
debug="false"
dist="zipfian"
cardinality="10"
value_size="4096"
operationcount="0"
zipfianconstant="0.99"
mem_pool_size_gb="32"
partition="range"

# CC
cc_nconn_workers="8"
num_rdma_fg_workers="8"
num_storage_workers="8"
num_compaction_workers="32"
block_cache_mb="1"
row_cache_mb="4096"
memtable_size_mb="4"
ltc_config_path=""
cc_nreplicas_per_range="1"
cc_nranges_per_server="1"
cc_log_buf_size="1024"
max_stoc_file_size_mb="4"
sstable_size_mb="2"
cc_stoc_files_path="/db/stoc_files"
ltc_num_stocs_scatter_data_blocks="3"
num_memtable_partitions="32"
number_of_ltcs="3"
cc_log_record_policy="exclusive"
cc_log_max_file_size_mb="18"

port=$((10000+RANDOM%1000))
rdma_port=$((20000+RANDOM%1000))
rdma_max_msg_size=$((256*1024))
rdma_max_num_sends="32"
rdma_doorbell_batch_size="8"
enable_load_data="true"
enable_rdma="true"
num_memtables="2"

log_record_mode="none"
num_log_replicas="1"
# zipfian_dist_file_path="/tmp/zipfian-$recordcount"
zipfian_dist_file_path=""
try="0"
function run_bench() {
	servers=()
	clis=()
	machines=()
	i=0
	n=0
	while [ $n -lt $nservers ]
	do
		# if [[ $i == "2" ]]; then
		# 	i=$((i+1))
		# 	continue
		# fi
		servers+=("node-$i")
		i=$((i+1))
		n=$((n+1))
	done

	for ((i=0;i<nclients;i++));
	do
		id=$((nmachines-1-i))
		clis+=("node-$id")
	done

	for ((i=0;i<nmachines;i++));
	do
		id=$((i))
		machines+=("node-$id")
	done

	echo ${clis[@]}
	echo ${servers[@]}
	echo ${machines[@]}

	nova_servers=""
	nova_all_servers=""
	i="0"
	for s in ${servers[@]}
	do
		nova_port="$port"
		nova_servers="$nova_servers,$s:$nova_port"
		i=$((i+1))
		if [[ $i -le number_of_ltcs ]]; then
			nova_all_servers="$nova_all_servers,$s:$nova_port"
		fi
		nova_port=$((nova_port+1))
	done
	nova_servers="${nova_servers:1}"
	nova_all_servers="${nova_all_servers:1}"

	current_time=$(date "+%Y-%m-%d-%H-%M-%S")
	nstoc=$((nservers-number_of_ltcs))
	result_dir_name="nova-d-$dist-w-$workload-ltc-$number_of_ltcs-stoc-$nstoc-l0-$cc_l0_stop_write_gb-np-$num_memtable_partitions-sr-$num_sstable_replicas-f-$failure_duration-el-$enable_lookup_index"
	echo "running experiment $result_dir_name"

	# Copy the files over local node
    dir="$exp_results_dir/$result_dir_name"
    echo "Result Dir $dir..."
    rm -rf $dir
    mkdir -p $dir
    chmod -R 777 $dir

	cmd="java -jar $cache_bin_dir/nova_config_generator.jar $config_dir "shared" $recordcount $number_of_ltcs $cc_nreplicas_per_range $cc_nranges_per_server"
	echo $cmd
	eval $cmd
	ltc_config_path="$config_dir/nova-shared-cc-nrecords-$recordcount-nccservers-$number_of_ltcs-nlogreplicas-$cc_nreplicas_per_range-nranges-$cc_nranges_per_server"
	
	db_path="/db/nova-db-$recordcount-$value_size"
	echo "$nova_servers $ltc_config_path $db_path"
	echo "cc servers $nova_all_servers"
	if [[ $dryrun == "true" ]]; then
		return
	fi

	for m in ${machines[@]}
	do
		echo "remove $results at machine $m"
    	ssh -oStrictHostKeyChecking=no $m "sudo rm -rf $results && mkdir -p $results && chmod -R 777 $results"
    	ssh -oStrictHostKeyChecking=no $m "sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'"
	done

	# restore the database image. 
	for s in ${servers[@]}
	do
		echo "restore database image $s"
		ssh -oStrictHostKeyChecking=no $s "rm -rf /db/nova-db-$recordcount-1024/ && cp -r /db/snapshot-$nservers-$number_of_ltcs-$dist-$num_memtable_partitions-$memtable_size_mb-$zipfianconstant-$num_sstable_replicas/nova-db-$recordcount-1024/ /db/ &" &
	done

	for m in ${machines[@]}
	do
		while ssh -oStrictHostKeyChecking=no $m "ps -ef | grep -v grep | grep -v ssh | grep -v bash | grep -c \"cp -r\""
		do
			sleep 10
			echo "waiting for $m"
		done
	done
	
	# start stats
	echo "Preparing sar"
	for m in ${machines[@]}
	do
		ssh -oStrictHostKeyChecking=no $m "sudo killall leveldb_main nova_shared_main nova_multi_thread_compaction nova_server_main java collectl sar"
		ssh -oStrictHostKeyChecking=no $m "sudo collectl -scx -i 1 -P > $results/$m-coll.txt &"
		ssh -oStrictHostKeyChecking=no $m "sar -P ALL 1 > $results/$m-cpu.txt &"
	    ssh -oStrictHostKeyChecking=no $m "sar -n DEV 1 > $results/$m-net.txt &"
	    ssh -oStrictHostKeyChecking=no $m "sar -r 1 > $results/$m-mem.txt &"
	    ssh -oStrictHostKeyChecking=no $m "sar -d 1 > $results/$m-disk.txt &"
	done

	for m in ${machines[@]}
	do
		while ssh -oStrictHostKeyChecking=no $m "ps -ef | grep -v grep | grep -v ssh | grep -v bash | grep -c nova_server_main"
		do
			sleep 10
			echo "waiting for $m"
		done
	done

	server_id=0
	for s in ${servers[@]}
	do
		echo "creating server on $s"
		nova_rdma_port=$((rdma_port))
		cmd="stdbuf --output=0 --error=0 ./nova_server_main --fail_stoc_id=$fail_stoc_id --exp_seconds_to_fail_stoc=$exp_seconds_to_fail_stoc --failure_duration=$failure_duration --num_sstable_replicas=$num_sstable_replicas --level=$level --l0_start_compaction_mb=$l0_start_compaction_mb --enable_range_index=$enable_range_index --subrange_no_flush_num_keys=$subrange_no_flush_num_keys --enable_detailed_db_stats=$enable_detailed_db_stats --major_compaction_type=$major_compaction_type --major_compaction_max_parallism=$major_compaction_max_parallism --major_compaction_max_tables_in_a_set=$major_compaction_max_tables_in_a_set --enable_flush_multiple_memtables=$enable_flush_multiple_memtables --recover_dbs=$recover_dbs --num_recovery_threads=$num_recovery_threads  --sampling_ratio=1 --zipfian_dist_ref_counts=$zipfian_dist_file_path --client_access_pattern=$dist  --memtable_type=static_partition --enable_subrange=$enable_subrange --num_log_replicas=$num_log_replicas --log_record_mode=$log_record_mode --scatter_policy=$scatter_policy --number_of_ltcs=$number_of_ltcs --enable_lookup_index=$enable_lookup_index --l0_stop_write_mb=$l0_stop_write_mb --num_memtable_partitions=$num_memtable_partitions --num_memtables=$num_memtables --num_rdma_bg_workers=$num_rdma_bg_workers --db_path=$db_path --num_storage_workers=$num_storage_workers --stoc_files_path=$cc_stoc_files_path --max_stoc_file_size_mb=$max_stoc_file_size_mb --sstable_size_mb=$sstable_size_mb --ltc_num_stocs_scatter_data_blocks=$ltc_num_stocs_scatter_data_blocks --all_servers=$nova_servers --server_id=$server_id --mem_pool_size_gb=$mem_pool_size_gb --use_fixed_value_size=$value_size --ltc_config_path=$ltc_config_path --ltc_num_client_workers=$cc_nconn_workers --num_rdma_fg_workers=$num_rdma_fg_workers --num_compaction_workers=$num_compaction_workers --block_cache_mb=$block_cache_mb --row_cache_mb=$row_cache_mb --memtable_size_mb=$memtable_size_mb --cc_log_buf_size=$cc_log_buf_size --rdma_port=$rdma_port --rdma_max_msg_size=$rdma_max_msg_size --rdma_max_num_sends=$rdma_max_num_sends --rdma_doorbell_batch_size=$rdma_doorbell_batch_size --enable_rdma=$enable_rdma --enable_load_data=$enable_load_data --use_local_disk=$use_local_disk"
		echo "$cmd"
		ssh -oStrictHostKeyChecking=no $s "mkdir -p $cc_stoc_files_path && mkdir -p $db_path && cd $cache_bin_dir && $cmd >& $results/server-$s-out &" &
		server_id=$((server_id+1))
		nova_rdma_port=$((nova_rdma_port+1))
		sleep 1
	done

	sleep 30
	for c in ${clis[@]}
	do
		for i in $(seq 1 $nclients_per_server);
		do
			echo "creating client on $c-$i"
			cmd="stdbuf --output=0 --error=0 bash $script_dir/run_ycsb.sh $nthreads $nova_all_servers $debug $partition $recordcount $maxexecutiontime $dist $value_size $workload $ltc_config_path $cardinality $operationcount $zipfianconstant 0"
			echo "$cmd"
			ssh -oStrictHostKeyChecking=no $c "cd $client_bin_dir && $cmd >& $results/client-$c-$i-out &" &
		done
	done
	
	port=$((port+1))
	rdma_port=$((rdma_port+1))
	sleep 10
	sleep_time=0
	stop="false"
	max_wait_time=$((maxexecutiontime+1200))
	for m in ${clis[@]}
	do
		while ssh -oStrictHostKeyChecking=no $m "ps -ef | grep -v \"grep --color=auto ycsb\" | grep -v ssh | grep -v bash | grep ycsb | grep -c java"
		do
			sleep 10
			sleep_time=$((sleep_time+10))
			echo "waiting for $m for $sleep_time seconds"

			if [[ $sleep_time -gt $max_wait_time ]]; then
				stop="true"
				break
			fi
		done
		if [[ $stop == "true" ]]; then
			echo "exceeded maximum wait time"
			break
		fi
	done

	# DB size. 
	for s in ${servers[@]}
	do
		cmd="du -sm $db_path"
		echo "$cmd"
		ssh -oStrictHostKeyChecking=no $s "$cmd >& $results/server-$s-db-disk-space"
	done

	for s in ${servers[@]}
	do
		cmd="du -sm $cc_stoc_files_path"
		echo "$cmd"
		ssh -oStrictHostKeyChecking=no $s "$cmd >& $results/server-$s-rtable-disk-space"
	done

    for m in ${machines[@]}
    do
    	echo "kill java at $m"
    	ssh -oStrictHostKeyChecking=no $m "sudo killall leveldb_main nova_shared_main nova_multi_thread_compaction nova_server_main java collectl sar"
    done

    dir="$exp_results_dir/$result_dir_name"
    echo "Save to $dir..."
    rm -rf $dir
    mkdir -p $dir
    chmod -R 777 $dir

	# DB logs.
    server_id=0
	for s in ${servers[@]}
	do
		ssh -oStrictHostKeyChecking=no $s "mkdir -p $results/server-$server_id-dblogs/ && cp -r $db_path/*/*/LOG* $results/server-$server_id-dblogs/"
		ssh -oStrictHostKeyChecking=no $s "rm -rf $db_path && rm -rf $cc_stoc_files_path"
		server_id=$((server_id+1))
	done

    for m in ${machines[@]}
    do
        scp -r $m:$results/* $dir
    done
    sleep 10
}

value_size="1024"
mem_pool_size_gb="30"
partition="range"
zipfianconstant="0.99"

block_cache_mb="0"
cc_nreplicas_per_range="1"
enable_rdma="true"
row_cache_mb="0"

cc_l0_stop_write_gb="10"
enable_lookup_index="true"

cc_nconn_workers="512"
num_rdma_fg_workers="16"
num_rdma_bg_workers="16"
num_compaction_workers="128"
num_storage_workers="128"

nservers="11"
number_of_ltcs="1"
nclients="5"
dist="zipfian"
workload="workloadw"
nclients_per_server="5"
nthreads="512"

scatter_policy="power_of_two"

memtable_size_mb="16"
sstable_size_mb="16"
use_local_disk="true"

ltc_num_stocs_scatter_data_blocks="1"
max_stoc_file_size_mb="18432"

log_record_mode="none"
num_log_replicas="0"
cc_nranges_per_server="1"
num_memtable_partitions="32"
num_memtables="64"

nmachines="18"
nclients="6"
number_of_ltcs="1"
# nclients="1"
maxexecutiontime=1200
workload="workloadw"
enable_load_data="false"
mem_pool_size_gb="20"

major_compaction_type="sc"
num_recovery_threads="32"
recover_dbs="true"
enable_subrange="true"
enable_detailed_db_stats="false"

l0_start_compaction_mb="4096"
log_record_mode="none"
num_log_replicas="0"
cc_nranges_per_server="1"
# enable_flush_multiple_memtables="false"
# subrange_no_flush_num_keys="0"

enable_flush_multiple_memtables="true"
subrange_no_flush_num_keys="100"

num_memtable_partitions="64"
use_local_disk="false"
num_memtables="256"
major_compaction_type="sc"
major_compaction_max_parallism="64"
major_compaction_max_tables_in_a_set="50"
enable_load_data="false"
num_memtable_partitions="64"
level="6"
number_of_ltcs="1"
nmachines="20"
nclients="5"
num_sstable_replicas="1"

exp_seconds_to_fail_stoc="-1"
fail_stoc_id="-1"
num_sstable_replicas="1"
failure_duration="0"
cc_l0_stop_write_gb="10"
l0_stop_write_mb=$((cc_l0_stop_write_gb*1024))

nservers="11"


enable_range_index="true"
workload="workloada"
for enable_lookup_index in "false" #"true"
do
for dist in "uniform" #"zipfian"
do
run_bench
done
done

enable_lookup_index="true"
nservers="11"
workload="workloadw"
dist="uniform"
fail_stoc_id="-1"
failure_duration="0"
exp_seconds_to_fail_stoc="-1"
num_sstable_replicas="1"
enable_range_index="false"

# maxexecutiontime="7200"

num_memtable_partitions="1"
num_memtables="2"
major_compaction_max_parallism="1"
# run_bench

# nservers="2"
# run_bench

# nservers="2"
# major_compaction_max_parallism="64"
# num_memtable_partitions="64"
# num_memtables="256"
# run_bench

nservers="11"
major_compaction_max_parallism="64"
num_memtable_partitions="64"
num_memtables="256"
# run_bench


maxexecutiontime="1200"

# for nservers in "11"
# do

# for num_sstable_replicas in "1" "2" "3"
# do
# for workload in "workloadw" "workloada"  "workloade"
# do
# for dist in "uniform"
# do
# if [[ $workload == "workloade" ]]; then
# 	enable_range_index="true"
# else
# 	enable_range_index="false"
# fi

# run_bench
# done
# done
# done

# num_sstable_replicas="3"
# exp_seconds_to_fail_stoc="600"
# fail_stoc_id="5"
# for failure_duration in "1" "30"
# do
# for workload in "workloadw" #"workloada"  "workloade"
# do
# for dist in "uniform" #"zipfian"
# do
# if [[ $workload == "workloade" ]]; then
# 	enable_range_index="true"
# else
# 	enable_range_index="false"
# fi
# run_bench
# done
# done
# done

# done

python /proj/bg-PG0/haoyu/latest/parse_ycsb_nova_leveldb.py 25 $exp_results_dir > scale_out
