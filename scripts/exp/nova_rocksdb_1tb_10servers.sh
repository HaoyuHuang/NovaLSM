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
exp_results_dir="$home_dir/sigmod-rocksdb-drain-10-servers-mid-load-$recordcount"
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

# Server
nconn_workers="8"
nasync_workers="8"
# ncompaction_workers="32"
cache_size_gb="32"
value_size="4096"
partition="range"
write_buffer_size_mb="4"
nreplicas_per_range="1"
nranges_per_server="1"
persist_log_record="local"
log_buf_size="1"
zipfianconstant="0.99"

port=$((10000+RANDOM%1000))
max_msg_size=$((256*1024))
enable_profiling="false"
sstable_mode="disk"
operationcount="0"
sstable_size_mb="2"
try="1"

# rm -rf $exp_results_dir

function run_bench() {
	servers=()
	clis=()
	machines=()

	i=0
	n=0
	while [ $n -lt $nservers ]
	do
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
	for s in ${servers[@]}
	do
		nova_port="$port"
		nova_servers="$nova_servers,$s:$nova_port"
		nova_port=$((nova_port+1))
	done

	nova_servers="${nova_servers:1}"
	current_time=$(date "+%Y-%m-%d-%H-%M-%S")
	# log_buf_size=$((log_buf_size_mb*1024*1024))

	result_dir_name="nova-d-$dist-w-$workload-l-$level-ltc-$nservers-l0-$l0_stop_write_mb-ss-$write_buffer_size_mb-sub-$num_max_subcompactions-nr-$nranges_per_server-nc-$ncompaction_workers"
	echo "running experiment $result_dir_name"

	# Copy the files over local node
    dir="$exp_results_dir/$result_dir_name"
    echo "Save to $dir..."
    sudo rm -rf $dir
    sudo mkdir -p $dir
    sudo chmod -R 777 $dir

	config_path="$config_dir/nova-shared-nrecords-$recordcount-nltc-$nservers-nstoc-$nservers-nranges-$nranges_per_server-zipfian-0.00-read-1"
	db_path="/db/nova-db-$recordcount-$value_size"
	echo "$nova_servers $config_path $db_path"
	if [[ $dryrun == "true" ]]; then
		return
	fi

	for m in ${machines[@]}
	do
		echo "remove $results at machine $m"
    	ssh -oStrictHostKeyChecking=no $m "sudo rm -rf $results && sudo mkdir -p $results && sudo chmod -R 777 $results"
    	ssh -oStrictHostKeyChecking=no $m "sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'"
	done
	
	# start stats
	echo "Preparing sar"
	for m in ${machines[@]}
	do
		ssh -oStrictHostKeyChecking=no $m "sudo killall rocksdb_main leveldb_main nova_server_main nova_shared_main nova_multi_thread_compaction java collectl sar"
		ssh -oStrictHostKeyChecking=no $m "sudo collectl -scx -i 1 -P > $results/$m-coll.txt &"
		ssh -oStrictHostKeyChecking=no $m "sar -P ALL 1 > $results/$m-cpu.txt &"
	    ssh -oStrictHostKeyChecking=no $m "sar -n DEV 1 > $results/$m-net.txt &"
	    ssh -oStrictHostKeyChecking=no $m "sar -r 1 > $results/$m-mem.txt &"
	    ssh -oStrictHostKeyChecking=no $m "sar -d 1 > $results/$m-disk.txt &"
	done

	for m in ${servers[@]}
	do
		while ssh -oStrictHostKeyChecking=no $m "ps -ef | grep -v grep | grep -v ssh | grep -v bash | grep -c rocksdb_main"
		do
			sleep 10
			echo "waiting for $m"
		done
	done

	server_id=0
	if [[ "$enable_profiling" == "true" ]]; then
		profiler_file_path="$results"
	fi

	for s in ${servers[@]}
	do
		echo "creating servers on $s"
		nova_rdma_port=$((rdma_port))
		cmd="stdbuf --output=0 --error=0 ./rocksdb_main --size_ratio=$size_ratio --num_max_subcompactions=$num_max_subcompactions --max_msg_size=$max_msg_size --num_memtables=$num_memtables --level=$level --l0_start_compaction_mb=$l0_start_compaction_mb --l0_stop_write_mb=$l0_stop_write_mb --block_cache_mb=$block_cache_mb --db_path=$db_path --write_buffer_size_mb=$write_buffer_size_mb --servers=$nova_servers --server_id=$server_id --recordcount=$recordcount --data_partition_alg=$partition --num_conn_workers=$nconn_workers --num_async_workers=$nasync_workers --num_compaction_workers=$ncompaction_workers --cache_size_gb=$cache_size_gb --use_fixed_value_size=$value_size --config_path=$config_path --enable_load_data=true"
		echo "$cmd"
		ssh -oStrictHostKeyChecking=no $s "rm -rf $db_path && mkdir -p $db_path && cd $cache_bin_dir && $cmd >& $results/server-$s-out &" &
		server_id=$((server_id+1))
		nova_rdma_port=$((nova_rdma_port+1))
		sleep 1
	done

	echo "warmup..."
	c=${clis[0]}
	i="1"
	echo "creating client on $c-$i"
	cmd="stdbuf --output=0 --error=0 bash $script_dir/run_ycsb.sh 512 $nova_servers $debug $partition $recordcount 600 $dist $value_size workloadw $config_path $cardinality $operationcount $zipfianconstant 0"
	echo "$cmd"
	ssh -oStrictHostKeyChecking=no $c "cd $client_bin_dir && $cmd >& $results/client-$c-$i-out"

	# echo "warmup complete..."
	# java -jar $cache_bin_dir/nova_client_stats.jar $nova_servers "drain"
	# sleep 10

	java -jar $cache_bin_dir/nova_client_stats.jar $nova_servers
	java -jar $cache_bin_dir/nova_client_stats.jar $nova_servers
	java -jar $cache_bin_dir/nova_client_stats.jar $nova_servers
	sleep 10

	for c in ${clis[@]}
	do
		for i in $(seq 1 $nclients_per_server);
		do
			echo "creating client on $c-$i"
			cmd="stdbuf --output=0 --error=0 bash $script_dir/run_ycsb.sh $nthreads $nova_servers $debug $partition $recordcount $maxexecutiontime $dist $value_size $workload $config_path $cardinality $operationcount $zipfianconstant 0"
			echo "$cmd"
			ssh -oStrictHostKeyChecking=no $c "cd $client_bin_dir && $cmd >& $results/client-$c-$i-out &" &
		done
	done
	
	port=$((port+1))
	rdma_port=$((rdma_port+1))
	sleep 10
	sleep_time=0
	stop="false"
	max_wait_time=$((maxexecutiontime+4000))
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

    for m in ${machines[@]}
    do
    	echo "kill java at $m"
    	ssh -oStrictHostKeyChecking=no $m "sudo killall leveldb_main nova_server_main nova_shared_main nova_multi_thread_compaction java collectl sar"
    done

	dir="$exp_results_dir/$result_dir_name"
    echo "Save to $dir..."
    sudo rm -rf $dir
    sudo mkdir -p $dir
    sudo chmod -R 777 $dir

	# DB logs.
    server_id=0
	for s in ${servers[@]}
	do
		ssh -oStrictHostKeyChecking=no $s "mkdir -p $results/server-$server_id-dblogs/ && cp -r $db_path/*/*/LOG* $results/server-$server_id-dblogs/"
		ssh -oStrictHostKeyChecking=no $s "rm -rf $db_path"
		server_id=$((server_id+1))
	done

    for m in ${machines[@]}
    do
        scp -r $m:$results/* $dir
    done
}

# server configurations. 
enable_profiling="false"
sstable_mode="disk"
log_buf_size="1048576"
enable_rdma="false"

nconn_workers="256"
nasync_workers="32"
ncompaction_workers="$3"

nreplicas_per_range="1"
write_buffer_size_mb="16"
sstable_size_mb="16"
block_cache_mb="0"
persist_log_record="nic"
nranges_per_server="128"

# client configurations. 
dist="uniform"
value_size="1024"
workload="workloada"
nthreads="512"
workload="workloadw"
zipfianconstant="0.99"
operationcount=0
maxexecutiontime=1200

# setup.
nservers="5"
nclients="6"
nthreads="512"
dist="uniform"

nclients="4"
nthreads="512"
dist="zipfian"
nreplicas_per_range="1"
enable_rdma="false"
write_buffer_size_mb="16"
nconn_workers="512"
nclients_per_server="5"
persist_log_record="none"
nservers="10"
nmachines="13"
nclients="3"
cache_size_gb="22"
level="2"
cardinality="10"
cache_size_gb="26"
nranges_per_server="1"


dist="uniform"
workload="workloada"
nranges_per_server="1"
num_memtables="128"

l0_start_compaction_mb=$((4*1024))
l0_stop_write_mb=$((10*1024))
nclients="3"
nclients_per_server="1"
nthreads="20"

# ncompaction_workers="32"
num_max_subcompactions="1"
size_ratio="3.2"
level="5"
# ncompaction_workers="128"
num_max_subcompactions="1"
# for nranges_per_server in "64"
# do
# l0_start_compaction_mb=$((4*1024))
# l0_stop_write_mb=$((10*1024))
# num_memtables="128"
# num_memtables=$((num_memtables/nranges_per_server))
# l0_start_compaction_mb=$((l0_start_compaction_mb/nranges_per_server))
# l0_stop_write_mb=$((l0_stop_write_mb/nranges_per_server))
# for dist in "uniform" "zipfian"
# do
# for workload in "workloada" "workloade" #"workloada"
# do
# run_bench
# done
# done
# done


# tuned
num_max_subcompactions="4"


# workload="workloade"
# dist="uniform"
# level="5"
# nranges_per_server="1"
# l0_start_compaction_mb=$((40*16))
# l0_stop_write_mb=$((40*16*2))
# num_memtables="128"
# run_bench

workload="workloade"
dist="zipfian"
level="5"
nranges_per_server="1"
l0_start_compaction_mb=$((40*16))
l0_stop_write_mb=$((40*16*2))
# l0_start_compaction_mb=$((60*16))
# l0_stop_write_mb=$((60*16*2))
num_memtables="128"
run_bench

# maxexecutiontime=3600
# workload="workloadw"
# dist="uniform"
# level="5"
# nranges_per_server="1"
# l0_start_compaction_mb=$((60*16))
# l0_stop_write_mb=$((60*16*2))
# num_memtables="128"
# run_bench

# workload="workloadw"
# dist="zipfian"
# level="5"
# nranges_per_server="1"
# l0_start_compaction_mb=$((40*16))
# l0_stop_write_mb=$((40*16*2))
# num_memtables="128"
# run_bench


workload="workloada"
dist="uniform"
level="5"
nranges_per_server="1"
# l0_start_compaction_mb=$((4*1024))
# l0_stop_write_mb=$((10*1024))
l0_start_compaction_mb=$((60*16))
l0_stop_write_mb=$((60*16*2))
num_memtables="128"
# run_bench

# workload="workloada"
# dist="zipfian"
# level="5"
# nranges_per_server="1"
# l0_start_compaction_mb=$((120*16))
# l0_stop_write_mb=$((60*16))
# num_memtables="128"
# run_bench




python /proj/bg-PG0/haoyu/scripts/parse_ycsb_nova_leveldb.py $nmachines $exp_results_dir > stats_leveldb_ranges_out
