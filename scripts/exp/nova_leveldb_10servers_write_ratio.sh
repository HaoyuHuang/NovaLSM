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
exp_results_dir="$home_dir/large-wr-nova-leveldb-10servers-ranges-$recordcount"
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
ncompaction_workers="32"
cache_size_gb="32"
value_size="4096"
partition="range"
lc_index_size_mb="10"
index_size_mb="10"
write_buffer_size_mb="4"
nreplicas_per_range="1"
nranges_per_server="1"
persist_log_record="local"
log_buf_size="1"
zipfianconstant="0.99"

port=$((10000+RANDOM%1000))
rdma_port=$((20000+RANDOM%1000))
rdma_max_msg_size="8192"
rdma_max_num_reads="256"
rdma_max_num_sends="128"
rdma_doorbell_batch_size="8"
rdma_pq_batch_size="16"
shed_load="90"
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
		# if [[ $i == "8" ]]; then
		# 	i=$((i+1))
		# 	continue	
		# fi
		servers+=("node-$i")
		i=$((i+1))
		n=$((n+1))
	done
	n=0
	i=$((nmachines-1))
	while [ $n -lt $nclients ]
	do
		# if [[ $i == "8" ]]; then
		# 	i=$((i-1))
		# 	continue	
		# fi
		clis+=("node-$i")
		i=$((i-1))
		n=$((n+1))
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

	result_dir_name="nova-d-$dist-w-$workload-ltc-$nservers-log-$persist_log_record-l0-$l0_stop_write_mb-np-$nranges_per_server-ss-$sstable_size_mb"
	echo "running experiment $result_dir_name"

	# Copy the files over local node
    dir="$exp_results_dir/$result_dir_name"
    echo "Save to $dir..."
    sudo rm -rf $dir
    sudo mkdir -p $dir
    sudo chmod -R 777 $dir

	java -jar $cache_bin_dir/nova_config_generator.jar $config_dir "shared" $recordcount $nservers $nreplicas_per_range $nranges_per_server
	config_path="$config_dir/nova-shared-cc-nrecords-$recordcount-nccservers-$nservers-nlogreplicas-$nreplicas_per_range-nranges-$nranges_per_server"

	# config_path="$config_dir/nova-nrecords-$recordcount-nservers-$nservers-nreplicas-$nreplicas_per_range-nranges-$nranges_per_server"
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
		ssh -oStrictHostKeyChecking=no $m "sudo killall leveldb_main nova_server_main nova_shared_main nova_multi_thread_compaction java collectl sar"
		ssh -oStrictHostKeyChecking=no $m "sudo collectl -scx -i 1 -P > $results/$m-coll.txt &"
		ssh -oStrictHostKeyChecking=no $m "sar -P ALL 1 > $results/$m-cpu.txt &"
	    ssh -oStrictHostKeyChecking=no $m "sar -n DEV 1 > $results/$m-net.txt &"
	    ssh -oStrictHostKeyChecking=no $m "sar -r 1 > $results/$m-mem.txt &"
	    ssh -oStrictHostKeyChecking=no $m "sar -d 1 > $results/$m-disk.txt &"
	done

	for m in ${servers[@]}
	do
		while ssh -oStrictHostKeyChecking=no $m "ps -ef | grep -v grep | grep -v ssh | grep -v bash | grep -c leveldb_main"
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
		cmd="stdbuf --output=0 --error=0 ./leveldb_main --level=$level --l0_start_compaction_mb=$l0_start_compaction_mb --l0_stop_write_mb=$l0_stop_write_mb --sstable_mode=$sstable_mode --block_cache_mb=$block_cache_mb --db_path=$db_path --write_buffer_size_mb=$write_buffer_size_mb --persist_log_records_mode=$persist_log_record --log_buf_size=$log_buf_size --servers=$nova_servers --server_id=$server_id --recordcount=$recordcount --data_partition_alg=$partition --num_conn_workers=$nconn_workers --num_async_workers=$nasync_workers --num_compaction_workers=$ncompaction_workers --cache_size_gb=$cache_size_gb --use_fixed_value_size=$value_size --rdma_port=$nova_rdma_port --rdma_max_msg_size=8192 --rdma_max_num_sends=128 --rdma_doorbell_batch_size=8 --rdma_pq_batch_size=8 --enable_rdma=$enable_rdma --config_path=$config_path --enable_load_data=true --profiler_file_path=$profiler_file_path --sstable_size_mb=$sstable_size_mb"
		echo "$cmd"
		ssh -oStrictHostKeyChecking=no $s "rm -rf $db_path && mkdir -p $db_path && cd $cache_bin_dir && $cmd >& $results/server-$s-out &" &
		server_id=$((server_id+1))
		nova_rdma_port=$((nova_rdma_port+1))
		sleep 1
	done

	sleep 120
	cli_nrecords=$((recordcount))
	for c in ${clis[@]}
	do
		for i in $(seq 1 $nclients_per_server);
		do
			echo "creating client on $c-$i"
			cmd="stdbuf --output=0 --error=0 bash $script_dir/run_ycsb.sh $nthreads $nova_servers $debug $partition $cli_nrecords $maxexecutiontime $dist $value_size $workload $config_path $cardinality $operationcount $zipfianconstant 0"
			echo "$cmd"
			ssh -oStrictHostKeyChecking=no $c "cd $client_bin_dir && $cmd >& $results/client-$c-$i-out &" &
		done
	done
	
	port=$((port+1))
	rdma_port=$((rdma_port+1))
	sleep 10
	sleep_time=0
	stop="false"
	max_wait_time=$((maxexecutiontime+2000))
	for m in ${clis[@]}
	do
		while ssh -oStrictHostKeyChecking=no $m "ps -ef | grep -v \"grep --color=auto ycsb\" | grep -v ssh | grep -v bash | grep ycsb | grep -c java"
		do
			sleep 10
			sleep_time=$((sleep_time+10))
			echo "waiting for $m for $sleep_time seconds"
		done
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
ncompaction_workers="128"

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

nclients="2"
nthreads="512"
dist="zipfian"
nreplicas_per_range="1"
enable_rdma="false"
write_buffer_size_mb="16"
sstable_size_mb="$write_buffer_size_mb"
nconn_workers="512"
nclients_per_server="5"
persist_log_record="none"
nservers="10"
nmachines="23"
nclients="12"
level="6"

for persist_log_record in "none" #"disk" "mem"
do
for nranges_per_server in "64" #"4" "16" "1"
do
ncompaction_workers="$nranges_per_server"
l0_start_compaction_mb="4096"
l0_stop_write_mb=$((10*1024))
l0_start_compaction_mb=$((l0_start_compaction_mb/nranges_per_server))
l0_stop_write_mb=$((l0_stop_write_mb/nranges_per_server))

for dist in "zipfian" #""
do
for workload in "workloada80" "workloada90" #"workloada60" "workloada70"   #"workloadw"
do
run_bench
done
done
done
done

python /proj/bg-PG0/haoyu/scripts/parse_ycsb_nova_leveldb.py 25 $exp_results_dir > stats_wr_leveldb_10servers_ranges_out_$recordcount
