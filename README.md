# NovaLSM
The cloud infrastructure motivates disaggregation of monolithic data stores into components that are assembled together based on an application's workload. This study investigates disaggregation of an LSM-tree key-value store into components that communicate using RDMA. These components separate storage from processing, enabling processing components to share storage bandwidth and space. The processing components scatter blocks of a file (SSTable) across an arbitrary number of storage components and balance load across them using power-of-d. They construct ranges dynamically at runtime to parallelize compaction and enhance performance. Each component has configuration knobs that control its scalability. The resulting component-based system, Nova-LSM, is elastic. It outperforms its monolithic counterparts, both LevelDB and RocksDB, by several orders of magnitude with workloads that exhibit a skewed pattern of access to data. 

[[arXiv paper]](https://arxiv.org/abs/2104.01305)

# Platform
Linux. We have tested it on [CloudLab R320 and R6220](https://docs.cloudlab.us/hardware.html) instances. 

# Dependencies
NovaLSM requires RDMA packages, `gflags`, and `fmt`. You may install all of its required dependencies using
```
bash scripts/bootstrap/env/install-deps.sh
```

# Build
```
cmake .
make -j4
```

# YCSB client binding
https://github.com/HaoyuHuang/NovaLSM-YCSB-Client

# Simulator to evaluate dynamic ranges
https://github.com/HaoyuHuang/NovaLSMSim

# Setup on Cloudlab
Cloudlab profile: https://github.com/HaoyuHuang/NovaLSM/blob/master/scripts/bootstrap/cloud_lab_profile.py

1. Setup SSH between cloudlab nodes. 
```
bash scripts/bootstrap/env/setup-apt-ssh.sh $number_of_nodes
```
2. Clone the YCSB client binding repo in your cloudlab node. 
3. SSH to node-0 on cloudlab to install everything required for experiments. This takes around 15-20 minutes. 
```
bash scripts/bootstrap/env/init.sh $number_of_nodes
```

# Experiments
We conducted all of our experiments using cloudlab r6220 nodes. The experiment scripts are under "scripts/exp". You need to modify the directory in these scripts to point to your directory that stores the server binaries. 
