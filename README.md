# NovaLSM
Nova-LSM is a component-based design of the LSM-tree using fast and high bandwidth networks such as RDMA.  Its components implement the following novel concepts.  First, they use RDMA to enable nodes of a shared-nothing architecture to share their disk bandwidth and storage.  Second, they construct ranges dynamically at runtime to parallelize compaction and boost performance. Third, they scatter blocks of a file (SSTable) across an arbitrary number of disks and use power-of-d to scale.  Fourth, the logging component separates availability of log records from their durability.  These design decisions provide for an elastic system with well-defined knobs that control its performance and scalability characteristics.  We present an implementation of these designs by extending [LevelDB](https://github.com/google/leveldb).  Our evaluation shows Nova-LSM scales and outperforms its monolithic counter-parts, both LevelDB and RocksDB, by several orders of magnitude.  This is especially true with workloads that exhibit a skewed pattern of access to data. 

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
