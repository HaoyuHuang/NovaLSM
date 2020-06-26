# NovaLSM
Nova-LSM is a component-based design of the LSM-tree using fast and high bandwidth networks such as RDMA. These components implement the following novel concepts.  First, they use RDMA to enable nodes of a shared-nothing architecture to share their disk bandwidth and space. Second, they construct ranges dynamically at runtime to boost performance and parallelize compaction. Third, they scatter blocks of a file (SSTable) across an arbitrary number of disks to scale. Fourth, the logging component separates availability of log records from their durability.  These designs provide for alternative implementations of a component with different configuration settings.  NovaLSM is built on top of [LevelDB](https://github.com/google/leveldb). 

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

# Simulator to evaluate subranges
https://github.com/HaoyuHuang/NovaLSMSim

# Setup on Cloudlab
1. Setup SSH between cloudlab nodes. 
```
bash scripts/bootstrap/env/setup-apt-ssh.sh $number_of_nodes
```
2. Clone the YCSB client binding repo in your cloudlab node. 
3. SSH to node-0 on cloudlab to install everything required for experiments. This takes around 15-20 minutes. 
```
bash scripts/bootstrap/env/init.sh $number_of_nodes
```
4. Scripts to reproduce numbers shown in the paper.
```
bash scripts/exp/nova_lsm_exps.sh
```
```
bash scripts/exp/nova_leveldb_comparison.sh
```
