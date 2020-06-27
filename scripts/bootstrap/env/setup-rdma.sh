#!/bin/bash

export DEBIAN_FRONTEND=noninteractive

sudo apt-get --yes install libibcm1 libibverbs1 ibverbs-utils librdmacm1 rdmacm-utils libdapl2 ibutils libibumad3 libmlx4-1 libmthca1 infiniband-diags  mstflint  perftest librdmacm-dev libmlx4-dev libibverbs-dev libevent-dev libibumad-dev
sudo apt-get -yq install collectl


# RDMA stack modules
sudo modprobe rdma_cm
sudo modprobe ib_uverbs
sudo modprobe rdma_ucm
sudo modprobe ib_ucm
sudo modprobe ib_umad
sudo modprobe ib_ipoib
# RDMA devices low-level drivers
sudo modprobe mlx4_ib
sudo modprobe mlx4_en
sudo modprobe iw_cxgb3
sudo modprobe iw_cxgb4
sudo modprobe iw_nes
sudo modprobe iw_c2
sudo modprobe msr

sudo mlnx_tune -p HIGH_THROUGHPUT

# sudo ip link set dev ib0 up