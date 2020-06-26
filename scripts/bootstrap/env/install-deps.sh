#!/bin/bash
export DEBIAN_FRONTEND=noninteractive

sudo apt-get update
bash scripts/bootstrap/env/setup-rdma.sh

sudo apt-get --yes install libgflags-dev

cd /tmp/
wget https://github.com/fmtlib/fmt/releases/download/6.1.2/fmt-6.1.2.zip
unzip fmt-6.1.2.zip
cd fmt-6.1.2/ && cmake . && make -j32 && sudo make install
