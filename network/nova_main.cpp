
//
// Created by Haoyu Huang on 2/20/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//


#include "rdma_ctrl.hpp"
#include "nova_common.h"
#include "nova_mem_config.h"
#include "nova_rdma_rc_store.h"
#include "nova_mem_server.h"

#include <stdio.h>
#include <string.h>
#include <thread>
#include <assert.h>
#include <csignal>
#include <gflags/gflags.h>

using namespace std;
using namespace rdmaio;

NovaConfig *NovaConfig::config;
RdmaCtrl *NovaConfig::rdma_ctrl;

DEFINE_string(servers, "localhost:11211", "A list of peer servers");
DEFINE_int64(server_id, -1, "Server id.");
DEFINE_uint64(recordcount, 0, "Number of records.");
DEFINE_string(data_partition_alg, "hash",
              "Data partition algorithm: hash, range, debug.");

DEFINE_uint64(num_mem_workers, 0, "Number of worker threads.");
DEFINE_uint64(cache_size_gb, 0, " Cache size in GB.");
DEFINE_uint64(use_fixed_value_size, 0, "Fixed value size.");
DEFINE_uint64(index_size_mb, 0, "Index size in MB.");
DEFINE_uint64(nindex_entry_per_bucket, 0,
              "Number of index entries per bucket.");
DEFINE_uint64(main_bucket_mem_percent, 0,
              "The percentage of memory dedicated to main buckets.");
DEFINE_uint64(lc_index_size_mb, 0, "Location cache: Index size in MB.");
DEFINE_uint64(lc_nindex_entry_per_bucket, 0,
              "Location cache: Number of index entries per bucket.");
DEFINE_uint64(lc_main_bucket_mem_percent, 0,
              "Location cache: The percentage of memory dedicated to main buckets.");
DEFINE_uint64(rdma_port, 0, "The port used by RDMA.");
DEFINE_uint64(rdma_max_msg_size, 0, "The maximum message size used by RDMA.");
DEFINE_uint64(rdma_max_num_reads, 0,
              "The maximum number of pending RDMA reads.");
DEFINE_uint64(rdma_max_num_sends, 0,
              "The maximum number of pending RDMA sends.");
DEFINE_uint64(rdma_doorbell_batch_size, 0, "The doorbell batch size.");
DEFINE_uint64(rdma_pq_batch_size, 0,
              "The number of pending requests a worker thread waits before polling RNIC.");
DEFINE_string(rdma_mode, "none", "Server mode: none, server_redirect, proxy.");
DEFINE_uint64(shed_load, 0,
              "The percentage of read requests shed to other servers.");
DEFINE_bool(enable_rdma, false, "Enable RDMA messaging.");
DEFINE_bool(enable_load_data, false, "Enable loading data.");
DEFINE_uint64(rdma_number_of_get_retries, 3, "Number of RDMA retries for get.");
DEFINE_string(config_path, "/tmp/uniform-3-32-10000000-frags.txt",
              "The path that stores fragment configuration.");

void start(NovaMemServer *server) {
    server->Start();
}

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    int i;
    const char **methods = event_get_supported_methods();
    printf("Starting Libevent %s.  Available methods are:\n",
           event_get_version());
    for (i = 0; methods[i] != NULL; ++i) {
        printf("    %s\n", methods[i]);
    }
    if (FLAGS_server_id == -1) {
        exit(0);
    }
    // data
    NovaConfig::config = new NovaConfig();
    NovaConfig::config->my_server_id = FLAGS_server_id;
    std::string servers = FLAGS_servers;
    printf("Servers %s\n", servers.c_str());
    NovaConfig::config->servers = convert_hosts(servers);
    NovaConfig::config->num_mem_workers = FLAGS_num_mem_workers;
    NovaConfig::config->recordcount = FLAGS_recordcount;
    NovaConfig::config->cache_size_gb = FLAGS_cache_size_gb;
    NovaConfig::config->load_default_value_size = FLAGS_use_fixed_value_size;
    string data_partition = FLAGS_data_partition_alg;

    // Index.
    NovaConfig::config->index_size_mb = FLAGS_index_size_mb;
    NovaConfig::config->nindex_entry_per_bucket = FLAGS_nindex_entry_per_bucket;
    NovaConfig::config->main_bucket_mem_percent = FLAGS_main_bucket_mem_percent;
    NovaConfig::config->ComputeNumberOfBuckets();

    // Location cache.
    NovaConfig::config->lc_size_mb = FLAGS_lc_index_size_mb;
    NovaConfig::config->lc_nindex_entry_per_bucket = FLAGS_lc_nindex_entry_per_bucket;
    NovaConfig::config->lc_main_bucket_mem_percent = FLAGS_main_bucket_mem_percent;

    // RDMA
    NovaConfig::config->rdma_port = FLAGS_rdma_port;
    NovaConfig::config->max_msg_size = FLAGS_rdma_max_msg_size;
    NovaConfig::config->rdma_max_num_reads = FLAGS_rdma_max_num_reads;
    NovaConfig::config->rdma_max_num_sends = FLAGS_rdma_max_num_sends;
    NovaConfig::config->rdma_doorbell_batch_size = FLAGS_rdma_doorbell_batch_size;
    NovaConfig::config->rdma_pq_batch_size = FLAGS_rdma_pq_batch_size;
    NovaConfig::config->rdma_number_of_get_retries = FLAGS_rdma_number_of_get_retries;
    string rdma_mode = FLAGS_rdma_mode;

    // sheding load.
    int shed_loads = FLAGS_shed_load;
    NovaConfig::config->enable_rdma = FLAGS_enable_rdma;
    NovaConfig::config->enable_load_data = FLAGS_enable_load_data;
    std::string path = FLAGS_config_path;
    printf("config path=%s\n", path.c_str());

    NovaConfig::config->shed_load = (double *) malloc(
            NovaConfig::config->servers.size() * sizeof(double));
    NovaConfig::config->shed_load[0] = 100 - shed_loads;
    uint64_t load_per_server = 100;
    if (NovaConfig::config->servers.size() > 1) {
        load_per_server = shed_loads / (NovaConfig::config->servers.size() - 1);
    }
    for (size_t i = 1; i < NovaConfig::config->servers.size(); i++) {
        NovaConfig::config->shed_load[i] = load_per_server;
        NovaConfig::config->shed_load[i] += NovaConfig::config->shed_load[i -
                                                                          1];
    }
    NovaConfig::config->shed_load[NovaConfig::config->servers.size() - 1] = 100;

    if (rdma_mode.find("none") != string::npos) {
        NovaConfig::config->mode = NovaRDMAMode::NORMAL;
    } else if (rdma_mode.find("server_redirect") != string::npos) {
        NovaConfig::config->mode = NovaRDMAMode::SERVER_REDIRECT;
    } else {
        NovaConfig::config->mode = NovaRDMAMode::PROXY;
    }
    if (data_partition.find("hash") != string::npos) {
        NovaConfig::config->partition_mode = NovaRDMAPartitionMode::HASH;
    } else if (data_partition.find("range") != string::npos) {
        NovaConfig::config->partition_mode = NovaRDMAPartitionMode::RANGE;
    } else {
        NovaConfig::config->partition_mode = NovaRDMAPartitionMode::DEBUG_RDMA;
    }
    RDMA_LOG(INFO) << NovaConfig::config->to_string();
    NovaConfig::config->ReadFragments(path);
    NovaConfig::rdma_ctrl = new RdmaCtrl(NovaConfig::config->my_server_id,
                                         NovaConfig::config->rdma_port);
    int port = NovaConfig::config->servers[NovaConfig::config->my_server_id].port;
    uint64_t nrdmatotal = (NovaConfig::config->rdma_max_num_sends * 2 +
                           NovaConfig::config->rdma_max_num_reads) *
                          NovaConfig::config->max_msg_size *
                          NovaConfig::config->servers.size() *
                          NovaConfig::config->num_mem_workers;
    uint64_t ntotal = nrdmatotal;
    NovaConfig::config->index_buf_offset = ntotal;
    ntotal += NovaConfig::config->index_size_mb * 1024 * 1024;
    NovaConfig::config->lc_buf_offset = ntotal;
    ntotal += NovaConfig::config->lc_size_mb * 1024 * 1024;
    NovaConfig::config->data_buf_offset = ntotal;
    ntotal += NovaConfig::config->cache_size_gb * 1024 * 1024 * 1024;
    auto *buf = (char *) malloc(ntotal);
    memset(buf, 0, ntotal);

    RDMA_LOG(INFO) << "Allocated buffer size in bytes: " << ntotal;
    NovaConfig::config->nova_buf = buf;
    NovaConfig::config->nnovabuf = ntotal;
    RDMA_ASSERT(buf != NULL) << "Not enough memory";
    auto *mem_server = new NovaMemServer(buf, port);
    mem_server->Start();

//    int cores[] = {8, 9, 10, 11, 12, 13, 14, 15, 24, 25, 26, 27, 28, 29, 30, 31};
//    for (int i = 0; i < threads.size(); i++) {
//        // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
//        // only CPU i as set.
//        cpu_set_t cpuset;
//        CPU_ZERO(&cpuset);
//        CPU_SET(i, &cpuset);
//        int rc = pthread_setaffinity_np(threads[i].native_handle(),
//                                        sizeof(cpu_set_t), &cpuset);
//        RDMA_ASSERT(rc == 0) << rc;
//    }
    return 0;
}
