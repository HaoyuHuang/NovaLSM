
//
// Created by Haoyu Huang on 2/20/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//


#include "rdma_ctrl.hpp"
#include "nova_common.h"
#include "nova_config.h"
#include "nova_rdma_rc_store.h"
#include "cc/nova_cc_nic_server.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"

#include <stdlib.h>
#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <thread>
#include <assert.h>
#include <csignal>
#include <gflags/gflags.h>

#include "db/filename.h"
#include "util/env_posix.h"
#include "db/version_set.h"

using namespace std;
using namespace rdmaio;
using namespace nova;

DEFINE_string(db_path, "/tmp/nova", "level db path");
DEFINE_string(rtable_path, "/tmp/rtables", "RTable path");

DEFINE_string(cc_servers, "localhost:11211", "A list of servers");
DEFINE_int64(server_id, -1, "Server id.");
DEFINE_int64(number_of_ccs, 0, "The first n are CCs and the rest are DCs.");

DEFINE_uint64(mem_pool_size_gb, 0, "Memory pool size in GB.");
DEFINE_uint64(use_fixed_value_size, 0, "Fixed value size.");

DEFINE_uint64(rdma_port, 0, "The port used by RDMA.");
DEFINE_uint64(rdma_max_msg_size, 0, "The maximum message size used by RDMA.");
DEFINE_uint64(rdma_max_num_sends, 0,
              "The maximum number of pending RDMA sends. This includes READ/WRITE/SEND. We also post the same number of RECV events. ");
DEFINE_uint64(rdma_doorbell_batch_size, 0, "The doorbell batch size.");
DEFINE_bool(enable_rdma, false, "Enable RDMA messaging.");
DEFINE_bool(enable_load_data, false, "Enable loading data.");

DEFINE_string(cc_config_path, "/tmp/uniform-3-32-10000000-frags.txt",
              "The path that stores fragment configuration.");
DEFINE_uint64(cc_num_conn_workers, 0, "Number of connection threads.");
DEFINE_uint32(cc_num_async_workers, 0, "Number of async worker threads.");
DEFINE_uint32(cc_num_compaction_workers, 0,
              "Number of compaction worker threads.");
DEFINE_uint32(cc_num_rdma_compaction_workers, 0,
              "Number of rdma compaction worker threads.");

DEFINE_uint32(cc_num_storage_workers, 0,
              "Number of storage worker threads.");
DEFINE_uint32(cc_rtable_num_servers_scatter_data_blocks, 0,
              "Number of servers to scatter data blocks ");

DEFINE_uint64(cc_block_cache_mb, 0, "leveldb block cache size in mb");
DEFINE_uint64(cc_row_cache_mb, 0, "leveldb row cache size in mb");

DEFINE_uint32(cc_num_memtables, 0, "");
DEFINE_uint32(cc_num_memtable_partitions, 0, "");
DEFINE_bool(cc_enable_table_locator, false, "");

DEFINE_uint32(cc_l0_stop_write_gb, 0, "");

DEFINE_uint64(cc_write_buffer_size_mb, 0, "write buffer size in mb");
DEFINE_uint64(cc_sstable_size_mb, 0, "sstable size in mb");
DEFINE_uint32(cc_log_buf_size, 0, "log buffer size");
DEFINE_uint32(cc_rtable_size_mb, 0, "RTable size");
DEFINE_bool(cc_multiple_disks, false, "");
DEFINE_string(cc_scatter_policy, "random", "random/stats");
DEFINE_string(cc_log_record_mode, "none", "none/rdma");
DEFINE_uint32(cc_num_log_replicas, 0, "");
DEFINE_string(cc_memtable_type, "", "pool/static_partition");

DEFINE_bool(cc_recover_dbs, false, "recovery");
DEFINE_uint32(cc_num_recovery_threads, 32, "recovery");

DEFINE_bool(cc_enable_subrange, false, "");
DEFINE_double(cc_sampling_ratio, 1, "");
DEFINE_string(cc_zipfian_dist, "/tmp/zipfian", "");
DEFINE_string(cc_client_access_pattern, "uniform", "");
DEFINE_uint32(cc_num_tinyranges_per_subrange, 10, "");

DEFINE_bool(cc_enable_detailed_db_stats, false, "");
DEFINE_bool(cc_enable_flush_multiple_memtables, false, "");
DEFINE_uint32(cc_subrange_no_flush_num_keys, 100, "");
DEFINE_string(cc_major_compaction_type, "no", "no/st/lc/sc");
DEFINE_uint32(cc_major_compaction_max_parallism, 1, "");
DEFINE_uint32(cc_major_compaction_max_tables_in_a_set, 15, "");

NovaConfig *NovaConfig::config;
std::atomic_int_fast32_t leveldb::EnvBGThread::bg_thread_id_seq;
std::atomic_int_fast32_t nova::NovaCCServer::storage_worker_seq_id_;
std::atomic_int_fast32_t leveldb::NovaBlockCCClient::rdma_worker_seq_id_;
std::atomic_int_fast32_t nova::NovaStorageWorker::storage_file_number_seq;
std::atomic_int_fast32_t nova::NovaCCServer::compaction_storage_worker_seq_id_;
std::unordered_map<uint64_t, leveldb::FileMetaData *> leveldb::Version::last_fnfile;

void start(NovaCCNICServer *server) {
    server->Start();
}

void StartServer() {
    RdmaCtrl *rdma_ctrl = new RdmaCtrl(NovaConfig::config->my_server_id,
                                       NovaConfig::config->rdma_port);
    int port = NovaConfig::config->servers[NovaConfig::config->my_server_id].port;
    uint64_t nrdmatotal = nrdma_buf_cc();
    uint64_t ntotal = nrdmatotal;
    ntotal += NovaConfig::config->mem_pool_size_gb * 1024 * 1024 * 1024;
    RDMA_LOG(INFO) << "Allocated buffer size in bytes: " << ntotal;

    auto *buf = (char *) malloc(ntotal);
    memset(buf, 0, ntotal);
    NovaConfig::config->nova_buf = buf;
    NovaConfig::config->nnovabuf = ntotal;
    RDMA_ASSERT(buf != NULL) << "Not enough memory";

    if (!FLAGS_cc_recover_dbs) {
        system(fmt::format("exec rm -rf {}/*",
                           NovaConfig::config->db_path).data());
        system(fmt::format("exec rm -rf {}/*",
                           NovaConfig::config->rtable_path).data());
    }
    mkdirs(NovaConfig::config->rtable_path.data());
    mkdirs(NovaConfig::config->db_path.data());
    auto *mem_server = new NovaCCNICServer(rdma_ctrl, buf, port);
    mem_server->Start();
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
    std::vector<gflags::CommandLineFlagInfo> flags;
    gflags::GetAllFlags(&flags);
    for (const auto &flag : flags) {
        printf("%s=%s\n", flag.name.c_str(),
               flag.current_value.c_str());
    }

    NovaConfig::config = new NovaConfig;
    NovaConfig::config->rtable_path = FLAGS_rtable_path;

    NovaConfig::config->mem_pool_size_gb = FLAGS_mem_pool_size_gb;
    if (!FLAGS_cc_multiple_disks) {
//        if (FLAGS_server_id < FLAGS_number_of_ccs) {
//            // I'm a CC.
//            NovaConfig::config->mem_pool_size_gb = 10;
//        }
    }

    NovaConfig::config->load_default_value_size = FLAGS_use_fixed_value_size;
    // RDMA
    NovaConfig::config->rdma_port = FLAGS_rdma_port;
    NovaConfig::config->max_msg_size = FLAGS_rdma_max_msg_size;
    NovaConfig::config->rdma_max_num_sends = FLAGS_rdma_max_num_sends;
    NovaConfig::config->rdma_doorbell_batch_size = FLAGS_rdma_doorbell_batch_size;

    NovaConfig::config->block_cache_mb = FLAGS_cc_block_cache_mb;
    NovaConfig::config->row_cache_mb = FLAGS_cc_row_cache_mb;
    NovaConfig::config->write_buffer_size_mb = FLAGS_cc_write_buffer_size_mb;

    NovaConfig::config->db_path = FLAGS_db_path;
    NovaConfig::config->enable_rdma = FLAGS_enable_rdma;
    NovaConfig::config->enable_load_data = FLAGS_enable_load_data;
    NovaConfig::config->major_compaction_type = FLAGS_cc_major_compaction_type;
    NovaConfig::config->enable_flush_multiple_memtables = FLAGS_cc_enable_flush_multiple_memtables;
    NovaConfig::config->major_compaction_max_parallism = FLAGS_cc_major_compaction_max_parallism;
    NovaConfig::config->major_compaction_max_tables_in_a_set = FLAGS_cc_major_compaction_max_tables_in_a_set;

    NovaConfig::config->number_of_recovery_threads = FLAGS_cc_num_recovery_threads;
    NovaConfig::config->recover_dbs = FLAGS_cc_recover_dbs;

    NovaConfig::config->servers = convert_hosts(FLAGS_cc_servers);
    if (FLAGS_cc_multiple_disks) {
        for (int i = 0; i < NovaConfig::config->servers.size(); i++) {
            NovaConfig::config->cc_servers.push_back(
                    NovaConfig::config->servers[i]);
            if (i != FLAGS_server_id) {
                NovaConfig::config->dc_servers.push_back(
                        NovaConfig::config->servers[i]);
            }
        }
    } else {
        for (int i = 0; i < NovaConfig::config->servers.size(); i++) {
            if (i < FLAGS_number_of_ccs) {
                NovaConfig::config->cc_servers.push_back(
                        NovaConfig::config->servers[i]);
            } else {
                NovaConfig::config->dc_servers.push_back(
                        NovaConfig::config->servers[i]);
            }
        }
    }

    for (int i = 0; i < NovaConfig::config->cc_servers.size(); i++) {
        Host host = NovaConfig::config->cc_servers[i];
        RDMA_LOG(INFO)
            << fmt::format("cc: {}:{}:{}", host.server_id, host.ip, host.port);
    }
    for (int i = 0; i < NovaConfig::config->dc_servers.size(); i++) {
        Host host = NovaConfig::config->dc_servers[i];
        RDMA_LOG(INFO)
            << fmt::format("dc: {}:{}:{}", host.server_id, host.ip, host.port);
    }
    RDMA_ASSERT(FLAGS_cc_num_log_replicas <=
                NovaConfig::config->dc_servers.size());

    NovaConfig::config->my_server_id = FLAGS_server_id;

    NovaConfig::ReadFragments(FLAGS_cc_config_path,
                              &NovaConfig::config->fragments);
    uint32_t start_stoc_id = 0;
    for (int i = 0; i < NovaConfig::config->fragments.size(); i++) {
        NovaConfig::config->fragments[i]->log_replica_stoc_ids.clear();
        for (int r = 0; r < FLAGS_cc_num_log_replicas; r++) {
            NovaConfig::config->fragments[i]->log_replica_stoc_ids.push_back(
                    start_stoc_id);
            start_stoc_id = (start_stoc_id + 1) %
                            NovaConfig::config->dc_servers.size();
        }
    }

    NovaConfig::config->num_conn_workers = FLAGS_cc_num_conn_workers;
    NovaConfig::config->num_conn_async_workers = FLAGS_cc_num_async_workers;
    NovaConfig::config->num_storage_workers = FLAGS_cc_num_storage_workers;
    NovaConfig::config->num_compaction_workers = FLAGS_cc_num_compaction_workers;
    NovaConfig::config->num_rdma_compaction_workers = FLAGS_cc_num_rdma_compaction_workers;
    NovaConfig::config->num_memtables = FLAGS_cc_num_memtables;
    NovaConfig::config->num_memtable_partitions = FLAGS_cc_num_memtable_partitions;
    NovaConfig::config->enable_subrange = FLAGS_cc_enable_subrange;
    NovaConfig::config->memtable_type = FLAGS_cc_memtable_type;

    NovaConfig::config->num_rtable_num_servers_scatter_data_blocks = FLAGS_cc_rtable_num_servers_scatter_data_blocks;
    NovaConfig::config->log_buf_size = FLAGS_cc_rtable_size_mb * 1024;
    NovaConfig::config->rtable_size = FLAGS_cc_rtable_size_mb * 1024;
    NovaConfig::config->sstable_size = FLAGS_cc_sstable_size_mb * 1024 * 1024;
    NovaConfig::config->use_multiple_disks = FLAGS_cc_multiple_disks;
    NovaConfig::config->num_tinyranges_per_subrange = FLAGS_cc_num_tinyranges_per_subrange;

    if (FLAGS_cc_scatter_policy == "random") {
        NovaConfig::config->scatter_policy = ScatterPolicy::RANDOM;
    } else if (FLAGS_cc_scatter_policy == "power_of_two") {
        NovaConfig::config->scatter_policy = ScatterPolicy::POWER_OF_TWO;
    } else if (FLAGS_cc_scatter_policy == "power_of_three") {
        NovaConfig::config->scatter_policy = ScatterPolicy::POWER_OF_THREE;
    } else {
        NovaConfig::config->scatter_policy = ScatterPolicy::SCATTER_DC_STATS;
    }

    if (FLAGS_cc_log_record_mode == "none") {
        NovaConfig::config->log_record_mode = NovaLogRecordMode::LOG_NONE;
    } else if (FLAGS_cc_log_record_mode == "rdma") {
        NovaConfig::config->log_record_mode = NovaLogRecordMode::LOG_RDMA;
    }

    NovaConfig::config->enable_table_locator = FLAGS_cc_enable_table_locator;
    NovaConfig::config->subrange_sampling_ratio = FLAGS_cc_sampling_ratio;
    NovaConfig::config->zipfian_dist_file_path = FLAGS_cc_zipfian_dist;
    NovaConfig::config->ReadZipfianDist();
    NovaConfig::config->client_access_pattern = FLAGS_cc_client_access_pattern;
    NovaConfig::config->enable_detailed_db_stats = FLAGS_cc_enable_detailed_db_stats;
    NovaConfig::config->subrange_num_keys_no_flush = FLAGS_cc_subrange_no_flush_num_keys;
    NovaConfig::config->l0_stop_write_gb = FLAGS_cc_l0_stop_write_gb;

    leveldb::EnvBGThread::bg_thread_id_seq = 0;
    nova::NovaCCServer::storage_worker_seq_id_ = 0;
    leveldb::NovaBlockCCClient::rdma_worker_seq_id_ = 0;
    nova::NovaStorageWorker::storage_file_number_seq = 0;
    nova::NovaCCServer::compaction_storage_worker_seq_id_ = 0;
    NovaConfig::config->use_multiple_disks = FLAGS_cc_multiple_disks;
    StartServer();
    return 0;
}
