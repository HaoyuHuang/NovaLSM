
//
// Created by Haoyu Huang on 2/20/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//


#include "rdma_ctrl.hpp"
#include "nova_common.h"
#include "nova_mem_config.h"
#include "nova_rdma_rc_store.h"
#include "nova_mem_server.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include "leveldb/comparator.h"

#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <thread>
#include <assert.h>
#include <csignal>
#include <gflags/gflags.h>
#include <db/filename.h>

using namespace std;
using namespace rdmaio;
using namespace nova;

NovaConfig *NovaConfig::config;
RdmaCtrl *NovaConfig::rdma_ctrl;

DEFINE_string(db_path, "/tmp/nova", "level db path");
DEFINE_uint64(block_cache_mb, 0, "leveldb block cache size in mb");
DEFINE_bool(write_sync, false, "fsync write");
DEFINE_string(persist_log_records_mode, "", "local/rdma/nic");
DEFINE_uint64(write_buffer_size_mb, 0, "write buffer size in mb");
DEFINE_uint32(log_buf_size, 0, "log buffer size");

DEFINE_string(profiler_file_path, "", "profiler file path.");
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
DEFINE_uint64(rdma_max_num_sends, 0,
              "The maximum number of pending RDMA sends. This includes READ/WRITE/SEND. We also post the same number of RECV events. ");
DEFINE_uint64(rdma_doorbell_batch_size, 0, "The doorbell batch size.");
DEFINE_uint64(rdma_pq_batch_size, 0,
              "The number of pending requests a worker thread waits before polling RNIC.");
DEFINE_string(rdma_mode, "none", "Server mode: none, server_redirect, proxy.");
DEFINE_bool(enable_rdma, false, "Enable RDMA messaging.");
DEFINE_bool(enable_load_data, false, "Enable loading data.");
DEFINE_uint64(rdma_number_of_get_retries, 3, "Number of RDMA retries for get.");
DEFINE_string(config_path, "/tmp/uniform-3-32-10000000-frags.txt",
              "The path that stores fragment configuration.");

namespace {
    class YCSBKeyComparator : public leveldb::Comparator {
    public:
        //   if a < b: negative result
        //   if a > b: positive result
        //   else: zero result
        int Compare(const leveldb::Slice &a, const leveldb::Slice &b) const {
            uint64_t ai = 0;
            str_to_int(a.data(), &ai, a.size());
            uint64_t bi = 0;
            str_to_int(b.data(), &bi, b.size());

            if (ai < bi) {
                return -1;
            } else if (ai > bi) {
                return 1;
            }
            return 0;
            return 0;
        }

        // Ignore the following methods for now:
        const char *Name() const { return "YCSBKeyComparator"; }

        void
        FindShortestSeparator(std::string *, const leveldb::Slice &) const {}

        void FindShortSuccessor(std::string *) const {}
    };
}

void start(NovaMemServer *server) {
    server->Start();
}

leveldb::DB *CreateDatabase(int db_index, leveldb::Cache *cache) {
    leveldb::DB *db;
    leveldb::Options options;
    options.block_cache = cache;
    if (FLAGS_write_buffer_size_mb > 0) {
        options.write_buffer_size = FLAGS_write_buffer_size_mb * 1024 * 1024;
    }
    options.create_if_missing = true;
    options.compression = leveldb::kNoCompression;
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    if (NovaConfig::config->profiler_file_path.empty()) {
        options.enable_tracing = false;
    } else {
        options.enable_tracing = true;
        options.trace_file_path = NovaConfig::config->profiler_file_path;
    }
    options.comparator = new YCSBKeyComparator();
    std::string db_path = DBName(NovaConfig::config->db_path, db_index);

    mkdir(db_path.c_str(), 0777);

    leveldb::Status status = leveldb::DB::Open(options, db_path, &db);
    RDMA_ASSERT(status.ok()) << "Open leveldb failed " << status.ToString();

    uint64_t index = 0;
    std::string logname = leveldb::LogFileName(db_path, 1111);
    ParseDBName(logname, &index);
    RDMA_ASSERT(index == db_index);
    return db;
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
    NovaConfig::config->rdma_max_num_sends = FLAGS_rdma_max_num_sends;
    NovaConfig::config->rdma_doorbell_batch_size = FLAGS_rdma_doorbell_batch_size;
    NovaConfig::config->rdma_pq_batch_size = FLAGS_rdma_pq_batch_size;
    NovaConfig::config->rdma_number_of_get_retries = FLAGS_rdma_number_of_get_retries;

    // LevelDB
    NovaConfig::config->db_path = FLAGS_db_path;
    NovaConfig::config->profiler_file_path = FLAGS_profiler_file_path;
    NovaConfig::config->fsync = FLAGS_write_sync;
    NovaConfig::config->log_buf_size = FLAGS_log_buf_size;
    if (FLAGS_persist_log_records_mode == "local") {
        NovaConfig::config->log_record_mode = NovaLogRecordMode::LOG_LOCAL;
    } else if (FLAGS_persist_log_records_mode == "rdma") {
        NovaConfig::config->log_record_mode = NovaLogRecordMode::LOG_RDMA;
    } else if (FLAGS_persist_log_records_mode == "nic") {
        NovaConfig::config->log_record_mode = NovaLogRecordMode::LOG_NIC;
    }

    string rdma_mode = FLAGS_rdma_mode;

    // sheding load.
    NovaConfig::config->enable_rdma = FLAGS_enable_rdma;
    NovaConfig::config->enable_load_data = FLAGS_enable_load_data;
    std::string path = FLAGS_config_path;
    printf("config path=%s\n", path.c_str());

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
    uint64_t nrdmatotal = (NovaConfig::config->rdma_max_num_sends * 2) *
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

    leveldb::Cache *cache = nullptr;
    if (FLAGS_block_cache_mb > 0) {
        cache = leveldb::NewLRUCache(
                FLAGS_block_cache_mb * 1024 * 1024);
    }
    int ndbs = NovaConfig::config->ParseNumberOfDatabases(
            NovaConfig::config->my_server_id);
    std::vector<leveldb::DB *> dbs;
    for (int db_index = 0; db_index < ndbs; db_index++) {
        dbs.push_back(CreateDatabase(db_index, cache));
    }
    auto *mem_server = new NovaMemServer(dbs, buf, port);
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
