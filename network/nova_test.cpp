
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

DEFINE_uint64(recordcount, 1000, "Number of records.");
DEFINE_uint64(cache_size_gb, 1, " Cache size in GB.");
DEFINE_uint64(use_fixed_value_size, 10, "Fixed value size.");
DEFINE_uint64(index_size_mb, 128, "Index size in MB.");
DEFINE_uint64(nindex_entry_per_bucket, 4,
              "Number of index entries per bucket.");
DEFINE_uint64(main_bucket_mem_percent, 90,
              "The percentage of memory dedicated to main buckets.");
DEFINE_uint64(lc_index_size_mb, 16, "Location cache: Index size in MB.");
DEFINE_uint64(lc_nindex_entry_per_bucket, 4,
              "Location cache: Number of index entries per bucket.");
DEFINE_uint64(lc_main_bucket_mem_percent, 90,
              "Location cache: The percentage of memory dedicated to main buckets.");

void test_load_data(NovaMemManager *manager) {
    RDMA_LOG(INFO) << "Test load data.";
    RDMA_ASSERT(manager);
    char key[1024];
    char value[NovaConfig::config->load_default_value_size];
    // load data.
    timeval start{};
    gettimeofday(&start, nullptr);
    int loaded_keys = 0;
    for (uint64_t record_id = 0;
         record_id < NovaConfig::config->recordcount; record_id++) {
        uint32_t nkey = int_to_str(key, record_id) - 1;
        auto v = static_cast<char>((record_id % 26) + 'a');
        memset(value, v, NovaConfig::config->load_default_value_size);
        RDMA_LOG(DEBUG) << "Insert " << record_id;
        RDMA_ASSERT(manager->LocalPut(key, nkey, value,
                                      NovaConfig::config->load_default_value_size,
                                      true, false).success);
        loaded_keys++;
        if (loaded_keys % 100000 == 0) {
            timeval now{};
            gettimeofday(&now, nullptr);
            RDMA_LOG(INFO) << "Load " << loaded_keys << " entries took "
                           << now.tv_sec - start.tv_sec;
        }
    }
    RDMA_LOG(INFO) << "Completed loading data " << loaded_keys;
    // Assert the loaded data is valid.
    for (uint64_t record_id = 0;
         record_id < NovaConfig::config->recordcount; record_id++) {
        uint32_t nkey = int_to_str(key, record_id) - 1;
        auto v = static_cast<char>((record_id % 26) + 'a');
        memset(value, v, NovaConfig::config->load_default_value_size);
        GetResult result = manager->LocalGet(key, nkey);
        DataEntry it = result.data_entry;
        RDMA_ASSERT(it.stale == 0);
        RDMA_ASSERT(it.nkey == nkey) << key << " " << it.nkey;
        RDMA_ASSERT(it.nval == NovaConfig::config->load_default_value_size)
            << key;
        RDMA_ASSERT(memcmp(it.user_key(), key, nkey) == 0) << key;
        RDMA_ASSERT(
                memcmp(it.user_value(), value, it.nval) ==
                0) << key;
    }
    RDMA_LOG(INFO) << "Test load data: Success";
}

void test_location_cache(NovaMemManager *manager) {
    RDMA_LOG(INFO) << "Test location cache.";
    RDMA_ASSERT(manager);
    // load data.
    char key[1024];
    timeval start{};
    gettimeofday(&start, nullptr);
    int loaded_keys = 0;
    std::set<uint64_t> truth_map;
    for (uint64_t record_id = 0;
         record_id < NovaConfig::config->recordcount; record_id++) {
        RDMA_LOG(DEBUG) << "Insert " << record_id;
        IndexEntry index_entry;
        index_entry.type = IndexEntryType::DATA;
        index_entry.slab_class_id = 1;
        index_entry.hash = record_id;
        index_entry.data_size = NovaConfig::config->load_default_value_size;
        index_entry.data_ptr = 1111111;
        index_entry.checksum = 88888888;
        manager->RemotePut(index_entry);
        loaded_keys++;
        if (loaded_keys % 100000 == 0) {
            timeval now{};
            gettimeofday(&now, nullptr);
            RDMA_LOG(INFO) << "Load " << loaded_keys << " entries took "
                           << now.tv_sec - start.tv_sec;
        }
    }
    RDMA_LOG(INFO) << "Completed loading data to the location cache "
                   << loaded_keys;
    // Assert the loaded data is valid.
    for (uint64_t record_id = 0;
         record_id < NovaConfig::config->recordcount; record_id++) {
        uint32_t nkey = int_to_str(key, record_id) - 1;
        IndexEntry index_entry = manager->RemoteGet(key, nkey);

        RDMA_ASSERT(index_entry.type == IndexEntryType::DATA);
        RDMA_ASSERT(index_entry.slab_class_id == 1);
        RDMA_ASSERT(index_entry.hash == record_id);
        RDMA_ASSERT(index_entry.data_size ==
                    NovaConfig::config->load_default_value_size);
        RDMA_ASSERT(index_entry.data_ptr == 1111111);
        // It should recompute the checksum.
        RDMA_ASSERT(index_entry.checksum != 88888888);
    }
    RDMA_LOG(INFO) << "Test location cache: Success";
}

void test_location_cache_eviction(NovaMemManager *manager) {
    RDMA_LOG(INFO) << "Test location cache eviction.";
    RDMA_ASSERT(manager);
    // load data.
    char key[1024];
    timeval start{};
    gettimeofday(&start, nullptr);
    int loaded_keys = 0;
    std::set<uint64_t> truth_map;
    uint64_t record_id = 0;
    bool terminate = false;
    uint64_t more_keys = 0;
    RDMA_LOG(INFO) << "Index entry size: " << IndexEntry::size();
    while (!terminate || more_keys < 1000000) {
        RDMA_LOG(DEBUG) << "Insert " << record_id;
        IndexEntry index_entry;
        index_entry.type = IndexEntryType::DATA;
        index_entry.slab_class_id = 1;
        index_entry.hash = record_id;
        index_entry.data_size = NovaConfig::config->load_default_value_size;
        index_entry.data_ptr = 1111111;
        index_entry.checksum = 88888888;
        PutResult put_result = manager->RemotePut(index_entry);
        if (put_result.success) {
            truth_map.insert(record_id);
            if (!put_result.old_index_entry.empty() &&
                put_result.old_index_entry.hash != record_id) {
                terminate = true;
                RDMA_ASSERT(
                        truth_map.erase(put_result.old_index_entry.hash) == 1);
            }
        } else {
            terminate = true;
        }
        record_id++;
        if (terminate) {
            more_keys++;
        }
        loaded_keys++;
        if (loaded_keys % 100000 == 0) {
            timeval now{};
            gettimeofday(&now, nullptr);
            RDMA_LOG(INFO) << "Load " << loaded_keys << " entries took "
                           << now.tv_sec - start.tv_sec;
        }
    }
    RDMA_LOG(INFO) << "Completed loading data to the location cache "
                   << loaded_keys << ", " << truth_map.size();
    // Assert the loaded data is valid.
    for (auto const &record_id : truth_map) {
        uint32_t nkey = int_to_str(key, record_id) - 1;
        IndexEntry index_entry = manager->RemoteGet(key, nkey);

        RDMA_ASSERT(index_entry.type == IndexEntryType::DATA);
        RDMA_ASSERT(index_entry.slab_class_id == 1);
        RDMA_ASSERT(index_entry.hash == record_id);
        RDMA_ASSERT(index_entry.data_size ==
                    NovaConfig::config->load_default_value_size);
        RDMA_ASSERT(index_entry.data_ptr == 1111111);
    }
    RDMA_LOG(INFO) << "Test location cache eviction: Success";
}

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    // data
    NovaConfig::config = new NovaConfig();
    NovaConfig::config->recordcount = FLAGS_recordcount;
    NovaConfig::config->cache_size_gb = FLAGS_cache_size_gb;
    NovaConfig::config->load_default_value_size = FLAGS_use_fixed_value_size;

    // Index.
    NovaConfig::config->index_size_mb = FLAGS_index_size_mb;
    NovaConfig::config->nindex_entry_per_bucket = FLAGS_nindex_entry_per_bucket;
    NovaConfig::config->main_bucket_mem_percent = FLAGS_main_bucket_mem_percent;
    NovaConfig::config->ComputeNumberOfBuckets();

    // Location cache.
    NovaConfig::config->lc_size_mb = FLAGS_lc_index_size_mb;
    NovaConfig::config->lc_nindex_entry_per_bucket = FLAGS_lc_nindex_entry_per_bucket;
    NovaConfig::config->lc_main_bucket_mem_percent = FLAGS_main_bucket_mem_percent;

    uint64_t ntotal = 0;
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
    auto *manager = new NovaMemManager(buf);
    test_location_cache(manager);
    test_location_cache_eviction(manager);
    test_load_data(manager);
    return 0;
}
