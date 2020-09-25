
//
// Created by Haoyu Huang on 2/29/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <gflags/gflags.h>

#include "benchmarks/bench_common.h"
#include "leveldb/comparator.h"

#include <stdlib.h>
#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <thread>
#include <assert.h>
#include <csignal>
#include <fmt/core.h>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "ltc/storage_selector.h"
#include "memtable_worker.h"

using namespace std;
using namespace rdmaio;
using namespace nova;

DEFINE_uint64(num_workers, 0, "Number of connection threads.");
DEFINE_uint32(nkeys, 0, "");
DEFINE_uint32(value_size, 0, "");
DEFINE_uint64(memtable_size_mb, 0, "");
DEFINE_uint32(npartitions, 0, "");
DEFINE_uint64(max_ops, 0, "");

NovaConfig *NovaConfig::config;
NovaGlobalVariables NovaGlobalVariables::global;
std::atomic<nova::Servers *> leveldb::StorageSelector::available_stoc_servers;

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    int i;
    const char **methods = event_get_supported_methods();
    printf("Starting Libevent %s.  Available methods are:\n",
           event_get_version());
    for (i = 0; methods[i] != NULL; ++i) {
        printf("    %s\n", methods[i]);
    }
    std::vector<gflags::CommandLineFlagInfo> flags;
    gflags::GetAllFlags(&flags);
    for (const auto &flag : flags) {
        printf("%s=%s\n", flag.name.c_str(),
               flag.current_value.c_str());
    }

    uint64_t memtable_size = FLAGS_memtable_size_mb * 1024 * 1024;
    leveldb::PartitionedMemTableBench *memtable = new leveldb::PartitionedMemTableBench(
            FLAGS_npartitions, memtable_size);

    std::vector<std::thread> worker_threads;
    std::vector<leveldb::MemTableWorker *> workers;

    for (int i = 0; i < FLAGS_num_workers; i++) {
        leveldb::MemTableWorker *worker = new leveldb::MemTableWorker(i,
                                                                      memtable,
                                                                      FLAGS_max_ops,
                                                                      FLAGS_nkeys,
                                                                      FLAGS_value_size,
                                                                      memtable_size);
        workers.push_back(worker);
        worker_threads.emplace_back(&leveldb::MemTableWorker::Start, worker);
    }

    for (int i = 0; i < FLAGS_num_workers; i++) {
        worker_threads[i].join();
    }
    double thpt = 0;
    for (int i = 0; i < FLAGS_num_workers; i++) {
        thpt += workers[i]->throughput_;
    }

    NOVA_LOG(INFO) << fmt::format("throughput,{}", thpt);
    return 0;
}
