
//
// Created by Haoyu Huang on 2/12/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <gflags/gflags.h>

#include "bench_common.h"
#include "rdma/rdma_ctrl.hpp"
#include "rdma/nova_rdma_rc_broker.h"
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
#include <db/filename.h>
#include <util/env_posix.h>
#include <fmt/core.h>

#include "rdma_write_client.h"
#include "rdma_write_server_worker.h"

using namespace std;
using namespace rdmaio;
using namespace nova;

DEFINE_string(table_path, "/tmp/rtables", "RTable path");

DEFINE_string(servers, "localhost:11211", "A list of ltc servers");
DEFINE_int64(server_id, -1, "Server id.");
DEFINE_uint64(mem_pool_size_gb, 0, "Memory pool size in GB.");

DEFINE_uint64(rdma_port, 0, "The port used by RDMA.");
DEFINE_uint64(rdma_max_msg_size, 0, "The maximum message size used by RDMA.");
DEFINE_uint64(rdma_max_num_sends, 0,
              "The maximum number of pending RDMA sends. This includes READ/WRITE/SEND. We also post the same number of RECV events. ");
DEFINE_uint64(rdma_doorbell_batch_size, 0, "The doorbell batch size.");
DEFINE_uint64(rdma_pq_batch_size, 0,
              "The number of pending requests a worker thread waits before polling RNIC.");

DEFINE_uint64(num_write_workers, 0, "Number of connection threads.");
DEFINE_uint32(num_persist_workers, 0, "Number of async worker threads.");

DEFINE_bool(is_local_disk_bench, false, "");
DEFINE_bool(disk_horizontal_scalability, false, "");

DEFINE_uint32(write_size_kb, 0, "");
DEFINE_uint32(rtable_size_mb, 0, "");
DEFINE_uint32(max_num_rtables, 0, "");

DEFINE_uint32(max_run_time, 0, "");

std::vector<nova::Host> servers;

namespace {
    uint64_t bench_nrdma_buf_unit() {
        return (FLAGS_rdma_max_num_sends * 2) *
               FLAGS_rdma_max_msg_size;
    }

    uint64_t bench_nrdma_buf() {
        // A CC async/bg thread connects to one thread at each DC.
        uint64_t nrdmatotal = bench_nrdma_buf_unit() *
                              (FLAGS_num_write_workers) * servers.size();
        return nrdmatotal;
    }
}

nova::NovaGlobalVariables nova::NovaGlobalVariables::global;

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

    RdmaCtrl *rdma_ctrl = new RdmaCtrl(FLAGS_server_id,
                                       FLAGS_rdma_port);
    servers = convert_hosts(FLAGS_servers);

    int port = servers[FLAGS_server_id].port;
    uint64_t nrdmatotal = bench_nrdma_buf();
    uint64_t ntotal = nrdmatotal;
    ntotal += FLAGS_mem_pool_size_gb * 1024 * 1024 * 1024;
    NOVA_LOG(INFO) << "Allocated buffer size in bytes: " << ntotal;

    auto *rdma_buf = (char *) malloc(ntotal);
    memset(rdma_buf, 0, ntotal);
    NOVA_ASSERT(rdma_buf != NULL) << "Not enough memory";
    int ret = system(fmt::format("exec rm -rf {}/*", FLAGS_table_path).data());

    mkdirs(FLAGS_table_path.data());

    char *cache_buf = rdma_buf + bench_nrdma_buf();
    NovaMemManager *mem_manager = new NovaMemManager(cache_buf, 1,
                                                     FLAGS_mem_pool_size_gb,
                                                     1024);

    // server.
    std::vector<RDMAWRITEServerWorker *> server_workers;
    std::vector<RDMAWRITEDiskWorker *> disk_workers;
    std::vector<std::thread> server_threads;
    std::vector<std::thread> disk_threads;

    leveldb::PosixEnv *env = new leveldb::PosixEnv;
    leveldb::EnvOptions env_option;
    env_option.sstable_mode = leveldb::NovaSSTableMode::SSTABLE_DISK;
    env->set_env_option(env_option);

    for (int worker_id = 0;
         worker_id < FLAGS_num_persist_workers; worker_id++) {
//            const std::string &table_path,
//            uint32_t write_size_kb,
//            uint32_t rtable_size,
//            uint32_t max_num_rtables,
//            uint32_t num_persist_workers

        RDMAWRITEDiskWorker *disk = new RDMAWRITEDiskWorker(
                FLAGS_table_path, FLAGS_write_size_kb,
                FLAGS_rtable_size_mb * 1024 * 1024, FLAGS_max_num_rtables);
        disk->env_ = env;
        disk->worker_id_ = worker_id;
        disk->mem_manager_ = mem_manager;
        disk_workers.push_back(disk);
    }

    uint32_t range = UINT32_MAX / servers.size();
    uint32_t lower_req_id = range * FLAGS_server_id;
    uint32_t upper_req_id = lower_req_id + range;

    if (lower_req_id == 0) {
        lower_req_id = 1;
    }

    uint32_t scid = mem_manager->slabclassid(0,
                                             16 * 1024 * 1024);
    char *buf = mem_manager->ItemAlloc(0, scid);
    NOVA_ASSERT(buf);

    mkdirs(FLAGS_table_path.c_str());
    MockRTable *rtable = new MockRTable(env, FLAGS_table_path, 16 * 1024 * 1024,
                                        100);
    for (int i = 0; i < 100; i++) {
        rtable->Persist(buf, 16 * 1024 * 1024);
    }

    char *rdma_worker_buf = rdma_buf;
    for (int worker_id = 0;
         worker_id < FLAGS_num_write_workers; worker_id++) {
        RDMAWRITEClient *client = new RDMAWRITEClient(FLAGS_write_size_kb,
                                                      FLAGS_server_id);
        client->req_id = lower_req_id;
        client->lower_req_id_ = lower_req_id;
        client->upper_req_id_ = upper_req_id;

        NovaRDMABroker *rdma_broker = nullptr;
        std::vector<QPEndPoint> endpoints;
        for (int i = 0; i < servers.size(); i++) {
            if (i == FLAGS_server_id) {
                continue;
            }
            QPEndPoint qp;
            qp.host = servers[i];
            qp.thread_id = worker_id;
            qp.server_id = i;
            endpoints.push_back(qp);
        }

        RDMAWRITEServerWorker *server_worker = new RDMAWRITEServerWorker(
                FLAGS_max_run_time, FLAGS_write_size_kb,
                FLAGS_is_local_disk_bench, FLAGS_disk_horizontal_scalability,
                FLAGS_server_id);
        server_worker->rtable_ = rtable;
        rdma_broker = new NovaRDMARCBroker(rdma_worker_buf, worker_id, endpoints, 0,
                                           FLAGS_rdma_max_num_sends,
                                           FLAGS_rdma_max_msg_size,
                                           FLAGS_rdma_doorbell_batch_size,
                                           FLAGS_server_id,
                                           rdma_buf,
                                           ntotal,
                                           FLAGS_rdma_port,
                                           server_worker);
        client->mem_manager_ = mem_manager;
        client->thread_id_ = worker_id;
        client->rdma_broker_ = rdma_broker;

        server_worker->client_ = client;
        server_worker->rdma_ctrl_ = rdma_ctrl;
        server_worker->rdma_broker_ = rdma_broker;
        server_worker->thread_id_ = worker_id;
        server_worker->mem_manager_ = mem_manager;
        server_worker->async_workers_ = disk_workers;
        server_worker->env_ = env;
        server_worker->write_size_kb_ = FLAGS_write_size_kb;
        server_worker->rtable_size_ = FLAGS_rtable_size_mb * 1024 * 1024;
        server_worker->max_num_rtables_ = FLAGS_max_num_rtables;
        server_worker->table_path_ = FLAGS_table_path;

        server_workers.push_back(server_worker);
        rdma_worker_buf += bench_nrdma_buf_unit() * servers.size();
    }
    // Address must match.
    NOVA_ASSERT((uint64_t) (rdma_worker_buf) == (uint64_t) (cache_buf));

    for (int worker_id = 0;
         worker_id < FLAGS_num_persist_workers; worker_id++) {
        disk_workers[worker_id]->cc_servers_ = server_workers;
        disk_workers[worker_id]->Init();
    }
    for (int worker_id = 0;
         worker_id < FLAGS_num_persist_workers; worker_id++) {
        disk_threads.emplace_back(
                std::thread(&RDMAWRITEDiskWorker::Start,
                            disk_workers[worker_id]));
    }
    for (int worker_id = 0;
         worker_id < FLAGS_num_write_workers; worker_id++) {
        server_threads.emplace_back(
                std::thread(&RDMAWRITEServerWorker::Start,
                            server_workers[worker_id]));
    }

    for (int worker_id = 0;
         worker_id < FLAGS_num_write_workers; worker_id++) {
        server_threads[worker_id].join();
    }

    uint32_t requests = 0;
    for (int worker_id = 0;
         worker_id < FLAGS_num_write_workers; worker_id++) {
        requests += server_workers[worker_id]->processed_number_of_req_;
    }

    NOVA_LOG(INFO) << fmt::format("Total requests: {}", requests);
    return 0;
}
