
//
// Created by Haoyu Huang on 1/10/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_dc_server.h"

#include "util/env_posix.h"
#include <netinet/tcp.h>
#include <signal.h>
#include <leveldb/write_batch.h>

namespace nova {

    NovaDCServer::NovaDCServer(rdmaio::RdmaCtrl *rdma_ctrl,
                               char *rdmabuf,
                               std::map<uint32_t, std::set<uint32_t >> &dbs) {
        NovaRDMARCStore *rdma_store;

        char *buf = rdmabuf;
        char *cache_buf = buf + nrdma_buf_dc();

        NovaMemManager *mem_manager = new NovaMemManager(cache_buf);
        leveldb::Cache *cache = leveldb::NewLRUCache(1024 * 1024 * 1024);
        LogFileManager *logFileManager = new LogFileManager(mem_manager);
        std::vector<std::string> dbnames;
        for (auto sid : dbs) {
            for (auto dbid : sid.second) {
                dbnames.push_back(DBName(NovaConfig::config->db_path,
                                         sid.first, dbid));
            }
        }

        leveldb::NovaDiskComponent *dc = new leveldb::NovaDiskComponent(
                new leveldb::PosixEnv, cache, dbnames);

        for (int worker_id = 0;
             worker_id < NovaDCConfig::dc_config->num_dc_workers; worker_id++) {
            NovaRDMADiskComponent *rdma_dc = new NovaRDMADiskComponent(rdma_ctrl,
                                                               mem_manager, dc,
                                                               logFileManager);
            NovaRDMAStore *store = nullptr;
            std::vector<QPEndPoint> endpoints;
            for (int i = 0;
                 i < NovaCCConfig::cc_config->cc_servers.size(); i++) {
                QPEndPoint qp;
                qp.host = NovaCCConfig::cc_config->cc_servers[i];
                qp.thread_id = worker_id;
                qp.server_id = i;
                endpoints.push_back(qp);
            }

            if (NovaConfig::config->enable_rdma) {
                store = new NovaRDMARCStore(buf, worker_id, endpoints, rdma_dc);
            } else {
                store = new NovaRDMANoopStore();
            }
            rdma_dc->rdma_store_ = store;
            dcs_.push_back(rdma_dc);
            worker_threads.emplace_back(
                    std::thread(&NovaRDMADiskComponent::Start, rdma_dc));
        }


    }

    void NovaDCServer::Start() {
        for (int worker_id = 0;
             worker_id < NovaDCConfig::dc_config->num_dc_workers; worker_id++) {
            worker_threads.emplace_back(
                    std::thread(&NovaRDMADiskComponent::Start, dcs_[worker_id]));
        }
        for (auto &t : worker_threads) {
            t.join();
        }
    }

}