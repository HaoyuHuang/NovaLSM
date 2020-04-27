
//
// Created by Haoyu Huang on 1/10/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_dc_server.h"

#include "nova/nova_common.h"
#include "util/env_posix.h"
#include <netinet/tcp.h>
#include <signal.h>
#include <leveldb/write_batch.h>

namespace nova {

    NovaDCServer::NovaDCServer(rdmaio::RdmaCtrl *rdma_ctrl,
                               char *rdmabuf,
                               std::map<uint32_t, std::set<uint32_t >> &dbs) {
        char *buf = rdmabuf;
        char *cache_buf = buf + nrdma_buf_dc();

        NovaMemManager *mem_manager = new NovaMemManager(cache_buf, 1,
                                                         NovaConfig::config->mem_pool_size_gb, 0);
        leveldb::Cache *cache = leveldb::NewLRUCache(1024 * 1024 * 1024);
        InMemoryLogFileManager *logFileManager = new InMemoryLogFileManager(mem_manager);
        std::vector<std::string> dbnames;
        for (auto sid : dbs) {
            for (auto dbid : sid.second) {
                dbnames.push_back(DBName(NovaConfig::config->db_path,
                                         sid.first, dbid));
            }
        }

        for (auto &dbname : dbnames) {
            mkdirs(dbname.c_str());
        }

        leveldb::EnvOptions env_option;
        env_option.sstable_mode = leveldb::NovaSSTableMode::SSTABLE_DISK;
        leveldb::PosixEnv *env = new leveldb::PosixEnv;
        env->set_env_option(env_option);

        leveldb::NovaDiskComponent *dc = new leveldb::NovaDiskComponent(env,
                                                                        cache,
                                                                        dbnames);

        for (int worker_id = 0;
             worker_id < NovaDCConfig::dc_config->num_dc_workers; worker_id++) {
            NovaCCServer *rdma_dc = new NovaCCServer(
                    rdma_ctrl,
                    mem_manager, nullptr,
                    logFileManager, worker_id, true);
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
//                int max_num_sends,
//                int max_msg_size,
//                int doorbell_batch_size,
//                uint32_t my_server_id,
//                char *mr_buf,
//                uint64_t mr_size,
//                uint64_t rdma_port,

                store = new NovaRDMARCStore(buf, worker_id, endpoints,
                        NovaConfig::config->rdma_max_num_sends,
                        NovaConfig::config->max_msg_size,
                        NovaConfig::config->rdma_doorbell_batch_size,
                        NovaConfig::config->my_server_id,
                        NovaConfig::config->nova_buf,
                        NovaConfig::config->nnovabuf,
                        NovaConfig::config->rdma_port,
                        rdma_dc);
            } else {
                store = new NovaRDMANoopStore();
            }
            rdma_dc->rdma_store_ = store;
            dcs_.push_back(rdma_dc);
            buf += nrdma_buf_unit() *
                   NovaCCConfig::cc_config->cc_servers.size();
        }
        RDMA_ASSERT(buf == cache_buf);
    }

    void NovaDCServer::Start() {
        for (int worker_id = 0;
             worker_id < NovaDCConfig::dc_config->num_dc_workers; worker_id++) {
//            worker_threads.emplace_back(&NovaCCServer::Start,
//                                        dcs_[worker_id]);
        }
        for (auto &t : worker_threads) {
            t.join();
        }
    }

}