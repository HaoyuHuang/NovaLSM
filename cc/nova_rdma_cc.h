
//
// Created by Haoyu Huang on 12/25/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_RDMA_CC_H
#define LEVELDB_NOVA_RDMA_CC_H

#include <string>
#include <port/port_stdcxx.h>
#include <leveldb/db.h>
#include "leveldb/options.h"
#include "nova/nova_common.h"
#include "nova/nova_msg_callback.h"
#include "nova/nova_rdma_store.h"
#include <semaphore.h>
#include <list>
#include <cc/nova_cc_client.h>
#include "log/nova_in_memory_log_manager.h"
#include "nova_cc_log_writer.h"
#include "nova_cc_server.h"
#include "nova/nova_config.h"

namespace nova {

    struct CacheValue {
        std::string value;
    };

    class NovaRDMAComputeComponent : public NovaMsgCallback {
    public:
        NovaRDMAComputeComponent(RdmaCtrl *rdma_ctrl,
                                 NovaMemManager *mem_manager,
                                 const std::vector<leveldb::DB *> &dbs) :
                rdma_ctrl_(rdma_ctrl), mem_manager_(mem_manager), dbs_(dbs) {
            stat_tasks_ = 0;
            sem_init(&sem_, 0, 0);
            should_pause = false;
            paused = false;
        }

        bool IsInitialized();

        bool
        ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id, int remote_server_id,
                      char *buf, uint32_t imm_data) override;

        void Start();

        void AddTask(const leveldb::RDMAAsyncClientRequestTask &task);

        int size();

        NovaRDMAStore *rdma_store_ = nullptr;
        leveldb::CCClient *cc_client_ = nullptr;
        NovaCCServer *cc_server_ = nullptr;
        uint64_t thread_id_ = 0;
        std::atomic_int_fast64_t stat_tasks_;

        bool verify_complete() {
            mutex_.Lock();
            bool vc = verify_complete_;
            mutex_.Unlock();
            return vc;
        }

        std::atomic_bool should_pause;
        std::atomic_bool paused;
        sem_t sem_;
    private:
        int ProcessQueue();

        struct RequestCtx {
            uint32_t req_id = 0;
            sem_t *sem = nullptr;
            leveldb::CCResponse *response = nullptr;
        };

        bool verify_complete_ = false;
        std::list<RequestCtx> pending_reqs_;
        RdmaCtrl *rdma_ctrl_ = nullptr;
        NovaMemManager *mem_manager_ = nullptr;
        bool is_running_ = false;
        std::vector<leveldb::DB *> dbs_;
        leveldb::port::Mutex mutex_;
        std::list<leveldb::RDMAAsyncClientRequestTask> queue_;
    };
}


#endif //LEVELDB_NOVA_RDMA_CC_H
