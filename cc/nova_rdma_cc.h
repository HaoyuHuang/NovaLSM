
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
#include "log/nova_log.h"
#include "nova_cc_log_writer.h"
#include "nova_cc_server.h"

namespace nova {

    struct NovaAsyncTask {
        RequestType type;
        int conn_worker_id;
        std::string key;
        std::string value;
        int sock_fd;
        Connection *conn;
    };

    struct NovaAsyncCompleteTask {
        int sock_fd;
        Connection *conn;
    };

    struct NovaAsyncCompleteQueue {
        std::list<NovaAsyncCompleteTask> queue;
        leveldb::port::Mutex mutex;
        int read_fd;
        int write_fd;
        struct event readevent;
    };

    class NovaRDMAComputeComponent : public NovaMsgCallback {
    public:
        NovaRDMAComputeComponent(RdmaCtrl *rdma_ctrl,
                                 NovaMemManager *mem_manager,
                                 const std::vector<leveldb::DB *> &dbs,
                                 NovaAsyncCompleteQueue **cqs,
                                 bool is_worker_thread) :
                rdma_ctrl_(rdma_ctrl), mem_manager_(mem_manager), dbs_(dbs),
                cqs_(cqs), is_worker_thread_(is_worker_thread) {
            conn_workers_ = new bool[NovaCCConfig::cc_config->num_conn_workers];
            sem_init(&sem_, 0, 0);
        }

        bool IsInitialized();

        bool
        ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id, int remote_server_id,
                      char *buf, uint32_t imm_data) override;

        void Start();

        void AddTask(const NovaAsyncTask &task);

        int size();

        NovaRDMAStore *rdma_store_ = nullptr;
        leveldb::CCClient *cc_client_ = nullptr;
        NovaCCServer *cc_server_ = nullptr;
        uint64_t thread_id_;

    private:
        void ProcessGet(const NovaAsyncTask &task);

        void ProcessPut(const NovaAsyncTask &task);

        void ProcessVerify(const NovaAsyncTask &task);

        int ProcessQueue();

        RdmaCtrl *rdma_ctrl_ = nullptr;
        NovaMemManager *mem_manager_ = nullptr;
        bool is_running_ = false;
        std::vector<leveldb::DB *> dbs_;
        leveldb::port::Mutex mutex_;
        std::list<NovaAsyncTask> queue_;
        NovaAsyncCompleteQueue **cqs_;
        bool *conn_workers_;
        sem_t sem_;
        bool is_worker_thread_;
    };
}


#endif //LEVELDB_NOVA_RDMA_CC_H
