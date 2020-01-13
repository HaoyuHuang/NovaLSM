
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_MEM_SERVER_H
#define RLIB_NOVA_MEM_SERVER_H

#include "mc/nova_mem_manager.h"
#include "nova_cc_conn_worker.h"
#include "nova/nova_config.h"
#include "nova/nova_rdma_store.h"
#include "nova/nova_rdma_rc_store.h"
#include "cc/nova_rdma_cc.h"
#include "leveldb/db.h"
#include "nova_cc.h"

namespace nova {
    class NovaCCConnWorker;

    class NovaCCServer {
    public:
        NovaCCServer(RdmaCtrl *rdma_ctrl, char *rdmabuf, int nport);

        void Start();

        void SetupListener();

        void LoadData();

        void LoadDataWithRangePartition();

        int nport_;
        int listen_fd_ = -1;            /* listener descriptor      */

        std::vector<leveldb::DB *> dbs_;
        NovaMemManager *manager;
        LogFileManager *log_manager;
        std::vector<NovaCCConnWorker*> conn_workers;
        std::vector<NovaRDMAComputeComponent *> async_workers;
        std::vector<leveldb::NovaCCCompactionThread *> bgs;

        struct event_base *base;
        int current_conn_worker_id_;
        vector<thread> conn_worker_threads;
        vector<thread> cc_workers;
        vector<thread> compaction_workers;
    };
}

#endif //RLIB_NOVA_MEM_SERVER_H
