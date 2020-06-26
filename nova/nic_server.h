
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_MEM_SERVER_H
#define RLIB_NOVA_MEM_SERVER_H

#include "mc/nova_mem_manager.h"
#include "client_req_worker.h"
#include "nova_config.h"
#include "nova_rdma_broker.h"
#include "nova_rdma_rc_broker.h"
#include "nova_async_worker.h"
#include "leveldb/db.h"

namespace nova {
    class NovaConnWorker;

    class NovaMemServer {
    public:
        NovaMemServer(const std::vector<leveldb::DB *>& dbs, char *rdmabuf, int nport);

        void Start();

        void SetupListener();

        void LoadData();

        void LoadDataWithRangePartition();

        int nport_;
        int listen_fd_ = -1;            /* listener descriptor      */

        std::vector<leveldb::DB *> dbs_;
        NovaMemManager *manager;
        LogFileManager *log_manager;
        NovaConnWorker **conn_workers;
        NovaAsyncWorker **async_workers;

        struct event_base *base;
        int current_store_id_;
        vector<thread> worker_threads;
        vector<thread> async_worker_threads;
    };
}

#endif //RLIB_NOVA_MEM_SERVER_H
