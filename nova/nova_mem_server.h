
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_MEM_SERVER_H
#define RLIB_NOVA_MEM_SERVER_H

#include "mc/nova_mem_manager.h"
#include "nova_mem_worker.h"
#include "nova_mem_config.h"
#include "nova_rdma_store.h"
#include "nova_rdma_rc_store.h"
#include "leveldb/db.h"

namespace nova {
    class NovaMemWorker;

    class NovaMemServer {
    public:
        NovaMemServer(leveldb::DB *db, char *rdmabuf, int nport);

        void Start();

        void SetupListener();

        void LoadData();

        void LoadDataWithRangePartition();

        void LoadDataWithHashPartition();

        int nport_;
        int listen_fd_ = -1;            /* listener descriptor      */

        leveldb::DB *db_;
        NovaMemManager *manager;
        NovaMemWorker **workers;
        struct event_base *base;
        int current_store_id_;
        vector<thread> worker_threads;
    };
}

#endif //RLIB_NOVA_MEM_SERVER_H
