
//
// Created by Haoyu Huang on 3/30/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_CC_STAT_THREAD_H
#define LEVELDB_NOVA_CC_STAT_THREAD_H

#include <vector>
#include "nova_rdma_cc.h"
#include "nova_storage_worker.h"

namespace nova {
    class NovaStatThread {
    public:
        void Start();

        std::vector<leveldb::DB*> dbs_;
        std::vector<NovaRDMAComputeComponent *> async_workers_;
        std::vector<NovaRDMAComputeComponent *> async_compaction_workers_;
        std::vector<NovaStorageWorker *> cc_server_workers_;
        std::vector<leveldb::EnvBGThread *> bgs_;
    };
}



#endif //LEVELDB_NOVA_CC_STAT_THREAD_H
