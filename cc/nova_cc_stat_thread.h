
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

        std::vector<leveldb::DB *> dbs_;
        std::vector<NovaRDMAComputeComponent *> async_workers_;
        std::vector<NovaRDMAComputeComponent *> async_compaction_workers_;
        std::vector<NovaStorageWorker *> fg_storage_workers_;
        std::vector<NovaStorageWorker *> bg_storage_workers_;
        std::vector<NovaStorageWorker *> compaction_storage_workers_;
        std::vector<leveldb::EnvBGThread *> bgs_;
    private:
        struct StorageWorkerStats {
            uint32_t tasks = 0;
            uint64_t read_bytes = 0;
            uint64_t write_bytes = 0;
        };

        void Initialize(std::vector<StorageWorkerStats> *storage_stats,
                        const std::vector<NovaStorageWorker *> &storage_workers);

        void OutputStats(const std::string &prefix,
                         std::string *output,
                         std::vector<StorageWorkerStats> *storage_stats,
                         const std::vector<NovaStorageWorker *> &storage_workers);
    };
}


#endif //LEVELDB_NOVA_CC_STAT_THREAD_H
