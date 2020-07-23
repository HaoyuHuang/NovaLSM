
//
// Created by Haoyu Huang on 3/30/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_STAT_THREAD_H
#define LEVELDB_STAT_THREAD_H

#include <vector>

#include "common/nova_common.h"
#include "novalsm/rdma_msg_handler.h"
#include "stoc/storage_worker.h"

namespace nova {
    class NovaStatThread {
    public:
        void Start();

        std::vector<RDMAMsgHandler *> async_workers_;
        std::vector<RDMAMsgHandler *> async_compaction_workers_;
        std::vector<StorageWorker *> fg_storage_workers_;
        std::vector<StorageWorker *> bg_storage_workers_;
        std::vector<StorageWorker *> compaction_storage_workers_;
        std::vector<leveldb::EnvBGThread *> bgs_;
    private:
        struct StorageWorkerStats {
            uint32_t tasks = 0;
            uint64_t read_bytes = 0;
            uint64_t write_bytes = 0;
        };

        void Initialize(std::vector<StorageWorkerStats> *storage_stats,
                        const std::vector<StorageWorker *> &storage_workers);

        void OutputStats(const std::string &prefix,
                         std::string *output,
                         std::vector<StorageWorkerStats> *storage_stats,
                         const std::vector<StorageWorker *> &storage_workers);
    };
}


#endif //LEVELDB_STAT_THREAD_H
