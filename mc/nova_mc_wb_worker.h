
//
// Created by Haoyu Huang on 1/22/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_MC_WB_WORKER_H
#define LEVELDB_NOVA_MC_WB_WORKER_H

#include <semaphore.h>
#include <map>

#include "leveldb/db_types.h"
#include "cc/nova_cc.h"
#include "nova_mem_manager.h"
#include "leveldb/env.h"
#include "cc/db_compaction_thread.h"

namespace nova {

    struct WBRequest {
        std::string dbname;
        uint32_t file_number;
        char *backing_mem;
        uint64_t table_size;
    };

    class NovaMCWBWorker {
    public:
        NovaMCWBWorker(std::vector<leveldb::DB *> dbs,
                       leveldb::NovaCCCompactionThread *compact_thread,
                       leveldb::SSTableManager *sstable_manager,
                       leveldb::Env *env);

        void AddRequest(const WBRequest &req);

        void Run();

    private:
        std::vector<leveldb::DB *> dbs_;
        leveldb::NovaCCCompactionThread *compact_thread_;
        leveldb::SSTableManager *sstable_manager_;
        leveldb::Env *env_;
        std::vector<WBRequest> requests_;
        sem_t sem_;
        std::mutex mutex_;
    };
}


#endif //LEVELDB_NOVA_MC_WB_WORKER_H
