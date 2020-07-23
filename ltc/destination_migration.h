
//
// Created by Haoyu Huang on 6/18/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_DESTINATION_MIGRATION_H
#define LEVELDB_DESTINATION_MIGRATION_H

#include <atomic>
#include <mutex>
#include <semaphore.h>

#include "db.h"
#include "db/db_impl.h"
#include "leveldb/db_types.h"
#include "leveldb/stoc_client.h"
#include "stoc_client_impl.h"

#include "log/log_recovery.h"

namespace leveldb {
    class DBImpl;

    class DestinationMigration {
    public:
        DestinationMigration(
                leveldb::StocPersistentFileManager *stoc_file_manager,
                const std::vector<leveldb::EnvBGThread *> &bg_compaction_threads,
                const std::vector<leveldb::EnvBGThread *> &bg_flush_memtable_threads);

        void Start();

        void AddReceivedDBId(char *buf, uint32_t msg_size);

        static std::atomic_uint_fast32_t migration_seq_id_;
    private:
        struct DBMeta {
            char *buf;
            uint32_t msg_size;
        };

        void RecoverDBMeta(DBMeta dbmeta, int cfg_id);

        std::mutex mu;
        std::vector<DBMeta> db_metas;
        sem_t sem_;
        MemManager *mem_manager_ = nullptr;

        leveldb::StocPersistentFileManager *stoc_file_manager_ = nullptr;
        std::vector<leveldb::EnvBGThread *> bg_compaction_threads_;
        std::vector<leveldb::EnvBGThread *> bg_flush_memtable_threads_;
    };
}


#endif //LEVELDB_DESTINATION_MIGRATION_H
