
//
// Created by Haoyu Huang on 6/18/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_DB_MIGRATION_H
#define LEVELDB_DB_MIGRATION_H

#include <atomic>
#include <mutex>
#include <semaphore.h>

#include "leveldb/db_types.h"
#include "leveldb/env_bg_thread.h"
#include "leveldb/stoc_client.h"
#include "stoc/persistent_stoc_file.h"
#include "stoc_client_impl.h"
#include "novalsm/rdma_msg_handler.h"
#include "log/stoc_log_manager.h"
#include "log/log_recovery.h"

namespace leveldb {
    class StoCBlockClient;
}

namespace nova {
    class RDMAMsgHandler;

    enum MigrateType {
        SOURCE = 0,
        DESTINATION = 1,
        STOC = 2
    };

    class DBMigration {
    public:
        DBMigration(
                leveldb::MemManager *mem_manager,
                leveldb::StoCBlockClient *client,
                nova::StoCInMemoryLogFileManager *log_manager,
                leveldb::StocPersistentFileManager *stoc_file_manager,
                const std::vector<RDMAMsgHandler *> &bg_rdma_msg_handlers,
                const std::vector<leveldb::EnvBGThread *> &bg_compaction_threads,
                const std::vector<leveldb::EnvBGThread *> &bg_flush_memtable_threads);

        void Start();

        void AddSourceMigrateDB(const std::vector<nova::LTCFragment *> &frags);

        void AddDestMigrateDB(char *buf, uint32_t msg_size);

        void AddStoCMigration(nova::LTCFragment * frag, const std::vector<uint32_t>& removed_stocs);

        static std::atomic_int_fast32_t migration_seq_id_;
    private:
        void MigrateDB(const std::vector<nova::LTCFragment *> &migrate_frags);

        struct DBMeta {
            MigrateType migrate_type;
            nova::LTCFragment *source_fragment = nullptr;
            char *buf = nullptr;
            uint32_t msg_size = 0;
            std::vector<uint32_t> removed_stocs;
        };

        void RecoverDBMeta(DBMeta dbmeta);

        void MigrateStoC(nova::LTCFragment * frag, const std::vector<uint32_t>& removed_stocs);

        std::mutex mu;
        std::vector<DBMeta> db_metas;
        sem_t sem_;
        std::vector<std::thread> threads_for_new_dbs_;

        leveldb::MemManager *mem_manager_ = nullptr;
        leveldb::StoCBlockClient *client_ = nullptr;
        nova::StoCInMemoryLogFileManager *log_manager_ = nullptr;
        leveldb::StocPersistentFileManager *stoc_file_manager_ = nullptr;
        std::vector<RDMAMsgHandler *> bg_rdma_msg_handlers_;
        std::vector<leveldb::EnvBGThread *> bg_compaction_threads_;
        std::vector<leveldb::EnvBGThread *> bg_flush_memtable_threads_;
    };
}


#endif //LEVELDB_DB_MIGRATION_H
