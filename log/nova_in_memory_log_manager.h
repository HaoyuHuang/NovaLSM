
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_IN_MEMORY_LOG_MANAGER_H
#define LEVELDB_NOVA_IN_MEMORY_LOG_MANAGER_H

#include "mc/nova_mem_manager.h"
#include "db/dbformat.h"
#include "leveldb/log_writer.h"
#include "nova/linked_list.h"

namespace nova {
    struct InMemoryReplica {
        uint32_t server_id = 0;
        uint64_t offset = 0;
        uint32_t size = 0;
    };

    class InMemoryLogFileManager {
    public:
        InMemoryLogFileManager(NovaMemManager *mem_manager);

        void Add(uint64_t thread_id, leveldb::MemTableIdentifier memtable_id,
                 char *buf);

        void DeleteLogBuf(leveldb::MemTableIdentifier);

        void
        AddReplica(leveldb::MemTableIdentifier memtable_id, uint32_t server_id,
                   uint64_t offset, uint32_t size);

        void RemoveMemTable(leveldb::MemTableIdentifier memtable_id);

        std::map<leveldb::MemTableIdentifier, std::vector<InMemoryReplica>> memtable_replicas_;
    private:
        struct LogRecords {
            std::map<uint64_t, std::vector<char *>> backing_mems;
            std::mutex mu;
        };

        struct DBLogFiles {
            std::map<leveldb::MemTableIdentifier, LogRecords *> logfiles_;
            leveldb::port::Mutex mutex_;
        };

        std::mutex mutex_;
        NovaMemManager *mem_manager_;
        DBLogFiles ***server_db_log_files_;
    };
}

#endif //LEVELDB_NOVA_IN_MEMORY_LOG_MANAGER_H
