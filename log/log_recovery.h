
//
// Created by Haoyu Huang on 6/19/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_LOG_RECOVERY_H
#define LEVELDB_LOG_RECOVERY_H

#include "db/memtable.h"
#include "ltc/stoc_client_impl.h"

namespace leveldb {
    class StoCBlockClient;

    struct MemTableLogFilePair {
        MemTable *memtable = nullptr;
        std::string logfile;
        uint64_t logfile_offset;
        uint64_t logfile_size;
    };

    class LogRecovery {
    public:
        LogRecovery(leveldb::MemManager *mem_manager,
                    leveldb::StoCBlockClient *client);

        void Recover(const std::vector<MemTableLogFilePair> &memtables_to_recover, uint32_t cfg_id, uint32_t dbid);

    private:
        leveldb::MemManager *mem_manager_;
        leveldb::StoCBlockClient *client_;
    };
}

#endif //LEVELDB_LOG_RECOVERY_H
