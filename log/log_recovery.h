
//
// Created by Haoyu Huang on 6/19/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_LOG_RECOVERY_H
#define LEVELDB_LOG_RECOVERY_H

#include "db/memtable.h"

namespace leveldb {
    struct MemTableLogFilePair {
        MemTable* memtable = nullptr;
        std::string logfile;
    };

    class LogRecovery {
    public:
        void Recover(std::vector<MemTableLogFilePair> memtables_to_recover);
    };
}

#endif //LEVELDB_LOG_RECOVERY_H
