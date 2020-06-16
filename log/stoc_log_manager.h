
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_STOC_LOG_MANAGER_H
#define LEVELDB_STOC_LOG_MANAGER_H

#include <unordered_map>

#include "common/nova_mem_manager.h"
#include "db/dbformat.h"
#include "leveldb/log_writer.h"

namespace nova {
    class StoCInMemoryLogFileManager {
    public:
        StoCInMemoryLogFileManager(NovaMemManager *mem_manager);

        void Add(uint64_t thread_id, const std::string &log_file, char *buf);

        void DeleteLogBuf(const std::vector<std::string> &log_files);

        void QueryLogFiles(uint32_t sid, uint32_t range_id,
                           std::unordered_map<std::string, uint64_t> *logfile_offset);

    private:
        struct LogRecords {
            std::unordered_map<uint64_t, std::vector<char *>> backing_mems;
            std::mutex mu;
        };

        struct DBLogFiles {
            std::unordered_map<std::string, LogRecords *> logfiles_;
            leveldb::port::Mutex mutex_;
        };
        NovaMemManager *mem_manager_;
        DBLogFiles ***server_db_log_files_;
    };
}

#endif //LEVELDB_STOC_LOG_MANAGER_H
