
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_STOC_LOG_MANAGER_H
#define LEVELDB_STOC_LOG_MANAGER_H

#include <unordered_map>

#include "db/memtable.h"
#include "common/nova_mem_manager.h"
#include "db/dbformat.h"
#include "leveldb/log_writer.h"

namespace nova {
    // Manage in-memory log files to provide high availability.
    class StoCInMemoryLogFileManager {
    public:
        StoCInMemoryLogFileManager(NovaMemManager *mem_manager);

        void AddLocalBuf(const std::string &log_file, char *buf);

        void AddRemoteBuf(const std::string &log_file, uint32_t remote_server_id, uint64_t remote_buf_offset);

        void DeleteLogBuf(const std::vector<std::string> &log_files);

        void QueryLogFiles(uint32_t range_id,
                           std::unordered_map<std::string, uint64_t> *logfile_offset);

        uint32_t EncodeLogFiles(char *buf, uint32_t dbid);

        bool
        DecodeLogFiles(leveldb::Slice *buf, std::unordered_map<uint32_t, leveldb::MemTableLogFilePair> *mid_table_map);

    private:
        struct LogRecords {
            std::vector<char *> local_backing_mems;
            std::unordered_map<uint32_t, uint64_t> remote_backing_mems;
            std::mutex mu;
        };

        struct DBLogFiles {
            std::unordered_map<std::string, LogRecords *> logfiles_;
            leveldb::port::Mutex mutex_;
        };
        NovaMemManager *mem_manager_;
        DBLogFiles **db_log_files_;
    };
}

#endif //LEVELDB_STOC_LOG_MANAGER_H
