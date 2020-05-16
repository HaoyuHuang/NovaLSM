
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_IN_MEMORY_LOG_MANAGER_H
#define LEVELDB_NOVA_IN_MEMORY_LOG_MANAGER_H

#include <unordered_map>

#include "mc/nova_mem_manager.h"
#include "db/dbformat.h"
#include "leveldb/log_writer.h"
#include "nova/linked_list.h"

namespace nova {

    class LogRecord {
    public:
        leveldb::Slice backing_mem;
        GlobalSSTableHandle table_handle;
        leveldb::ValueType type;
        leveldb::Slice key;
        leveldb::Slice value;

        leveldb::Status Encode(char *data);

        leveldb::Status Decode(char *data);
    };

    class LogFile {
    public:
        struct TableIndex {
            GlobalSSTableHandle handle;
            uint32_t file_offset;
            uint32_t nrecords;
        };

        LogFile(GlobalLogFileHandle handle, char *backing_mem,
                uint64_t file_size);

        void AddIndex(const TableIndex &index) {
            table_index_.push_back(index);
        }

        const std::vector<TableIndex> &table_index() {
            return table_index_;
        }

    private:
        GlobalLogFileHandle handle_;
        char *backing_mem_;
        uint64_t file_size_;
        std::vector<TableIndex> table_index_;
    };

    class InMemoryLogFileManager {
    public:
        InMemoryLogFileManager(NovaMemManager *mem_manager);

        void Add(uint64_t thread_id, const std::string &log_file, char *buf);

        void DeleteLogBuf(const std::string &log_file);

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

#endif //LEVELDB_NOVA_IN_MEMORY_LOG_MANAGER_H
