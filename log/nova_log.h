
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_LOG_H
#define LEVELDB_NOVA_LOG_H

#include "mc/nova_mem_manager.h"
#include "db/dbformat.h"
#include "leveldb/log_writer.h"
#include "nova/linked_list.h"

namespace nova {

    class LogFileManager {
    public:
        LogFileManager(NovaMemManager *mem_manager);

        void Add(uint64_t thread_id, const std::string &log_file, char *buf);

        void DeleteLogBuf(const std::string &log_file);

    private:
        struct LogRecords {
            std::map<uint64_t, std::vector<char *>> backing_mems;
            std::mutex mu;
        };

        struct DBLogFiles {
            std::map<std::string, LogRecords *> logfiles_;
            leveldb::port::Mutex mutex_;
        };

        NovaMemManager *mem_manager_;

        DBLogFiles ***server_db_log_files_;
    };
}

#endif //LEVELDB_NOVA_LOG_H
