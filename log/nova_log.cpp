
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova/nova_config.h"
#include "nova_log.h"

namespace nova {
    leveldb::Status LogRecord::Encode(char *data) {
//        uint32_t partition_id;
//        uint32_t memtable_id;
//        leveldb::ValueType type;
//        leveldb::Slice key;
//        leveldb::Slice value;
        RDMA_ASSERT(data);
        char *ptr = data;
        ptr += table_handle.Encode(ptr);
        leveldb::EncodeFixed32(ptr, type);
        ptr += 4;
        leveldb::EncodeFixed32(ptr, key.size());
        ptr += 4;
        memcpy(ptr, key.data(), key.size());
        ptr += key.size();
        leveldb::EncodeFixed32(ptr, value.size());
        ptr += 4;
        memcpy(ptr, value.data(), value.size());
    }

    leveldb::Status LogRecord::Decode(char *data) {
        char *ptr = data;
        ptr += table_handle.Decode(data);
        if (ptr == data) {
            return leveldb::Status::InvalidArgument("END");
        }

        key = leveldb::Slice(ptr, leveldb::DecodeFixed32(ptr));
        ptr += 4 + key.size();
        value = leveldb::Slice(ptr, leveldb::DecodeFixed32(ptr));
        ptr += 4 + value.size();
        backing_mem = leveldb::Slice(data, data - ptr);
        return leveldb::Status::OK();
    }

    LogFile::LogFile(GlobalLogFileHandle handle, char *backing_mem,
                     uint64_t file_size) : handle_(handle),
                                           backing_mem_(backing_mem),
                                           file_size_(file_size) {}

    LogFileManager::LogFileManager(
            nova::NovaMemManager *mem_manager) : mem_manager_(mem_manager) {
        server_db_log_files_ = new DBLogFiles **[NovaConfig::config->servers.size()];
        uint32_t nranges = NovaCCConfig::cc_config->fragments.size() /
                           NovaCCConfig::cc_config->cc_servers.size();
        for (int i = 0; i < NovaCCConfig::cc_config->cc_servers.size(); i++) {
            server_db_log_files_[i] = new DBLogFiles *[nranges];
            for (int j = 0; j < nranges; j++) {
                server_db_log_files_[i][j] = new DBLogFiles;
            }
        }
    }

    void LogFileManager::Add(const std::string &log_file, char *buf) {
        uint32_t sid;
        uint32_t db_index;
        ParseDBName(log_file, &sid, &db_index);

        DBLogFiles *db = server_db_log_files_[sid][db_index];
        db->mutex_.Lock();
        auto it = db->logfiles_.find(log_file);
        LogRecords *records;
        if (it == db->logfiles_.end()) {
            records = new LogRecords;
            db->logfiles_[log_file] = records;
        } else {
            records = it->second;
        }
        db->mutex_.Unlock();

        records->mu.lock();
        records->backing_mems.push_back(buf);
        records->mu.unlock();
    }

    void LogFileManager::DeleteLogBuf(uint64_t thread_id, const std::string &log_file) {
        uint32_t slabclassid = mem_manager_->slabclassid(thread_id,
                nova::NovaConfig::config->log_buf_size);
        uint32_t sid;
        uint32_t db_index;
        ParseDBName(log_file, &sid, &db_index);

        DBLogFiles *db = server_db_log_files_[sid][db_index];

        db->mutex_.Lock();
        auto it = db->logfiles_.find(log_file);
        if (it == db->logfiles_.end()) {
            db->mutex_.Unlock();
            return;
        }
        LogRecords *records = it->second;
        // Make a copy.
        std::vector<char *> items = records->backing_mems;
        db->logfiles_.erase(log_file);
        db->mutex_.Unlock();

        if (!items.empty()) {
            mem_manager_->FreeItems(thread_id, items, slabclassid);
        }


    }
}