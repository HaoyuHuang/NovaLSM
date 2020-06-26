
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <nova/nova_config.h>
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
        uint32_t nranges = NovaConfig::config->nfragments /
                           NovaConfig::config->servers.size();
        for (int i = 0; i < NovaConfig::config->servers.size(); i++) {
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

    void LogFileManager::AddLogRecord(const std::string &log_file,
                                      const leveldb::Slice &log_record) {
        uint32_t lb = nova::NovaConfig::config->log_buf_size;
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

        char *lb_buf = nullptr;
        uint32_t index = 0;
        records->mu.lock();
        if (records->backing_mems.empty() ||
            records->index + log_record.size() > lb) {
            uint64_t hash = nova::LogFileHash(log_file);
            uint32_t slabclassid = mem_manager_->slabclassid(
                    hash, nova::NovaConfig::config->log_buf_size);
            char *buf = mem_manager_->ItemAlloc(hash, slabclassid);
            RDMA_ASSERT(buf != nullptr);
            records->backing_mems.push_back(buf);
            records->index = 0;
        }
        lb_buf = records->backing_mems[records->backing_mems.size() - 1];
        index = records->index;
        records->index += log_record.size();
        records->mu.unlock();

        memcpy(lb_buf + index, log_record.data(), log_record.size());
    }

    void LogFileManager::DeleteLogBuf(const std::string &log_file) {
        uint64_t hash = nova::LogFileHash(log_file);
        uint32_t slabclassid = mem_manager_->slabclassid(hash,
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
            mem_manager_->FreeItems(hash, items, slabclassid);
        }
    }
}