
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

    void LogFileManager::Add(const std::string &log_file, char *buf) {
        mutex_.Lock();
        logfiles_[log_file].push_back(buf);
        mutex_.Unlock();
    }

    void LogFileManager::DeleteLogBuf(const std::string &log_file) {
        uint32_t slabclassid = mem_manager_->slabclassid(
                nova::NovaConfig::config->log_buf_size);
        mutex_.Lock();
        auto it = logfiles_.find(log_file);
        if (it == logfiles_.end()) {
            mutex_.Unlock();
            return;
        }
        // Make a copy.
        std::vector<char *> items = it->second;
        logfiles_.erase(log_file);
        mutex_.Unlock();
        mem_manager_->FreeItems(items, slabclassid);
    }
}