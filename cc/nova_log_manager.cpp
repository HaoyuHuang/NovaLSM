
//
// Created by Haoyu Huang on 3/18/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova/logging.hpp"
#include <fmt/core.h>
#include "nova_log_manager.h"

namespace nova {
    NovaLogFile::NovaLogFile(leveldb::Env *env, uint32_t log_file_id,
                             std::string log_file_name,
                             uint64_t max_file_size) : env_(
            env), log_file_id_(log_file_id), log_file_name_(log_file_name),
                                                       max_file_size_(
                                                               max_file_size) {
        leveldb::EnvFileMetadata meta = {};
        meta.level = 0;
        RDMA_ASSERT(env_->NewWritableFile(log_file_name, meta,
                                          &writable_file_).ok());
        current_file_size_ = 0;
    }

    void NovaLogFile::Seal() {
        RDMA_ASSERT(!seal_);
        seal_ = true;
        is_full_ = true;
        delete writable_file_;
        writable_file_ = nullptr;
    }

    void NovaLogFile::DeleteMemTables(
            const std::vector<nova::MemTableIdentifier> &ids) {
        mutex_.lock();
        if (deleted_) {
            mutex_.unlock();
            return;
        }

        for (auto &id : ids) {
            int n = memtables.erase(id);
            RDMA_ASSERT(n == 1);
        }
        if (memtables.empty() && pending_log_records_.empty()) {
            Seal();
            deleted_ = true;
            RDMA_ASSERT(env_->DeleteFile(log_file_name_).ok());
        }
        mutex_.unlock();
    }

    bool NovaLogFile::ReserveSpaceForLogRecord(const LogRecord &record) {
        mutex_.lock();
        if (is_full_) {
            mutex_.unlock();
            return false;
        }
        if (max_file_size_ != 0 &&
            record.log_record_size + current_file_size_ >= max_file_size_) {
            is_full_ = true;
            mutex_.unlock();
            return false;
        }
        pending_log_records_.push_back(record);
        current_file_size_ += record.log_record_size;
        mutex_.unlock();
        return true;
    }

    void NovaLogFile::PersistLogRecords() {
        mutex_.lock();
        if (pending_log_records_.empty()) {
            mutex_.unlock();
            return;
        }

        for (auto &record : pending_log_records_) {
            memtables.insert(record.memtable_id);
            RDMA_ASSERT(writable_file_->Append(
                    leveldb::Slice(record.log_record,
                                   record.log_record_size)).ok());
        }
        writable_file_->Sync();
        if (is_full_) {
            Seal();
        }
        pending_log_records_.clear();
        mutex_.unlock();
    }


    NovaLogManager::NovaLogManager(leveldb::Env *env, uint64_t max_file_size,
                                   nova::NovaMemManager *mem_manager,
                                   const std::string &log_file_path,
                                   uint32_t nccs, uint32_t nworkers,
                                   uint32_t log_buf_size) : env_(env),
                                                            max_file_size_(
                                                                    max_file_size),
                                                            log_file_path_(
                                                                    log_file_path) {
        log_file_id_seq_.store(0);
        init_log_bufs_ = new char **[nccs];
        for (int i = 0; i < nccs; i++) {
            init_log_bufs_[i] = new char *[nworkers];
        }
        uint32_t scid = mem_manager->slabclassid(0, log_buf_size);
        for (int i = 0; i < nccs; i++) {
            for (int j = 0; j < nworkers; j++) {
                init_log_bufs_[i][j] = mem_manager->ItemAlloc(0, scid);
            }
        }
    }

    NovaLogFile *NovaLogManager::CreateNewLogFile() {
        uint32_t log_file_id = log_file_id_seq_.fetch_add(1);
        RDMA_ASSERT(log_file_id < MAX_NUM_LOG_FILES);
        NovaLogFile *file = new NovaLogFile
                (env_, log_file_id,
                 fmt::format("{}/log-{}", log_file_path_, log_file_id),
                 max_file_size_);
        log_files[log_file_id] = file;
        return file;
    }

    NovaLogFile *NovaLogManager::log_file(uint32_t log_file_id) {
        return log_files[log_file_id];
    }

    NovaLogFile *NovaLogManager::log_file(
            nova::MemTableIdentifier memtable_id) {
        NovaLogFile *file = nullptr;
        mutex_.lock();
        auto it = memtableid_log_file_id_map_.find(memtable_id);
        if (it == memtableid_log_file_id_map_.end()) {
            file = CreateNewLogFile();
            memtableid_log_file_id_map_[memtable_id] = file;
        } else {
            file = it->second;
        }
        mutex_.unlock();
        return file;
    }
}