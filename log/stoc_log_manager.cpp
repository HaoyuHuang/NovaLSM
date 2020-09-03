
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "common/nova_config.h"
#include "stoc_log_manager.h"

namespace nova {
    uint32_t StoCInMemoryLogFileManager::EncodeLogFiles(char *buf, uint32_t dbid) {
        uint32_t msg_size = 0;
        DBLogFiles *logfiles = db_log_files_[dbid];
        logfiles->mutex_.Lock();
        msg_size += leveldb::EncodeFixed32(buf, logfiles->logfiles_.size());
        NOVA_LOG(rdmaio::INFO) << fmt::format("Log: Number of log files: {}", logfiles->logfiles_.size());
        for (const auto &logfile : logfiles->logfiles_) {
            uint32_t dbindex = 0;
            uint32_t memtableid = 0;
            ParseDBIndexFromLogFileName(logfile.first, &dbindex, &memtableid);
            NOVA_ASSERT(dbindex == dbid) << fmt::format("{}-{}", dbindex, dbid);
            msg_size += leveldb::EncodeFixed32(buf + msg_size, memtableid);
            msg_size += leveldb::EncodeFixed32(buf + msg_size, logfile.second->remote_backing_mems.size());
            for (const auto &replica : logfile.second->remote_backing_mems) {
                msg_size += leveldb::EncodeFixed32(buf + msg_size, replica.first);
                msg_size += leveldb::EncodeFixed64(buf + msg_size, replica.second);
            }
            NOVA_LOG(rdmaio::INFO) << fmt::format("Log file {}: Range {} memtable:{}", logfile.first, dbid, memtableid);
        }
        logfiles->mutex_.Unlock();
        return msg_size;
    }

    bool StoCInMemoryLogFileManager::DecodeLogFiles(leveldb::Slice *buf,
                                                    std::unordered_map<uint32_t, leveldb::MemTableLogFilePair> *mid_table_map) {
        uint32_t num_logfiles = 0;
        NOVA_ASSERT(leveldb::DecodeFixed32(buf, &num_logfiles));
        NOVA_LOG(rdmaio::INFO) << fmt::format("Log: Decoded number of log files: {}", num_logfiles);
        for (int i = 0; i < num_logfiles; i++) {
            uint32_t memtableid = 0;
            NOVA_ASSERT(leveldb::DecodeFixed32(buf, &memtableid));
            uint32_t nreplicas = 0;
            NOVA_ASSERT(leveldb::DecodeFixed32(buf, &nreplicas));
            for (int r = 0; r < nreplicas; r++) {
                uint32_t sid = 0;
                uint64_t offset = 0;
                NOVA_ASSERT(leveldb::DecodeFixed32(buf, &sid));
                NOVA_ASSERT(leveldb::DecodeFixed64(buf, &offset));
                (*mid_table_map)[memtableid].server_logbuf[sid] = offset;
            }
        }
        return true;
    }


    StoCInMemoryLogFileManager::StoCInMemoryLogFileManager(
            nova::NovaMemManager *mem_manager) : mem_manager_(mem_manager) {
        uint32_t nranges = NovaConfig::config->cfgs[0]->fragments.size();
        db_log_files_ = new DBLogFiles *[nranges];
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("{} {}", NovaConfig::config->servers.size(),
                           nranges);
        for (int i = 0; i < nranges; i++) {
            db_log_files_[i] = new DBLogFiles;
        }
    }

    void
    StoCInMemoryLogFileManager::QueryLogFiles(uint32_t range_id,
                                              std::unordered_map<std::string, uint64_t> *logfile_offset) {
        DBLogFiles *db = db_log_files_[range_id];
        db->mutex_.Lock();
        for (const auto &it : db->logfiles_) {
            if (it.second->local_backing_mems.empty()) {
                continue;
            }
            uint64_t offset = (uint64_t) it.second->local_backing_mems[0];
            (*logfile_offset)[it.first] = offset;
        }
        db->mutex_.Unlock();
    }

    void
    StoCInMemoryLogFileManager::AddLocalBuf(const std::string &log_file,
                                            char *local_buf) {
        uint32_t db_index;
        ParseDBIndexFromLogFileName(log_file, &db_index);
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("{} {}", log_file, db_index);
        DBLogFiles *db = db_log_files_[db_index];
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
        records->local_backing_mems.push_back(local_buf);
        records->mu.unlock();
        NOVA_LOG(DEBUG)
            << fmt::format("Allocate log buf for file:{}", log_file);
    }

    void StoCInMemoryLogFileManager::AddRemoteBuf(const std::string &log_file, uint32_t remote_server_id,
                                                  uint64_t remote_buf_offset) {
        uint32_t db_index;
        ParseDBIndexFromLogFileName(log_file, &db_index);
        DBLogFiles *db = db_log_files_[db_index];
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
        records->remote_backing_mems[remote_server_id] = remote_buf_offset;
        records->mu.unlock();
        NOVA_LOG(DEBUG) << fmt::format("Allocate log buf for file:{}", log_file);
    }

    void StoCInMemoryLogFileManager::DeleteLogBuf(
            const std::vector<std::string> &log_file) {
        uint32_t db_index;
        ParseDBIndexFromLogFileName(log_file[0], &db_index);
        DBLogFiles *db = db_log_files_[db_index];
        db->mutex_.Lock();
        for (int i = 0; i < log_file.size(); i++) {
            auto it = db->logfiles_.find(log_file[i]);
            if (it == db->logfiles_.end()) {
                continue;
            }
            LogRecords *records = it->second;
            for (auto buf : records->local_backing_mems) {
                if (!buf) {
                    continue;
                }
                uint32_t scid = mem_manager_->slabclassid(0, NovaConfig::config->max_stoc_file_size);
                mem_manager_->FreeItem(0, buf, scid);
                NOVA_LOG(DEBUG) << fmt::format("Free log buf for file:{}", log_file[i]);
            }
            db->logfiles_.erase(log_file[i]);
        }
        db->mutex_.Unlock();
    }
}