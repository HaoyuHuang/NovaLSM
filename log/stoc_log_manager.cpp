
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "common/nova_config.h"
#include "stoc_log_manager.h"

namespace nova {
    StoCInMemoryLogFileManager::StoCInMemoryLogFileManager(
            nova::NovaMemManager *mem_manager) : mem_manager_(mem_manager) {
        server_db_log_files_ = new DBLogFiles **[NovaConfig::config->servers.size()];
        uint32_t nranges = NovaConfig::config->fragments.size() /
                           NovaConfig::config->ltc_servers.size();
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("{} {}", NovaConfig::config->servers.size(),
                           nranges);
        for (int i = 0; i < NovaConfig::config->ltc_servers.size(); i++) {
            server_db_log_files_[i] = new DBLogFiles *[nranges];
            for (int j = 0; j < nranges; j++) {
                server_db_log_files_[i][j] = new DBLogFiles;
            }
        }
    }

    void StoCInMemoryLogFileManager::QueryLogFiles(uint32_t sid, uint32_t range_id,
                                                   std::unordered_map<std::string, uint64_t> *logfile_offset) {
        DBLogFiles *db = server_db_log_files_[sid][range_id];
        db->mutex_.Lock();
        for (const auto &it : db->logfiles_) {
            uint64_t offset = (uint64_t) it.second->backing_mems.begin()->second[0];
            (*logfile_offset)[it.first] = offset;
        }
        db->mutex_.Unlock();
    }

    void
    StoCInMemoryLogFileManager::Add(uint64_t thread_id, const std::string &log_file,
                                    char *buf) {
        uint32_t sid;
        uint32_t db_index;
        ParseDBIndexFromLogFileName(log_file, &sid, &db_index);
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("{} {} {}", log_file, sid, db_index);
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
        records->backing_mems[thread_id].push_back(buf);
        records->mu.unlock();

        NOVA_LOG(DEBUG)
            << fmt::format("Allocate log buf for file:{} from thread {}",
                           log_file, thread_id);
    }

    void StoCInMemoryLogFileManager::DeleteLogBuf(
            const std::vector<std::string> &log_file) {
        uint32_t sid;
        uint32_t db_index;
        ParseDBIndexFromLogFileName(log_file[0], &sid, &db_index);
        DBLogFiles *db = server_db_log_files_[sid][db_index];
        db->mutex_.Lock();
        for (int i = 0; i < log_file.size(); i++) {
            auto it = db->logfiles_.find(log_file[i]);
            if (it == db->logfiles_.end()) {
                continue;
            }
            LogRecords *records = it->second;
            for (const auto &it : records->backing_mems) {
                const auto &items = it.second;
                uint32_t scid = mem_manager_->slabclassid(it.first,
                                                          NovaConfig::config->log_buf_size);
                if (!items.empty()) {
                    mem_manager_->FreeItems(it.first, items, scid);
                }
                NOVA_LOG(DEBUG)
                    << fmt::format("Free {} log buf for file:{} from thread {}",
                                   items.size(), log_file[i], it.first);
            }
            db->logfiles_.erase(log_file[i]);
        }
        db->mutex_.Unlock();
    }
}