
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova/nova_config.h"
#include "nova_in_memory_log_manager.h"

namespace nova {
    InMemoryLogFileManager::InMemoryLogFileManager(
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

    void InMemoryLogFileManager::AddReplica(
            leveldb::MemTableIdentifier memtable_id, uint32_t server_id,
            uint64_t offset, uint32_t size) {
        mutex_.lock();
        InMemoryReplica replica = {};
        replica.server_id = server_id;
        replica.offset = offset;
        replica.size = size;
        memtable_replicas_[memtable_id].push_back(replica);
        mutex_.unlock();
    }

    void InMemoryLogFileManager::RemoveMemTable(
            leveldb::MemTableIdentifier memtable_id) {
        mutex_.lock();
        memtable_replicas_.erase(memtable_id);
        mutex_.unlock();
    }

    void
    InMemoryLogFileManager::Add(uint64_t thread_id, leveldb::MemTableIdentifier memtable_id,
                                char *buf) {

        DBLogFiles *db = server_db_log_files_[memtable_id.cc_id][memtable_id.db_id];
        db->mutex_.Lock();
        auto it = db->logfiles_.find(memtable_id);
        LogRecords *records;
        if (it == db->logfiles_.end()) {
            records = new LogRecords;
            db->logfiles_[memtable_id] = records;
        } else {
            records = it->second;
        }
        db->mutex_.Unlock();

        records->mu.lock();
        records->backing_mems[thread_id].push_back(buf);
        records->mu.unlock();

//        RDMA_LOG(DEBUG)
//            << fmt::format("Allocate log buf for file:{} from thread {}",
//                           log_file, thread_id);
    }

    void InMemoryLogFileManager::DeleteLogBuf(leveldb::MemTableIdentifier memtable_id) {
        DBLogFiles *db = server_db_log_files_[memtable_id.cc_id][memtable_id.db_id];

        db->mutex_.Lock();
        auto it = db->logfiles_.find(memtable_id);
        if (it == db->logfiles_.end()) {
            db->mutex_.Unlock();
            return;
        }
        LogRecords *records = it->second;
        db->logfiles_.erase(memtable_id);
        db->mutex_.Unlock();

        for (const auto &it : records->backing_mems) {
            const auto &items = it.second;
            uint32_t scid = mem_manager_->slabclassid(it.first,
                                                      NovaConfig::config->log_buf_size);
            if (!items.empty()) {
                mem_manager_->FreeItems(it.first, items, scid);
            }

//            RDMA_LOG(DEBUG)
//                << fmt::format("Free {} log buf for file:{} from thread {}",
//                               items.size(), log_file, it.first);
        }


    }
}