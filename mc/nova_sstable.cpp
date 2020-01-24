
//
// Created by Haoyu Huang on 1/22/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_sstable.h"

#include "nova/nova_common.h"
#include "nova/nova_config.h"

namespace leveldb {
    void WBTable::Ref() {
        mutex_.lock();
        refcount++;
        mutex_.unlock();
    }

    void WBTable::Unref() {
        int remaining_ref = 0;
        bool delete_itself = false;
        mutex_.lock();
        refcount--;
        remaining_ref = refcount;
        mutex_.unlock();

        if (remaining_ref == 0 && deleted) {
            uint32_t scid = mem_manager_->slabclassid(thread_id,
                                                      allocated_size);
            mem_manager_->FreeItem(thread_id, backing_mem, scid);
            delete_itself = true;
        }

        if (delete_itself) {
            delete this;
        }
    }

    bool WBTable::is_deleted() {
        bool deleted_;
        mutex_.lock();
        deleted_ = deleted;
        mutex_.unlock();
        return deleted_;
    }

    void WBTable::Delete() {
        bool delete_itself = false;
        mutex_.lock();
        deleted = true;
        if (refcount == 0) {
            uint32_t scid = mem_manager_->slabclassid(thread_id,
                                                      allocated_size);
            mem_manager_->FreeItem(thread_id, backing_mem, scid);
            delete_itself = true;
        }
        mutex_.unlock();

        if (delete_itself) {
            delete this;
        }
    }

    NovaSSTableManager::NovaSSTableManager(MemManager *mem_manager,
                                           const std::vector<nova::NovaMCWBWorker *> &workers)
            : mem_manager_(mem_manager), workers_(workers) {
        server_db_sstables_ = new DBSSTables **[nova::NovaConfig::config->servers.size()];
        uint32_t nranges = nova::NovaCCConfig::cc_config->fragments.size() /
                           nova::NovaCCConfig::cc_config->cc_servers.size();
        for (int i = 0;
             i < nova::NovaCCConfig::cc_config->cc_servers.size(); i++) {
            server_db_sstables_[i] = new DBSSTables *[nranges];
            for (int j = 0; j < nranges; j++) {
                server_db_sstables_[i][j] = new DBSSTables;
            }
        }
    }

    void NovaSSTableManager::AddSSTable(const std::string &dbname,
                                        uint64_t file_number,
                                        uint64_t thread_id,
                                        char *backing_mem, uint64_t used_size,
                                        uint64_t allocated_size,
                                        bool async_flush) {
        uint32_t server_id;
        uint32_t db_indx;
        nova::ParseDBName(dbname, &server_id, &db_indx);

        DBSSTables *tables = server_db_sstables_[server_id][db_indx];
        tables->mutex_.lock();
        WBTable *table = new WBTable;
        table->thread_id = thread_id;
        table->backing_mem = backing_mem;
        table->allocated_size = allocated_size;
        table->used_size = used_size;
        tables->fn_table[file_number] = table;
        tables->mutex_.unlock();

        if (async_flush) {
            nova::WBRequest req;
            req.file_number = file_number;
            req.dbname = dbname;
            req.backing_mem = backing_mem;
            req.table_size = used_size;
            workers_[current_worker_id_]->AddRequest(req);
        }
    }

    void NovaSSTableManager::GetSSTable(const std::string &dbname,
                                        uint64_t file_number,
                                        WBTable **table) {
        *table = nullptr;
        uint32_t server_id;
        uint32_t db_indx;
        nova::ParseDBName(dbname, &server_id, &db_indx);

        DBSSTables *tables = server_db_sstables_[server_id][db_indx];
        tables->mutex_.lock();
        auto it = tables->fn_table.find(file_number);
        if (it != tables->fn_table.end()) {
            WBTable *t = it->second;
            if (!t->is_deleted()) {
                t->Ref();
                *table = t;
            }
        }
    }

    void NovaSSTableManager::RemoveSSTable(const std::string &dbname,
                                           uint64_t file_number) {
        uint32_t server_id;
        uint32_t db_indx;
        nova::ParseDBName(dbname, &server_id, &db_indx);

        DBSSTables *tables = server_db_sstables_[server_id][db_indx];
        tables->mutex_.lock();
        auto it = tables->fn_table.find(file_number);
        RDMA_ASSERT(it != tables->fn_table.end());
        WBTable *table = it->second;
        tables->fn_table.erase(file_number);
        tables->mutex_.unlock();
        table->Delete();
    }

    void NovaSSTableManager::RemoveSSTables(const std::string &dbname,
                                            const std::vector<uint64_t> &file_numbers) {
        uint32_t server_id;
        uint32_t db_indx;
        nova::ParseDBName(dbname, &server_id, &db_indx);

        DBSSTables *tables = server_db_sstables_[server_id][db_indx];
        tables->mutex_.lock();
        for (auto file_number : file_numbers) {
            auto it = tables->fn_table.find(file_number);
            RDMA_ASSERT(it != tables->fn_table.end());
            WBTable *table = it->second;
            tables->fn_table.erase(file_number);
            table->Delete();
        }
        tables->mutex_.unlock();
    }
}