
//
// Created by Haoyu Huang on 1/23/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_SSTABLE_H
#define LEVELDB_NOVA_SSTABLE_H

#include "leveldb/db_types.h"
#include "mc/nova_mc_wb_worker.h"

namespace leveldb {
    class NovaSSTableManager : public SSTableManager {
    public:
        NovaSSTableManager(MemManager *mem_manager,
                           const std::vector<nova::NovaMCWBWorker *> &workers);

        void AddSSTable(const std::string &dbname, uint64_t file_number,
                        uint64_t thread_id,
                        char *backing_mem, uint64_t used_size,
                        uint64_t allocated_size,
                        bool async_flush) override;

        void GetSSTable(const std::string &dbname, uint64_t file_number,
                        WBTable **table) override;

        void
        RemoveSSTable(const std::string &dbname, uint64_t file_number) override;

    private:
        struct DBSSTables {
            std::map<uint64_t, WBTable *> fn_table;
            std::mutex mutex_;
        };

        DBSSTables ***server_db_sstables_;
        MemManager *mem_manager_;

        int current_worker_id_ = 0;
        const std::vector<nova::NovaMCWBWorker *> &workers_;
    };
}

#endif //LEVELDB_NOVA_SSTABLE_H
