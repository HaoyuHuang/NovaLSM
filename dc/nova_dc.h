
//
// Created by Haoyu Huang on 1/7/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_DC_H
#define LEVELDB_NOVA_DC_H

#include <map>
#include <mutex>
#include "leveldb/env.h"

#include "leveldb/dc_client.h"
#include "mc/nova_mem_manager.h"
#include "include/leveldb/cache.h"

namespace leveldb {
    struct DCTableMetadata {
        int file_size;
    };


    struct DCTables {
        std::map<std::string, DCTableMetadata> tables;
        std::mutex mutex;
    };

    // Only one instance of this.
    class NovaDiskComponent {
    public:
        NovaDiskComponent(Env *env,
                          Cache *cache, const std::vector<std::string> &dbs);

        // Read the blocks and return the total size.
        uint64_t
        ReadBlocks(const std::string &dbname, uint64_t file_number,
                   const std::vector<DCBlockHandle> &block_handls, char *buf);

        // Read the SSTable and return the total size.
        uint64_t
        ReadSSTable(const std::string &dbname, uint64_t file_number, char *buf,
                    uint64_t size);

        void FlushSSTable(const std::string &dbname, uint64_t file_number,
                          char *buf,
                          uint64_t table_size);

        uint64_t TableSize(const std::string &dbname, uint64_t file_number);

    private:
        Status
        FindTable(const std::string &dbname, uint64_t file_number,
                  Cache::Handle **);

        Env *env_;
        Cache *cache_;
        // sstable file name to tables.
        std::map<std::string, DCTables *> db_tables_;
    };
}
#endif //LEVELDB_NOVA_DC_H
