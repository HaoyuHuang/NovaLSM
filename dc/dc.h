
//
// Created by Haoyu Huang on 1/7/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_DC_H
#define LEVELDB_DC_H

#include "mc/nova_mem_manager.h"
#include "include/leveldb/cache.h"

namespace leveldb {
    struct DCBlockHandle {
        uint64_t offset;
        uint64_t size;
    };

    struct DCTableMetadata {
        int file_size;
    };


    struct DCTables {
        std::map<std::string, DCTableMetadata> tables;
        std::mutex mutex;
    };

    class DiskComponent {
    public:
        DiskComponent(Env *env, nova::NovaMemManager *mem_manager,
                      Cache *cache, const std::vector<std::string>& dbs);

        // Read the blocks and return the total size.
        uint64_t
        Read(const std::string &dbname, uint64_t file_number,
             const std::vector<DCBlockHandle> &block_handls, char *buf);

        // Read the SSTable and return the total size.
        uint64_t
        Read(const std::string &dbname, uint64_t file_number, char *buf, uint64_t size);

        void FlushSSTable(const std::string &dbname, uint64_t file_number,
                          char *buf,
                          uint64_t table_size);

        uint64_t TableSize(const std::string &dbname, uint64_t file_number);

    private:
        Status
        FindTable(const std::string &dbname, uint64_t file_number,
                  Cache::Handle **);

        Env *env_;
        nova::NovaMemManager *mem_manager_;
        Cache *cache_;
        std::map<std::string, DCTables*> db_tables_;
    };
}
#endif //LEVELDB_DC_H
