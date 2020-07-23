
//
// Created by Haoyu Huang on 6/18/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
// TODO: Support migration.

#ifndef LEVELDB_SOURCE_MIGRATION_H
#define LEVELDB_SOURCE_MIGRATION_H

#include "leveldb/db_types.h"
#include "leveldb/stoc_client.h"
#include "stoc_client_impl.h"

namespace leveldb {

    class SourceMigration {
    public:
        void MigrateDB(const std::vector<nova::LTCFragment*>& migrate_frags);

    private:

        MemManager *mem_manager_ = nullptr;
        StoCBlockClient *client_ = nullptr;
    };
}


#endif //LEVELDB_SOURCE_MIGRATION_H
