
//
// Created by Haoyu Huang on 6/18/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
// TODO: Support migration.

#ifndef LEVELDB_SOURCE_MIGRATION_H
#define LEVELDB_SOURCE_MIGRATION_H

#include "db.h"
#include "db/db_impl.h"
#include "leveldb/db_types.h"
#include "leveldb/stoc_client.h"
#include "stoc_client_impl.h"

namespace leveldb {
    class DBImpl;

    class SourceMigration {
    public:
        void MigrateDB(const std::vector<nova::LTCFragment*>& migrate_frags);

    private:
        uint32_t EncodeDBMeta(DBImpl* db, char *buf, uint32_t dbindex);

        MemManager *mem_manager_ = nullptr;
        StoCBlockClient *client_ = nullptr;
    };
}


#endif //LEVELDB_SOURCE_MIGRATION_H
