
//
// Created by Haoyu Huang on 6/18/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_DESTINATION_MIGRATION_H
#define LEVELDB_DESTINATION_MIGRATION_H

#include <semaphore.h>

#include "db.h"
#include "db/db_impl.h"

namespace leveldb {
    class DBImpl;

    class DestinationMigration {
    public:
        void Start();

    private:
        MemManager* mem_manager_ = nullptr;
        DBImpl *db_ = nullptr;
    };
}



#endif //LEVELDB_DESTINATION_MIGRATION_H
