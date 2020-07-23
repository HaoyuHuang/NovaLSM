
//
// Created by Haoyu Huang on 6/20/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_RDMA_WRITE_HANDLER_H
#define LEVELDB_RDMA_WRITE_HANDLER_H

#include <vector>

#include "leveldb/db_types.h"
#include "common/nova_common.h"
#include "ltc/destination_migration.h"


namespace nova {
    class RDMAWriteHandler {
    public:
        RDMAWriteHandler(
                const std::vector<leveldb::DestinationMigration *> &destination_migration_threads);

        void Handle(char *buf, uint32_t size);

    private:
        std::vector<leveldb::DestinationMigration *> destination_migration_threads_;
    };
}


#endif //LEVELDB_RDMA_WRITE_HANDLER_H
