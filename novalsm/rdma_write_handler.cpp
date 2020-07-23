
//
// Created by Haoyu Huang on 6/20/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "rdma_write_handler.h"

namespace nova {
    RDMAWriteHandler::RDMAWriteHandler(
            const std::vector<leveldb::DestinationMigration *> &destination_migration_threads)
            : destination_migration_threads_(destination_migration_threads) {}

    void RDMAWriteHandler::Handle(char *buf, uint32_t size) {
        NOVA_ASSERT(buf[0] == leveldb::StoCRequestType::LTC_MIGRATION);
        int value = leveldb::DestinationMigration::migration_seq_id_ %
                    destination_migration_threads_.size();
        leveldb::DestinationMigration::migration_seq_id_ += 1;
        destination_migration_threads_[value]->AddReceivedDBId(buf, size);
    }
}