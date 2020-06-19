
//
// Created by Haoyu Huang on 6/20/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "rdma_write_handler.h"
#include "db/db_impl.h"

namespace nova {
    void RDMAWriteHandler::Handle(char *buf, uint32_t size) {
        NOVA_ASSERT(buf[0] == leveldb::StoCRequestType::LTC_MIGRATION);

//        leveldb::DB::Open

    }
}