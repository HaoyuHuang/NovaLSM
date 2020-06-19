
//
// Created by Haoyu Huang on 6/20/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_RDMA_WRITE_HANDLER_H
#define LEVELDB_RDMA_WRITE_HANDLER_H

#include "leveldb/db_types.h"
#include "common/nova_common.h"

namespace nova {
    class RDMAWriteHandler {
    public:
        void Handle(char *buf, uint32_t size);

    private:
    };
}




#endif //LEVELDB_RDMA_WRITE_HANDLER_H
