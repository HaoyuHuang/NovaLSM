
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_RDMA_DC_H
#define LEVELDB_RDMA_DC_H

#include "mc/nova_mem_manager.h"
#include "dc.h"

namespace nova {

    class RDMADiskComponent {
    public:
// Read the blocks and return the total size.
        uint64_t
        Read(const std::string &dbname, uint64_t file_number,
             const std::vector<leveldb::DCBlockHandle> &block_handls,
             char *buf);

        // Read the SSTable and return the total size.
        uint64_t
        Read(const std::string &dbname, uint64_t file_number, char *buf,
             uint64_t size);

        void FlushSSTable(const std::string &dbname, uint64_t file_number,
                          char *buf,
                          uint64_t table_size);

        uint64_t TableSize(const std::string &dbname, uint64_t file_number);
    };
}


#endif //LEVELDB_RDMA_DC_H
