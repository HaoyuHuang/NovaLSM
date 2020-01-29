
//
// Created by Haoyu Huang on 1/11/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_CC_CLIENT_H
#define LEVELDB_CC_CLIENT_H

#include <cstdint>
#include <vector>
#include <infiniband/verbs.h>
#include "db_types.h"

namespace leveldb {
    struct CCBlockHandle {
        uint64_t offset;
        uint64_t size;
    };

    enum CCRequestType : char {
        CC_RTABLE_READ_BLOCKS = 'a',
        CC_READ_BLOCKS = 'b',
        CC_READ_SSTABLE = 'c',
        CC_FLUSH_SSTABLE = 'd',
        CC_WRITE_REPLICATE_SSTABLE = 'e',
        CC_FLUSH_SSTABLE_BUF = 'f',
        CC_FLUSH_SSTABLE_SUCC = 'g',
        CC_ALLOCATE_SSTABLE_BUFFER = 'h',
        CC_ALLOCATE_SSTABLE_BUFFER_SUCC = 'i',
        CC_RELEASE_SSTABLE_BUFFER = 'j',
        CC_ALLOCATE_LOG_BUFFER = 'k',
        CC_ALLOCATE_LOG_BUFFER_SUCC = 'l',
        CC_DELETE_LOG_FILE = 'm',
        CC_DELETE_LOG_FILE_SUCC = 'n',
        CC_DELETE_TABLES = 'o',
        CC_RTABLE_READ_SSTABLE = 'p',
        CC_RTABLE_WRITE_SSTABLE = 'q',
        CC_RTABLE_PERSIST = 'r',
        CC_RTABLE_WRITE_SSTABLE_RESPONSE = 's',
    };

    struct CCRequestContext {
        CCRequestType req_type;
        uint32_t remote_server_id;
        std::string dbname;
        uint64_t file_number;
        char *backing_mem;
        uint32_t size;
        bool done;

        uint64_t wr_id = 0;
        uint32_t rtable_id = 0;
        std::vector<RTableHandle> rtable_handles;
    };

    struct CCResponse {
        uint32_t rtable_id = 0;
        std::vector<RTableHandle> rtable_handles;
    };

    struct SSTableRTablePair {
        std::string sstable_id;
        uint32_t rtable_id;
    };

    class LEVELDB_EXPORT CCClient {
    public:
        virtual uint32_t
        InitiateRTableReadDataBlock(const RTableHandle &rtable_handle,
                                    char *result) = 0;

        virtual uint32_t
        InitiateRTableReadSSTableDataBlock(uint32_t server_id,
                                           const std::string &dbname,
                                           uint64_t file_number,
                                           uint32_t size,
                                           char *result) = 0;

        virtual uint32_t
        InitiateRTableWriteDataBlocks(uint32_t server_id, uint32_t thread_id, uint32_t *rtable_id,
                                      char *buf,
                                      const std::string &dbname,
                                      uint64_t file_number, uint32_t size) = 0;

        virtual uint32_t
        InitiatePersist(uint32_t server_id,
                        std::vector<SSTableRTablePair> rtable_ids) = 0;


        virtual uint32_t
        InitiateDeleteTables(uint32_t server_id, const std::string &dbname,
                             const std::vector<uint64_t> &filenumbers) = 0;

        virtual uint32_t
        InitiateReplicateLogRecords(const std::string &log_file_name,
                                    uint64_t thread_id,
                                    const Slice &slice) = 0;


        virtual uint32_t
        InitiateCloseLogFile(const std::string &log_file_name) = 0;

        virtual void OnRecv(ibv_wc_opcode type, uint64_t wr_id,
                            int remote_server_id, char *buf,
                            uint32_t imm_data) = 0;

        virtual bool IsDone(uint32_t req_id, CCResponse *response) = 0;
    };
}


#endif //LEVELDB_CC_CLIENT_H
