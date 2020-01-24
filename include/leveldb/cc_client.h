
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
        CC_READ_BLOCKS = 'r',
        CC_READ_SSTABLE = 's',
        CC_FLUSH_SSTABLE = 'f',
        CC_WRITE_REPLICATE_SSTABLE = 'w',
        CC_FLUSH_SSTABLE_BUF = 'F',
        CC_FLUSH_SSTABLE_SUCC = 'S',
        CC_ALLOCATE_SSTABLE_BUFFER = 'b',
        CC_ALLOCATE_SSTABLE_BUFFER_SUCC = 'c',
        CC_RELEASE_SSTABLE_BUFFER = 'B',
        CC_ALLOCATE_LOG_BUFFER = 'a',
        CC_ALLOCATE_LOG_BUFFER_SUCC = 'A',
        CC_DELETE_LOG_FILE = 'd',
        CC_DELETE_LOG_FILE_SUCC = 'D',
        CC_DELETE_TABLES = 't'
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
        uint64_t remote_sstable_buf = 0;
    };

    struct CCResponse {
        uint64_t remote_sstable_buf;
    };

    class LEVELDB_EXPORT CCClient {
    public:
        virtual uint32_t
        InitiateReadBlocks(const std::string &dbname, uint64_t file_number,
                           const FileMetaData &meta,
                           const std::vector<CCBlockHandle> &block_handls,
                           char *result) = 0;

        virtual uint32_t
        InitiateReadBlock(const std::string &dbname, uint64_t file_number,
                          const FileMetaData &meta,
                          const CCBlockHandle &block_handle,
                          char *result) = 0;

        // Read the SSTable and return the total size.
        virtual uint32_t
        InitiateReadSSTable(const std::string &dbname, uint64_t file_number,
                            const FileMetaData &meta, char *result) = 0;

        virtual uint32_t InitiateFlushSSTable(const std::string &dbname,
                                              uint64_t file_number,
                                              const FileMetaData &meta,
                                              char *backing_mem) = 0;

        virtual uint32_t
        InitiateAllocateSSTableBuffer(uint32_t remote_server_id,
                                      const std::string &dbname,
                                      uint64_t file_number,
                                      uint64_t file_size) = 0;

        virtual uint32_t
        InitiateWRITESSTableBuffer(uint32_t remote_server_id, char *src,
                                   uint64_t dest,
                                   uint64_t file_size) = 0;

        virtual uint32_t InitiateReleaseSSTableBuffer(uint32_t remote_server_id,
                                                      const std::string &dbname,
                                                      uint64_t file_number,
                                                      uint64_t file_size) = 0;

        virtual uint32_t InitiateDeleteTables(const std::string &dbname,
                                              const std::vector<uint64_t> &filenumbers) = 0;

        virtual uint32_t
        InitiateDeleteFiles(const std::string &dbname,
                            const std::vector<FileMetaData> &filenames) = 0;

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
