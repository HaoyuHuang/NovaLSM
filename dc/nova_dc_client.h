
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_DC_CLIENT_H
#define LEVELDB_NOVA_DC_CLIENT_H

#include "util/env_mem.h"
#include "db/version_edit.h"
#include "nova/nova_common.h"
#include "nova_dc.h"
#include "nova/nova_rdma_rc_store.h"
#include "log/rdma_log_writer.h"

namespace leveldb {

    enum DCRequestType : char {
        DC_READ_BLOCKS = 'r',
        DC_READ_SSTABLE = 's',
        DC_FLUSH_SSTABLE = 'f',
        DC_FLUSH_SSTABLE_BUF = 'F',
        DC_FLUSH_SSTABLE_SUCC = 'S',
        DC_ALLOCATE_LOG_BUFFER = 'a',
        DC_ALLOCATE_LOG_BUFFER_SUCC = 'A',
        DC_DELETE_LOG_FILE = 'd',
        DC_DELETE_LOG_FILE_SUCC = 'D',
    };

    struct DCRequestContext {
        std::string dbname;
        uint64_t file_number;
        FileMetaData meta;
        MemWritableFile *sstable;
        bool done;
    };

    class NovaDCClient {
    public:
        uint32_t
        InitiateReadBlocks(const std::string &dbname, uint64_t file_number,
                           const FileMetaData &meta,
                           const std::vector<DCBlockHandle> &block_handls,
                           BlockContents *results);

        // Read the SSTable and return the total size.
        uint32_t
        InitiateReadSSTable(const std::string &dbname, uint64_t file_number,
                            const FileMetaData &meta,
                            uint32_t table_size,
                            RandomAccessFile **table_reader);

        void InitiateFlushSSTable(const std::string &dbname,
                                  uint64_t file_number,
                                  const FileMetaData &meta,
                                  WritableFile *sstable);

        void OnRecv(ibv_wc_opcode type, uint64_t wr_id,
                    int remote_server_id, char *buf,
                    uint32_t imm_data);


        bool IsDone(uint32_t req_id);

    private:
        nova::NovaRDMAStore *rdma_store_;
        nova::NovaMemManager *mem_manager_;
        leveldb::log::RDMALogWriter *rdma_log_writer_ = nullptr;

        uint32_t HomeDCNode(const FileMetaData &meta);

        uint32_t current_req_id_ = 0;
        std::map<uint32_t, DCRequestContext> request_context_;
    };
}


#endif //LEVELDB_NOVA_DC_CLIENT_H
