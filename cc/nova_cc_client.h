
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_CC_CLIENT_H
#define LEVELDB_NOVA_CC_CLIENT_H

#include "util/env_mem.h"
#include "db/version_edit.h"
#include "include/leveldb/db_types.h"
#include "include/leveldb/cc_client.h"
#include "nova/nova_common.h"
#include "dc/nova_dc.h"
#include "nova/nova_rdma_rc_store.h"
#include "cc/nova_cc_log_writer.h"

#include "log/nova_log.h"

namespace leveldb {

    class NovaCCClient : public CCClient {
    public:
        NovaCCClient(uint32_t dc_client_id, nova::NovaRDMAStore *rdma_store,
                     nova::NovaMemManager *mem_manager,
                     leveldb::log::RDMALogWriter *rdma_log_writer)
                : cc_client_id_(dc_client_id), rdma_store_(rdma_store),
                  mem_manager_(mem_manager),
                  rdma_log_writer_(rdma_log_writer) {}

        uint32_t
        InitiateReadBlocks(const std::string &dbname, uint64_t file_number,
                           const FileMetaData &meta,
                           const std::vector<CCBlockHandle> &block_handls,
                           char *result) override;

        uint32_t
        InitiateReadBlock(const std::string &dbname, uint64_t file_number,
                          const FileMetaData &meta,
                          const CCBlockHandle &block_handle,
                          char *result) override;

        // Read the SSTable and return the total size.
        uint32_t
        InitiateReadSSTable(const std::string &dbname, uint64_t file_number,
                            const FileMetaData &meta, char *result) override;

        uint32_t InitiateFlushSSTable(const std::string &dbname,
                                      uint64_t file_number,
                                      const FileMetaData &meta,
                                      char *backing_mem) override;

        uint32_t
        InitiateAllocateSSTableBuffer(uint32_t remote_server_id,
                                      const std::string &dbname,
                                      uint64_t file_number,
                                      uint64_t file_size) override;

        uint32_t
        InitiateWRITESSTableBuffer(uint32_t remote_server_id, char *src,
                                   uint64_t dest,
                                   uint64_t file_size) override;

        uint32_t InitiateReleaseSSTableBuffer(uint32_t remote_server_id,
                                              const std::string &dbname,
                                              uint64_t file_number,
                                              uint64_t file_size) override;

        uint32_t InitiateDeleteTables(const std::string &dbname,
                                      const std::vector<uint64_t> &filenumbers) override;

        uint32_t
        InitiateDeleteFiles(const std::string &dbname,
                            const std::vector<FileMetaData> &filenames) override;

        uint32_t
        InitiateReplicateLogRecords(const std::string &log_file_name,
                                    const Slice &slice) override;


        uint32_t
        InitiateCloseLogFile(const std::string &log_file_name) override;

        void OnRecv(ibv_wc_opcode type, uint64_t wr_id,
                    int remote_server_id, char *buf,
                    uint32_t imm_data) override;

        bool IsDone(uint32_t req_id, CCResponse *response) override;

        uint32_t GetCurrentReqId();

        void IncrementReqId();

    private:
        uint32_t cc_client_id_ = 0;
        nova::NovaRDMAStore *rdma_store_;
        nova::NovaMemManager *mem_manager_;
        leveldb::log::RDMALogWriter *rdma_log_writer_ = nullptr;

        leveldb::SSTableManager *sstable_manager_;

        uint32_t HomeCCNode(const FileMetaData &meta);

        uint32_t current_req_id_ = 1;
        std::map<uint32_t, CCRequestContext> request_context_;
    };
}


#endif //LEVELDB_NOVA_CC_CLIENT_H
