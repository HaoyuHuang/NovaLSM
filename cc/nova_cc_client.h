
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_CC_CLIENT_H
#define LEVELDB_NOVA_CC_CLIENT_H

#include <semaphore.h>
#include <unordered_map>

#include "util/env_mem.h"
#include "db/version_edit.h"
#include "leveldb/db_types.h"
#include "leveldb/cc_client.h"
#include "nova/nova_common.h"
#include "nova/nova_rdma_rc_store.h"
#include "cc/nova_cc_log_writer.h"

#include "log/nova_in_memory_log_manager.h"
#include "nova_rtable.h"
#include "nova_rdma_cc.h"

namespace nova {
    class NovaRDMAComputeComponent;
}

namespace leveldb {

    class NovaBlockCCClient : public CCClient {
    public:
        NovaBlockCCClient(uint32_t client_id,
                          NovaRTableManager *rtable_manager);

        uint32_t
        InitiateIsReadyForProcessingRequests(uint32_t remote_server_id) override;

        uint32_t InitiateCompaction(uint32_t remote_server_id,
                                    CompactionRequest *compaction_request) override;

        uint32_t
        InitiateDeleteTables(uint32_t server_id,
                             const std::vector<SSTableRTablePair> &rtable_ids) override;

        uint32_t
        InitiateRTableReadDataBlock(const RTableHandle &rtable_handle,
                                    uint64_t offset, uint32_t size,
                                    char *result,
                                    uint32_t result_size,
                                    std::string filename,
                                    bool is_foreground_reads) override;

        uint32_t
        InitiateFileNameRTableMapping(uint32_t stoc_id,
                                      const std::unordered_map<std::string, uint32_t> &fn_rtableid) override;

        uint32_t
        InitiateQueryLogFile(uint32_t storage_server_id, uint32_t server_id,
                             uint32_t dbid,
                             std::unordered_map<std::string, uint64_t> *logfile_offset) override;

        uint32_t
        InitiateReadInMemoryLogFile(char *local_buf, uint32_t remote_server_id,
                                    uint64_t remote_offset,
                                    uint64_t size) override;

        uint32_t
        InitiateRTableWriteDataBlocks(uint32_t server_id, uint32_t thread_id,
                                      uint32_t *rtable_id, char *buf,
                                      const std::string &dbname,
                                      uint64_t file_number,
                                      uint32_t size,
                                      bool is_meta_blocks) override;

        uint32_t
        InitiateReplicateLogRecords(const std::string &log_file_name,
                                    uint64_t thread_id,
                                    uint32_t db_id,
                                    uint32_t memtable_id,
                                    char *rdma_backing_mem,
                                    const std::vector<LevelDBLogRecord> &log_records,
                                    WriteState *replicate_log_record_states) override;


        uint32_t
        InitiateCloseLogFiles(const std::vector<std::string> &log_file_name,
                             uint32_t dbid) override;

        uint32_t InitiateReadDCStats(uint32_t server_id) override;

        bool OnRecv(ibv_wc_opcode type, uint64_t wr_id,
                    int remote_server_id, char *buf,
                    uint32_t imm_data, bool* generate_a_new_request) override {
            RDMA_ASSERT(false);
            return false;
        };

        bool
        IsDone(uint32_t req_id, CCResponse *response,
               uint64_t *timeout) override;

        std::vector<nova::NovaRDMAComputeComponent *> ccs_;

        void set_dbid(uint32_t dbid) {
            dbid_ = dbid;
        }

        sem_t Wait() {
            RDMA_ASSERT(sem_wait(&sem_) == 0);
        }

        static std::atomic_int_fast32_t rdma_worker_seq_id_;
        sem_t sem_;

        void IncrementReqId();
    private:
        std::unordered_map<uint32_t, CCResponse *> req_response;

        void AddAsyncTask(const RDMAAsyncClientRequestTask &task);

        NovaRTableManager *rtable_manager_;
        uint32_t current_cc_id_ = 0;
        uint32_t req_id_ = 1;
        uint32_t dbid_ = 0;

    };


    class NovaCCClient : public CCClient {
    public:
        NovaCCClient(uint32_t dc_client_id, nova::NovaRDMAStore *rdma_store,
                     nova::NovaMemManager *mem_manager,
                     leveldb::RDMALogWriter *rdma_log_writer,
                     uint32_t lower_req_id, uint32_t upper_req_id,
                     CCServer *cc_server)
                : cc_client_id_(dc_client_id), rdma_store_(rdma_store),
                  mem_manager_(mem_manager),
                  rdma_log_writer_(rdma_log_writer),
                  lower_req_id_(lower_req_id), upper_req_id_(upper_req_id),
                  current_req_id_(lower_req_id), cc_server_(cc_server) {
        }

        uint32_t
        InitiateIsReadyForProcessingRequests(uint32_t remote_server_id) override;

        uint32_t InitiateCompaction(uint32_t remote_server_id,
                                    CompactionRequest *compaction_request) override;

        uint32_t
        InitiateFileNameRTableMapping(uint32_t stoc_id,
                                      const std::unordered_map<std::string, uint32_t> &fn_rtableid) override;

        uint32_t
        InitiateDeleteTables(uint32_t server_id,
                             const std::vector<SSTableRTablePair> &rtable_ids) override;

        uint32_t
        InitiateRTableReadDataBlock(const RTableHandle &rtable_handle,
                                    uint64_t offset, uint32_t size,
                                    char *result,
                                    uint32_t result_size,
                                    std::string filename,
                                    bool is_foreground_reads) override;

        uint32_t
        InitiateReadInMemoryLogFile(char *local_buf, uint32_t remote_server_id,
                                    uint64_t remote_offset,
                                    uint64_t size) override;

        uint32_t InitiateQueryLogFile(
                uint32_t storage_server_id,
                uint32_t server_id,
                uint32_t dbid,
                std::unordered_map<std::string, uint64_t> *logfile_offset) override;

        uint32_t
        InitiateRTableWriteDataBlocks(uint32_t server_id, uint32_t thread_id,
                                      uint32_t *rtable_id, char *buf,
                                      const std::string &dbname,
                                      uint64_t file_number,
                                      uint32_t size,
                                      bool is_meta_blocks) override;

        uint32_t
        InitiateReplicateLogRecords(const std::string &log_file_name,
                                    uint64_t thread_id,
                                    uint32_t db_id,
                                    uint32_t memtable_id,
                                    char *rdma_backing_mem,
                                    const std::vector<LevelDBLogRecord> &log_records,
                                    WriteState *replicate_log_record_states) override;

        uint32_t InitiateReadDCStats(uint32_t server_id) override;

        uint32_t
        InitiateCloseLogFiles(const std::vector<std::string> &log_file_name,
                             uint32_t dbid) override;

        bool OnRecv(ibv_wc_opcode type, uint64_t wr_id,
                    int remote_server_id, char *buf,
                    uint32_t imm_data, bool* generate_a_new_request) override;

        bool IsDone(uint32_t req_id, CCResponse *response,
                    uint64_t *timeout) override;

        uint32_t GetCurrentReqId();

        void IncrementReqId();

        nova::NovaRDMAComputeComponent *cc_ = nullptr;
    private:
        uint32_t cc_client_id_ = 0;
        nova::NovaRDMAStore *rdma_store_;
        nova::NovaMemManager *mem_manager_;
        CCServer *cc_server_;
        leveldb::RDMALogWriter *rdma_log_writer_ = nullptr;

        uint32_t current_req_id_ = 1;
        uint32_t lower_req_id_;
        uint32_t upper_req_id_;
        std::unordered_map<uint32_t, CCRequestContext> request_context_;

    };
}


#endif //LEVELDB_NOVA_CC_CLIENT_H
