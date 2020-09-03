
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
// StoCBlockingClient uses a semaphore to wait for the response after issuing a request.
// StoCRDMAClient is based on RDMA.

#ifndef LEVELDB_STOC_CLIENT_IMPL_H
#define LEVELDB_STOC_CLIENT_IMPL_H

#include <semaphore.h>
#include <unordered_map>

#include "util/env_mem.h"
#include "db/version_edit.h"
#include "leveldb/db_types.h"
#include "leveldb/stoc_client.h"
#include "common/nova_common.h"
#include "rdma/nova_rdma_rc_broker.h"
#include "log/logc_log_writer.h"

#include "log/stoc_log_manager.h"
#include "stoc/persistent_stoc_file.h"
#include "novalsm/rdma_msg_handler.h"

namespace nova {
    class RDMAMsgHandler;
}

namespace leveldb {

    class StoCBlockClient : public StoCClient {
    public:
        StoCBlockClient(uint32_t client_id,
                        StocPersistentFileManager *stoc_file_manager);

        uint32_t InitiateReplicateSSTables(uint32_t stoc_server_id,
                                           const std::string& dbname,
                                           const std::vector<leveldb::ReplicationPair> &pairs) override;

        uint32_t
        InitiateIsReadyForProcessingRequests(
                uint32_t remote_server_id) override;

        uint32_t
        InitiateRDMAWRITE(uint32_t remote_server_id, char *data,
                          uint32_t size) override;

        uint32_t InitiateCompaction(uint32_t remote_server_id,
                                    CompactionRequest *compaction_request) override;

        uint32_t
        InitiateDeleteTables(uint32_t server_id,
                             const std::vector<SSTableStoCFilePair> &stoc_file_ids) override;

        uint32_t
        InitiateReadDataBlock(const StoCBlockHandle &block_handle,
                              uint64_t offset, uint32_t size,
                              char *result,
                              uint32_t result_size,
                              std::string filename,
                              bool is_foreground_reads) override;

        uint32_t
        InitiateInstallFileNameStoCFileMapping(uint32_t stoc_id,
                                               const std::unordered_map<std::string, uint32_t> &fn_stocfnid) override;

        uint32_t
        InitiateQueryLogFile(uint32_t stoc_id, uint32_t server_id,
                             uint32_t dbid,
                             std::unordered_map<std::string, uint64_t> *logfile_offset) override;

        uint32_t
        InitiateReadInMemoryLogFile(char *local_buf, uint32_t stoc_id,
                                    uint64_t remote_offset,
                                    uint64_t size) override;

        // file_number = 0 means manifest file.
        uint32_t
        InitiateAppendBlock(uint32_t stoc_id,
                            uint32_t thread_id,
                            uint32_t *stoc_file_id, char *buf,
                            const std::string &dbname,
                            uint64_t file_number,
                            uint32_t replica_id,
                            uint32_t size,
                            FileInternalType internal_type) override;

        uint32_t
        InitiateReplicateLogRecords(const std::string &log_file_name,
                                    uint64_t thread_id,
                                    uint32_t db_id,
                                    uint32_t memtable_id,
                                    char *rdma_backing_mem,
                                    const std::vector<LevelDBLogRecord> &log_records,
                                    StoCReplicateLogRecordState *replicate_log_record_states) override;


        uint32_t
        InitiateCloseLogFiles(const std::vector<std::string> &log_file_name,
                              uint32_t dbid) override;

        uint32_t InitiateReadStoCStats(uint32_t stoc_id) override;

        bool OnRecv(ibv_wc_opcode type, uint64_t wr_id,
                    int remote_server_id, char *buf,
                    uint32_t imm_data, bool *generate_a_new_request) override {
            NOVA_ASSERT(false);
            return false;
        };

        bool
        IsDone(uint32_t req_id, StoCResponse *response,
               uint64_t *timeout) override;

        std::vector<nova::RDMAMsgHandler *> rdma_msg_handlers_;

        sem_t Wait() {
            NOVA_ASSERT(sem_wait(&sem_) == 0);
        }

        static std::atomic_int_fast32_t rdma_worker_seq_id_;
        sem_t sem_;

        void IncrementReqId();

    private:
        std::unordered_map<uint32_t, StoCResponse *> req_response;

        void AddAsyncTask(const RDMARequestTask &task);

        StocPersistentFileManager *stoc_file_manager_;
        uint32_t current_rdma_msg_handler_id_ = 0;
        uint32_t req_id_ = 1;
    };

    class StoCRDMAClient : public StoCClient {
    public:
        StoCRDMAClient(uint32_t stoc_client_id,
                       nova::NovaRDMABroker *rdma_broker,
                       nova::NovaMemManager *mem_manager,
                       leveldb::LogCLogWriter *rdma_log_writer,
                       uint32_t lower_req_id, uint32_t upper_req_id,
                       RDMAServer *rdma_server)
                : stoc_client_id_(stoc_client_id), rdma_broker_(rdma_broker),
                  mem_manager_(mem_manager),
                  rdma_log_writer_(rdma_log_writer),
                  lower_req_id_(lower_req_id), upper_req_id_(upper_req_id),
                  current_req_id_(lower_req_id), rdma_server_(rdma_server) {
        }

        uint32_t InitiateReplicateSSTables(uint32_t stoc_server_id,
                                           const std::string&  dbname,
                                           const std::vector<leveldb::ReplicationPair> &pairs) override;

        uint32_t
        InitiateRDMAWRITE(uint32_t remote_server_id, char *data,
                          uint32_t size) override;

        uint32_t
        InitiateIsReadyForProcessingRequests(uint32_t stoc_id) override;

        uint32_t InitiateCompaction(uint32_t stoc_id,
                                    CompactionRequest *compaction_request) override;

        uint32_t
        InitiateInstallFileNameStoCFileMapping(uint32_t stoc_id,
                                               const std::unordered_map<std::string, uint32_t> &fn_stocid) override;

        uint32_t
        InitiateDeleteTables(uint32_t stoc_id,
                             const std::vector<SSTableStoCFilePair> &stoc_fileids) override;

        uint32_t
        InitiateReadDataBlock(const StoCBlockHandle &block_handle,
                              uint64_t offset, uint32_t size,
                              char *result,
                              uint32_t result_size,
                              std::string filename,
                              bool is_foreground_reads) override;

        uint32_t
        InitiateReadInMemoryLogFile(char *local_buf, uint32_t stoc_id,
                                    uint64_t remote_offset,
                                    uint64_t size) override;

        uint32_t InitiateQueryLogFile(
                uint32_t stoc_id,
                uint32_t server_id,
                uint32_t dbid,
                std::unordered_map<std::string, uint64_t> *logfile_offset) override;

        uint32_t
        InitiateAppendBlock(uint32_t stoc_id, uint32_t thread_id,
                            uint32_t *stoc_file_id, char *buf,
                            const std::string &dbname,
                            uint64_t file_number,
                            uint32_t replica_id,
                            uint32_t size,
                            FileInternalType internal_type) override;

        uint32_t
        InitiateReplicateLogRecords(const std::string &log_file_name,
                                    uint64_t thread_id,
                                    uint32_t db_id,
                                    uint32_t memtable_id,
                                    char *rdma_backing_mem,
                                    const std::vector<LevelDBLogRecord> &log_records,
                                    StoCReplicateLogRecordState *replicate_log_record_states) override;

        uint32_t InitiateReadStoCStats(uint32_t server_id) override;

        uint32_t
        InitiateCloseLogFiles(const std::vector<std::string> &log_file_name,
                              uint32_t dbid) override;

        bool OnRecv(ibv_wc_opcode type, uint64_t wr_id,
                    int remote_server_id, char *buf,
                    uint32_t imm_data, bool *generate_a_new_request) override;

        bool IsDone(uint32_t req_id, StoCResponse *response,
                    uint64_t *timeout) override;

        uint32_t GetCurrentReqId();

        void IncrementReqId();

        nova::RDMAMsgHandler *rdma_msg_handler_ = nullptr;
    private:
        uint32_t stoc_client_id_ = 0;
        nova::NovaRDMABroker *rdma_broker_;
        nova::NovaMemManager *mem_manager_;
        RDMAServer *rdma_server_;
        leveldb::LogCLogWriter *rdma_log_writer_ = nullptr;

        uint32_t current_req_id_ = 1;
        uint32_t lower_req_id_;
        uint32_t upper_req_id_;
        std::unordered_map<uint32_t, StoCRequestContext> request_context_;
    };
}


#endif //LEVELDB_STOC_CLIENT_IMPL_H
