
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.

#ifndef RDMA_SERVER_H
#define RDMA_SERVER_H

#include <semaphore.h>
#include <unordered_map>

#include "leveldb/db_types.h"
#include "db/table_cache.h"
#include "db/compaction.h"

#include "rdma/nova_rdma_rc_broker.h"
#include "common/nova_mem_manager.h"
#include "log/stoc_log_manager.h"
#include "stoc/persistent_stoc_file.h"
#include "stoc/storage_worker.h"
#include "rdma_admission_ctrl.h"

#include "ltc/db_migration.h"

namespace leveldb {
    class CompactionState;
}

namespace nova {
    class DBMigration;
    struct StorageTask {
        leveldb::StoCRequestType request_type;
        uint32_t rdma_server_thread_id = 0;
        uint32_t stoc_req_id = 0;
        uint32_t remote_server_id = 0;
        uint32_t stoc_file_id = 0;

        // Read request
        leveldb::StoCBlockHandle stoc_block_handle = {};
        char *rdma_buf = nullptr;
        uint64_t ltc_mr_offset = 0;
        leveldb::FileInternalType internal_type;

        // Persist request
        std::vector<leveldb::SSTableStoCFilePair> persist_pairs;

        // Compaction request
        leveldb::CompactionRequest *compaction_request = nullptr;

        // Replication request
        std::string dbname;
        std::vector<leveldb::ReplicationPair> replication_pairs;
    };

    struct ServerCompleteTask {
        leveldb::StoCRequestType request_type;
        int remote_server_id = -1;
        uint32_t stoc_req_id = 0;

        uint32_t stoc_file_id = 0;
        uint64_t stoc_file_buf_offset = 0;

        // Read result.
        char *rdma_buf = nullptr;
        uint32_t size = 0;
        uint64_t ltc_mr_offset = 0;
        leveldb::StoCBlockHandle stoc_block_handle = {};
        // Persist result.
        std::vector<leveldb::StoCBlockHandle> stoc_block_handles = {};
        leveldb::CompactionState *compaction_state = nullptr;
        leveldb::CompactionRequest *compaction_request = nullptr;
        std::vector<leveldb::ReplicationPair> replication_results = {};
    };

    class StorageWorker;

    struct RequestContext {
        leveldb::StoCRequestType request_type;
        uint32_t remote_server_id;
        char *buf;
        uint32_t stoc_file_id;
        uint64_t stoc_file_buf_offset;
        uint32_t size;
        std::string sstable_name;
        leveldb::FileInternalType internal_type;
    };

    class RDMAWriteHandler {
    public:
        RDMAWriteHandler(
                const std::vector<DBMigration *> &destination_migration_threads);

        void Handle(char *buf, uint32_t size);

    private:
        std::vector<DBMigration *> destination_migration_threads_;
    };

    // RDMA server class that handles RDMA client requests.
    class RDMAServerImpl : public RDMAMsgCallback, public leveldb::RDMAServer {
    public:
        RDMAServerImpl(rdmaio::RdmaCtrl *rdma_ctrl,
                       NovaMemManager *mem_manager,
                       leveldb::StocPersistentFileManager *stoc_file_manager,
                       StoCInMemoryLogFileManager *log_manager,
                       uint32_t thread_id, bool is_compaction_thread,
                       RDMAAdmissionCtrl *admission_control);

        bool
        ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id, int remote_server_id,
                      char *buf, uint32_t imm_data,
                      bool *generate_a_new_request) override;

        NovaRDMABroker *rdma_broker_;

        void AddCompleteTasks(const std::vector<ServerCompleteTask> &tasks);

        void AddCompleteTask(const ServerCompleteTask &task);

        int ProcessCompletionQueue() override;

        std::vector<StorageWorker *> fg_storage_workers_;
        std::vector<StorageWorker *> bg_storage_workers_;
        std::vector<StorageWorker *> compaction_storage_workers_;
        RDMAWriteHandler *rdma_write_handler_ = nullptr;

        static std::atomic_int_fast32_t fg_storage_worker_seq_id_;
        static std::atomic_int_fast32_t bg_storage_worker_seq_id_;
        static std::atomic_int_fast32_t compaction_storage_worker_seq_id_;
    private:
        bool is_running_ = true;
        bool is_compaction_thread_ = false;

        void AddBGStorageTask(const StorageTask &task);

        void AddFGStorageTask(const StorageTask &task);

        void AddCompactionStorageTask(const StorageTask &task);

        uint32_t thread_id_;
        rdmaio::RdmaCtrl *rdma_ctrl_;
        NovaMemManager *mem_manager_;
        StoCInMemoryLogFileManager *log_manager_;
        leveldb::StocPersistentFileManager *stoc_file_manager_;
        RDMAAdmissionCtrl *admission_control_ = nullptr;
        std::mutex mutex_;
        std::list<ServerCompleteTask> private_cq_;
        std::list<ServerCompleteTask> public_cq_;

        uint32_t current_worker_id_ = 0;
        std::unordered_map<uint64_t, RequestContext> request_context_map_;
    };
}


#endif //RDMA_SERVER_H
