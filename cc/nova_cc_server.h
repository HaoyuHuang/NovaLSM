
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_CC_SERVER_H
#define LEVELDB_NOVA_CC_SERVER_H

#include <semaphore.h>
#include <unordered_map>

#include "leveldb/db_types.h"
#include "db/table_cache.h"
#include "db/compaction.h"

#include "nova/nova_rdma_rc_store.h"
#include "mc/nova_mem_manager.h"
#include "log/nova_in_memory_log_manager.h"
#include "nova_rtable.h"
#include "nova_storage_worker.h"
#include "rdma_admission_ctrl.h"

namespace leveldb {
    class CompactionState;
}

namespace nova {

    struct NovaStorageTask {
        leveldb::CCRequestType request_type;
        uint32_t cc_server_thread_id = 0;
        uint32_t dc_req_id = 0;
        uint32_t remote_server_id = 0;
        uint32_t rtable_id = 0;

        // Read request
        leveldb::RTableHandle rtable_handle = {};
        char *rdma_buf = nullptr;
        uint64_t cc_mr_offset = 0;
        bool is_meta_blocks;

        // Persist request
        std::vector<leveldb::SSTableRTablePair> persist_pairs;

        // Compaction request
        leveldb::CompactionRequest *compaction_request = nullptr;
    };

    struct NovaServerCompleteTask {
        leveldb::CCRequestType request_type;
        int remote_server_id = -1;
        uint32_t dc_req_id = 0;

        // Read result.
        char *rdma_buf = nullptr;
        uint64_t cc_mr_offset = 0;
        leveldb::RTableHandle rtable_handle = {};
        // Persist result.
        std::vector<leveldb::RTableHandle> rtable_handles = {};
        leveldb::CompactionState *compaction_state = nullptr;
        leveldb::CompactionRequest *compaction_request = nullptr;
    };

    class NovaStorageWorker;

    struct RequestContext {
        leveldb::CCRequestType request_type;
        uint32_t remote_server_id;
        std::string db_name;
        uint32_t file_number;
        char *buf;
        uint32_t sstable_size;
        uint32_t rtable_id;
        uint64_t rtable_offset;
        uint32_t size;
        std::string sstable_id;
        bool is_meta_blocks;
    };

    class NovaCCServer : public NovaMsgCallback, public leveldb::CCServer {
    public:
        NovaCCServer(rdmaio::RdmaCtrl *rdma_ctrl,
                     NovaMemManager *mem_manager,
                     leveldb::NovaRTableManager *rtable_manager,
                     InMemoryLogFileManager *log_manager,
                     uint32_t thread_id, bool is_compaction_thread,
                     RDMAAdmissionCtrl *admission_control);

        bool
        ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id, int remote_server_id,
                      char *buf, uint32_t imm_data,
                      bool *generate_a_new_request) override;

        NovaRDMAStore *rdma_store_;

        void AddCompleteTasks(const std::vector<NovaServerCompleteTask> &tasks);

        void AddCompleteTask(const NovaServerCompleteTask &task);

        int ProcessCompletionQueue() override;

        std::vector<NovaStorageWorker *> fg_storage_workers_;
        std::vector<NovaStorageWorker *> bg_storage_workers_;
        std::vector<NovaStorageWorker *> compaction_storage_workers_;

        static std::atomic_int_fast32_t fg_storage_worker_seq_id_;
        static std::atomic_int_fast32_t bg_storage_worker_seq_id_;
        static std::atomic_int_fast32_t compaction_storage_worker_seq_id_;

    private:
        bool is_running_ = true;
        bool is_compaction_thread_ = false;

        void AddBGStorageTask(const NovaStorageTask &task);

        void AddFGStorageTask(const NovaStorageTask &task);

        void AddCompactionStorageTask(const NovaStorageTask &task);

        uint32_t thread_id_;
        rdmaio::RdmaCtrl *rdma_ctrl_;
        NovaMemManager *mem_manager_;
        InMemoryLogFileManager *log_manager_;
        leveldb::NovaRTableManager *rtable_manager_;
        leveldb::NovaRTable *current_rtable_ = nullptr;
        RDMAAdmissionCtrl *admission_control_ = nullptr;
        std::mutex mutex_;
        std::list<NovaServerCompleteTask> private_cq_;
        std::list<NovaServerCompleteTask> public_cq_;

        uint32_t current_worker_id_ = 0;
        std::unordered_map<uint64_t, RequestContext> request_context_map_;
    };
}


#endif //LEVELDB_NOVA_CC_SERVER_H
