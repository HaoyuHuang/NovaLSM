
//
// Created by Haoyu Huang on 12/25/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <leveldb/write_batch.h>
#include "nova_rdma_cc.h"
#include "nova/nova_config.h"
#include "nova/nova_common.h"

namespace nova {

    static void DeleteEntry(const leveldb::Slice &key, void *value) {
        CacheValue *tf = reinterpret_cast<CacheValue *>(value);
        delete tf;
    }

    void NovaRDMAComputeComponent::AddTask(
            const leveldb::RDMAAsyncClientRequestTask &task) {
        mutex_.Lock();
        public_queue_.push_back(task);
        mutex_.Unlock();
    }

    int NovaRDMAComputeComponent::size() {
        mutex_.Lock();
        int size = public_queue_.size();
        mutex_.Unlock();
        return size;
    }

    int NovaRDMAComputeComponent::ProcessRequestQueue() {
        {
            auto it = pending_reqs_.begin();
            while (it != pending_reqs_.end()) {
                if (cc_client_->IsDone(it->req_id, it->response, nullptr)) {
                    if (it->sem) {
                        RDMA_LOG(rdmaio::DEBUG)
                            << fmt::format("Wake up request: {}", it->req_id);
                        RDMA_ASSERT(sem_post(it->sem) == 0);
                    }
                    it = pending_reqs_.erase(it);
                } else {
                    it++;
                }
            }
        }

        mutex_.Lock();
        while (!public_queue_.empty()) {
            private_queue_.push_back(public_queue_.front());
            public_queue_.pop_front();
        }
        mutex_.Unlock();

        stat_tasks_ += private_queue_.size();
        uint32_t size = private_queue_.size();
        auto it = private_queue_.begin();
        while (it != private_queue_.end()) {
            const auto &task = *it;
            if (task.server_id != -1 &&
                !admission_control_->CanIssueRequest(task.server_id)) {
                it++;
                continue;
            }
            if (task.server_id == -1) {
                // A log record request.
                RDMA_ASSERT(task.type == leveldb::RDMA_ASYNC_REQ_CLOSE_LOG ||
                            task.type == leveldb::RDMA_ASYNC_REQ_LOG_RECORD);
                std::vector<int> serverids;
                nova::CCFragment *frag = nova::NovaConfig::config->db_fragment[task.dbid];
                for (int i = 0; i < frag->log_replica_stoc_ids.size(); i++) {
                    uint32_t stoc_server_id = nova::NovaConfig::config->dc_servers[frag->log_replica_stoc_ids[i]].server_id;
                    serverids.push_back(stoc_server_id);
                }
                if (!admission_control_->CanIssueRequest(serverids)) {
                    it++;
                    continue;
                }
            }

            RequestCtx ctx = {};
            ctx.sem = task.sem;
            ctx.response = task.response;
            bool failed = false;
            switch (task.type) {
                case leveldb::RDMA_ASYNC_COMPACTION:
                    ctx.req_id = cc_client_->InitiateCompaction(
                            task.server_id,
                            task.compaction_request);
                    break;
                case leveldb::RDMA_ASYNC_FILENAME_RTABLE_MAPPING:
                    ctx.req_id = cc_client_->InitiateFileNameRTableMapping(
                            task.server_id, task.fn_rtableid);
                    break;
                case leveldb::RDMA_ASYNC_READ_LOG_FILE:
                    ctx.req_id = cc_client_->InitiateReadInMemoryLogFile(
                            task.rdma_log_record_backing_mem,
                            task.server_id,
                            task.remote_dc_offset, task.size);
                    break;
                case leveldb::RDMA_ASYNC_REQ_READ:
                    ctx.req_id = cc_client_->InitiateRTableReadDataBlock(
                            task.rtable_handle,
                            task.offset,
                            task.size,
                            task.result, task.write_size, task.filename,
                            task.is_foreground_reads);
                    break;
                case leveldb::RDMA_ASYNC_REQ_QUERY_LOG_FILES:
                    ctx.req_id = cc_client_->InitiateQueryLogFile(
                            task.server_id,
                            nova::NovaConfig::config->my_server_id,
                            task.dbid,
                            task.logfile_offset);
                    break;
                case leveldb::RDMA_ASYNC_REQ_WRITE_DATA_BLOCKS:
                    ctx.req_id = cc_client_->InitiateRTableWriteDataBlocks(
                            task.server_id, task.thread_id,
                            nullptr, task.write_buf, task.dbname,
                            task.file_number, task.write_size,
                            task.is_meta_blocks);
                    break;
                case leveldb::RDMA_ASYNC_REQ_DELETE_TABLES:
                    ctx.req_id = cc_client_->InitiateDeleteTables(
                            task.server_id, task.rtable_ids);
                    break;
                case leveldb::RDMA_ASYNC_READ_DC_STATS:
                    ctx.req_id = cc_client_->InitiateReadDCStats(
                            task.server_id);
                    break;
                case leveldb::RDMA_ASYNC_REQ_CLOSE_LOG:
                    ctx.req_id = cc_client_->InitiateCloseLogFiles(
                            task.log_files, task.dbid);
                    break;
                case leveldb::RDMA_ASYNC_REQ_LOG_RECORD:
                    ctx.req_id = cc_client_->InitiateReplicateLogRecords(
                            task.log_file_name,
                            task.thread_id,
                            task.dbid,
                            task.memtable_id,
                            task.write_buf,
                            task.log_records,
                            task.replicate_log_record_states);
                    if (ctx.req_id == 0) {
                        // Failed and must retry.
                        failed = true;
                    }
                    break;
            }
            if (failed) {
                it++;
            } else {
                pending_reqs_.push_back(ctx);
                it = private_queue_.erase(it);
            }
        }
        return size;
    }

    bool NovaRDMAComputeComponent::IsInitialized() {
        mutex_.Lock();
        bool t = is_running_;
        mutex_.Unlock();
        return t;
    }

    void NovaRDMAComputeComponent::Start() {
        RDMA_LOG(INFO) << "Async worker started";

        if (NovaConfig::config->enable_rdma) {
            rdma_store_->Init(rdma_ctrl_);
        }

        nova::NovaConfig::config->add_tid_mapping();

        mutex_.Lock();
        is_running_ = true;
        mutex_.Unlock();

        bool should_sleep = true;
        uint32_t timeout = RDMA_POLL_MIN_TIMEOUT_US;
        while (is_running_) {
            while (should_pause) {
                paused = true;
                sem_wait(&sem_);
                paused = false;
            }

            if (should_sleep &&
                nova::NovaConfig::config->num_conn_async_workers > 1) {
                usleep(timeout);
            }
            int n = 0;

            auto endpoints = rdma_store_->end_points();
            for (const auto &endpoint : endpoints) {
                uint32_t new_requests = 0;
                uint32_t completed_requests = rdma_store_->PollSQ(
                        endpoint.server_id, &new_requests);
                new_requests = 0;
                rdma_store_->PollRQ(endpoint.server_id, &new_requests);
                n += completed_requests;
                n += new_requests;
            }
            n += cc_server_->ProcessCompletionQueue();
            n += ProcessRequestQueue();
            if (n == 0) {
                should_sleep = true;
                timeout *= 2;
                if (timeout > RDMA_POLL_MAX_TIMEOUT_US) {
                    timeout = RDMA_POLL_MAX_TIMEOUT_US;
                }
            } else {
                should_sleep = false;
                timeout = RDMA_POLL_MIN_TIMEOUT_US;
            }
        }
    }

    bool
    NovaRDMAComputeComponent::ProcessRDMAWC(ibv_wc_opcode opcode,
                                            uint64_t wr_id,
                                            int remote_server_id,
                                            char *buf, uint32_t imm_data,
                                            bool *generate_a_new_request) {
        if (opcode == IBV_WC_SEND ||
            opcode == IBV_WC_RDMA_WRITE ||
            opcode == IBV_WC_RDMA_READ) {
            // a send request completes.
            admission_control_->RemoveRequests(remote_server_id, 1);
        }
        if (opcode == IBV_WC_SEND || opcode == IBV_WC_RDMA_READ) {
            return true;
        }
        *generate_a_new_request = false;
        bool processed_by_client = cc_client_->OnRecv(opcode, wr_id,
                                                      remote_server_id, buf,
                                                      imm_data,
                                                      generate_a_new_request);
        if (*generate_a_new_request) {
            admission_control_->AddRequests(remote_server_id, 1);
        }
        *generate_a_new_request = false;
        bool processed_by_server = cc_server_->ProcessRDMAWC(opcode, wr_id,
                                                             remote_server_id,
                                                             buf,
                                                             imm_data,
                                                             generate_a_new_request);
        if (*generate_a_new_request) {
            admission_control_->AddRequests(remote_server_id, 1);
        }
        if (processed_by_client && processed_by_server) {
            RDMA_ASSERT(false)
                << fmt::format("Processed by both client and server");
        }
    }
}