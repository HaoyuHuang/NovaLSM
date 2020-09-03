
//
// Created by Haoyu Huang on 12/25/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <leveldb/write_batch.h>
#include "rdma_msg_handler.h"
#include "common/nova_config.h"
#include "common/nova_common.h"

namespace nova {

    static void DeleteEntry(const leveldb::Slice &key, void *value) {
        CacheValue *tf = reinterpret_cast<CacheValue *>(value);
        delete tf;
    }

    void RDMAMsgHandler::AddTask(
            const leveldb::RDMARequestTask &task) {
        mutex_.Lock();
        public_queue_.push_back(task);
        mutex_.Unlock();
    }

    int RDMAMsgHandler::size() {
        mutex_.Lock();
        int size = public_queue_.size();
        mutex_.Unlock();
        return size;
    }

    int RDMAMsgHandler::ProcessRequestQueue() {
        {
            auto it = pending_reqs_.begin();
            while (it != pending_reqs_.end()) {
                if (stoc_client_->IsDone(it->req_id, it->response, nullptr)) {
                    if (it->sem) {
                        NOVA_LOG(rdmaio::DEBUG)
                            << fmt::format("Wake up request: {}", it->req_id);
                        NOVA_ASSERT(sem_post(it->sem) == 0);
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
        std::vector<int> serverids;
        while (it != private_queue_.end()) {
            const auto &task = *it;
            serverids.clear();
            if (task.type == leveldb::RDMA_CLIENT_REQ_CLOSE_LOG ||
                task.type == leveldb::RDMA_CLIENT_REQ_LOG_RECORD) {
                NOVA_ASSERT(task.server_id == -1);
                // A log record request.
                nova::LTCFragment *frag = nova::NovaConfig::config->cfgs[0]->fragments[task.dbid];
                for (int i = 0; i < frag->log_replica_stoc_ids.size(); i++) {
                    uint32_t stoc_server_id = nova::NovaConfig::config->cfgs[0]->stoc_servers[frag->log_replica_stoc_ids[i]];
                    serverids.push_back(stoc_server_id);
                }
                if (!admission_control_->CanIssueRequest(serverids)) {
                    it++;
                    continue;
                }
            } else {
                NOVA_ASSERT(task.server_id >= 0);
                if (!admission_control_->CanIssueRequest(task.server_id)) {
                    it++;
                    continue;
                }
            }
            RequestCtx ctx = {};
            ctx.sem = task.sem;
            ctx.response = task.response;
            bool failed = false;
            switch (task.type) {
                case leveldb::RDMA_CLIENT_ALLOCATE_LOG_BUFFER_SUCC:
                    rdma_log_writer_->AckAllocLogBuf(task.log_file_name,
                                                     task.server_id,
                                                     task.offset,
                                                     task.size,
                                                     task.rdma_log_record_backing_mem,
                                                     task.write_size,
                                                     task.thread_id,
                                                     task.replicate_log_record_states);
                    ctx.req_id = task.thread_id;
                    break;
                case leveldb::RDMA_CLIENT_RDMA_WRITE_REQUEST:
                    ctx.req_id = stoc_client_->InitiateRDMAWRITE(task.server_id, task.write_buf, task.size);
                    break;
                case leveldb::RDMA_CLIENT_RDMA_WRITE_REMOTE_BUF_ALLOCATED: {
                    char *sendbuf = rdma_broker_->GetSendBuf(task.server_id);
                    sendbuf[0] = leveldb::StoCRequestType::RDMA_WRITE_REMOTE_BUF_ALLOCATED;
                    leveldb::EncodeFixed32(sendbuf + 1,
                                           (uint64_t) task.thread_id);
                    rdma_broker_->PostWrite(
                            task.write_buf,
                            task.write_size, task.server_id,
                            task.offset, false, task.thread_id);
                    ctx.req_id = task.thread_id;
                }
                    break;
                case leveldb::RDMA_CLIENT_WRITE_SSTABLE_RESPONSE:
                    rdma_broker_->PostWrite(
                            task.write_buf,
                            task.size, task.server_id,
                            task.offset, false, task.thread_id);
                    ctx.req_id = task.thread_id;
                    break;
                case leveldb::RDMA_CLIENT_COMPACTION:
                    ctx.req_id = stoc_client_->InitiateCompaction(
                            task.server_id,
                            task.compaction_request);
                    break;
                case leveldb::RDMA_CLIENT_FILENAME_STOC_FILE_MAPPING:
                    ctx.req_id = stoc_client_->InitiateInstallFileNameStoCFileMapping(
                            task.server_id, task.fn_stoc_file_id);
                    break;
                case leveldb::RDMA_CLIENT_READ_LOG_FILE:
                    ctx.req_id = stoc_client_->InitiateReadInMemoryLogFile(
                            task.rdma_log_record_backing_mem, task.server_id, task.remote_stoc_offset, task.size);
                    break;
                case leveldb::RDMA_CLIENT_REQ_READ:
                    ctx.req_id = stoc_client_->InitiateReadDataBlock(
                            task.stoc_block_handle,
                            task.offset,
                            task.size,
                            task.result, task.write_size, task.filename,
                            task.is_foreground_reads);
                    break;
                case leveldb::RDMA_CLIENT_REQ_QUERY_LOG_FILES:
                    ctx.req_id = stoc_client_->InitiateQueryLogFile(
                            task.server_id,
                            nova::NovaConfig::config->my_server_id,
                            task.dbid,
                            task.logfile_offset);
                    break;
                case leveldb::RDMA_CLIENT_REQ_WRITE_DATA_BLOCKS:
                    ctx.req_id = stoc_client_->InitiateAppendBlock(
                            task.server_id, task.thread_id,
                            nullptr, task.write_buf, task.dbname,
                            task.file_number, task.replica_id, task.write_size,
                            task.internal_type);
                    break;
                case leveldb::RDMA_CLIENT_REQ_DELETE_TABLES:
                    ctx.req_id = stoc_client_->InitiateDeleteTables(
                            task.server_id, task.stoc_file_ids);
                    break;
                case leveldb::RDMA_CLIENT_READ_STOC_STATS:
                    ctx.req_id = stoc_client_->InitiateReadStoCStats(
                            task.server_id);
                    break;
                case leveldb::RDMA_CLIENT_REQ_CLOSE_LOG:
                    ctx.req_id = stoc_client_->InitiateCloseLogFiles(
                            task.log_files, task.dbid);
                    break;
                case leveldb::RDMA_CLIENT_IS_READY_FOR_REQUESTS:
                    ctx.req_id = stoc_client_->InitiateIsReadyForProcessingRequests(
                            task.server_id);
                    break;
                case leveldb::RDMA_CLIENT_REQ_LOG_RECORD:
                    ctx.req_id = stoc_client_->InitiateReplicateLogRecords(
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
                case leveldb::RDMA_CLIENT_RECONSTRUCT_MISSING_REPLICA:
                    ctx.req_id = stoc_client_->InitiateReplicateSSTables(
                            task.server_id, task.dbname, task.missing_replicas);
                    break;
            }
            if (failed) {
                NOVA_ASSERT(serverids.size() > 0);
                for (auto sid : serverids) {
                    admission_control_->RemoveRequests(sid, 1);
                }
                it++;
            } else {
                pending_reqs_.push_back(ctx);
                it = private_queue_.erase(it);
            }
        }
        return size;
    }

    bool RDMAMsgHandler::IsInitialized() {
        mutex_.Lock();
        bool t = is_running_;
        mutex_.Unlock();
        return t;
    }

    void RDMAMsgHandler::Start() {
        NOVA_LOG(DEBUG) << "Async worker started";
        if (NovaConfig::config->enable_rdma) {
            rdma_broker_->Init(rdma_ctrl_);
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
                nova::NovaConfig::config->num_fg_rdma_workers > 1) {
                usleep(timeout);
            }
            int n = 0;

            auto endpoints = rdma_broker_->end_points();
            for (const auto &endpoint : endpoints) {
                uint32_t new_requests = 0;
                uint32_t completed_requests = rdma_broker_->PollSQ(
                        endpoint.server_id, &new_requests);
                new_requests = 0;
                rdma_broker_->PollRQ(endpoint.server_id, &new_requests);
                n += completed_requests;
                n += new_requests;
            }
            n += rdma_server_->ProcessCompletionQueue();
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
    RDMAMsgHandler::ProcessRDMAWC(ibv_wc_opcode opcode,
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
        if (opcode == IBV_WC_SEND) {
            return true;
        }
        bool processed_by_client = stoc_client_->OnRecv(opcode, wr_id, remote_server_id, buf, imm_data, nullptr);
        bool processed_by_server = rdma_server_->ProcessRDMAWC(opcode, wr_id, remote_server_id, buf, imm_data, nullptr);
        if (processed_by_client && processed_by_server) {
            NOVA_ASSERT(false)
                << fmt::format("Processed by both client and server");
        }
    }
}