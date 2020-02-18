
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
        queue_.push_back(task);
        mutex_.Unlock();
    }

    int NovaRDMAComputeComponent::size() {
        mutex_.Lock();
        int size = queue_.size();
        mutex_.Unlock();
        return size;
    }

    int NovaRDMAComputeComponent::ProcessQueue() {
        auto it = pending_reqs_.begin();
        while (it != pending_reqs_.end()) {
            if (cc_client_->IsDone(it->req_id, it->response, nullptr)) {
                if (it->sem) {
                    RDMA_ASSERT(sem_post(it->sem) == 0);
                }
                it = pending_reqs_.erase(it);
            } else {
                it++;
            }
        }

        mutex_.Lock();
        if (queue_.empty()) {
            mutex_.Unlock();
            return 0;
        }

        std::list<leveldb::RDMAAsyncClientRequestTask> queue;
        while (!queue_.empty()) {
            queue.push_back(queue_.front());
            queue_.pop_front();
        }
        mutex_.Unlock();

        for (const leveldb::RDMAAsyncClientRequestTask &task : queue) {
            RequestCtx ctx = {};
            ctx.sem = task.sem;
            ctx.response = task.response;
            switch (task.type) {
                case leveldb::RDMAAsyncRequestType::RDMA_ASYNC_REQ_READ:
                    ctx.req_id = cc_client_->InitiateRTableReadDataBlock(
                            task.rtable_handle,
                            task.offset,
                            task.size,
                            task.result);

                    break;
                case leveldb::RDMAAsyncRequestType::RDMA_ASYNC_REQ_CLOSE_LOG:
                    ctx.req_id = cc_client_->InitiateCloseLogFile(
                            task.log_file_name);
                    break;
                case leveldb::RDMAAsyncRequestType::RDMA_ASYNC_REQ_LOG_RECORD:
                    ctx.req_id = cc_client_->InitiateReplicateLogRecords(
                            task.log_file_name,
                            task.thread_id,
                            task.log_record);
                    break;
                case leveldb::RDMAAsyncRequestType::RDMA_ASYNC_REQ_WRITE_DATA_BLOCKS:
                    ctx.req_id = cc_client_->InitiateRTableWriteDataBlocks(
                            task.server_id, task.thread_id,
                            nullptr, task.write_buf, task.dbname,
                            task.file_number, task.write_size);
                    break;
                case leveldb::RDMAAsyncRequestType::RDMA_ASYNC_REQ_DELETE_TABLES:
                    ctx.req_id = cc_client_->InitiateDeleteTables(
                            task.server_id, task.rtable_ids);
                    break;
            }
            pending_reqs_.push_back(ctx);
        }
        return queue.size();
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

        mutex_.Lock();
        is_running_ = true;
        mutex_.Unlock();

        bool should_sleep = true;
        uint32_t timeout = RDMA_POLL_MIN_TIMEOUT_US;
        while (is_running_) {
            if (should_sleep) {
                usleep(timeout);
            }
            int n = 0;
            n += rdma_store_->PollSQ();
            n += rdma_store_->PollRQ();
            n += cc_server_->PullAsyncCQ();
            n += ProcessQueue();

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
                                            char *buf, uint32_t imm_data) {
        if (opcode == IBV_WC_SEND || opcode == IBV_WC_RDMA_READ) {
            return true;
        }
        bool processed_by_client = cc_client_->OnRecv(opcode, wr_id,
                                                      remote_server_id, buf,
                                                      imm_data);
        bool processed_by_server = cc_server_->ProcessRDMAWC(opcode, wr_id,
                                                             remote_server_id,
                                                             buf,
                                                             imm_data);
        if (processed_by_client && processed_by_server) {
            RDMA_ASSERT(false)
                << fmt::format("Processed by both client and server");
        }
    }
}