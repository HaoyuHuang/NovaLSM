
//
// Created by Haoyu Huang on 2/12/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <fmt/core.h>
#include "rdma_write_client.h"
#include "bench_common.h"

namespace nova {
    RDMAWRITEClient::RDMAWRITEClient(uint32_t write_size_kb,
                                     uint32_t my_server_id) : write_size_kb_(
            write_size_kb), my_server_id_(my_server_id) {
    }

    uint32_t RDMAWRITEClient::Initiate() {
        if (!buf_) {
            uint64_t size = write_size_kb_ * 1024;
            uint32_t scid = mem_manager_->slabclassid(thread_id_, size);

            NOVA_LOG(DEBUG) << fmt::format("Allocate size {}", size);

            buf_ = mem_manager_->ItemAlloc(thread_id_, scid);
            NOVA_ASSERT(buf_);
            memset(buf_, '1', size);
        }

        RequestContext ctx = {};
        ctx.local_buf = buf_;
        uint32_t reqid = req_id;

        req_context_[req_id] = ctx;

        for (int i = 0; i < servers.size(); i++) {
            if (my_server_id_ == i) {
                continue;
            }

            char *sendbuf = rdma_broker_->GetSendBuf(i);
            sendbuf[0] = BenchRequestType::BENCH_ALLOCATE;
            leveldb::EncodeFixed64(sendbuf + 1, write_size_kb_ * 1024);
            rdma_broker_->PostSend(sendbuf, 9, i, req_id);
            NOVA_LOG(DEBUG)
                << fmt::format("client[{}]: allocate req:{} to server {}",
                               thread_id_, req_id, i);
            rdma_broker_->FlushPendingSends(i);
        }

        req_id++;
        if (req_id == upper_req_id_) {
            req_id = lower_req_id_;
        }
        return reqid;
    }

    bool RDMAWRITEClient::IsDone(uint32_t req_id) {
        auto it = req_context_.find(req_id);
        if (it == req_context_.end()) {
            return true;
        }
        if (it->second.persisted == servers.size() - 1) {
            req_context_.erase(req_id);
            return true;
        }
        return false;
    }

    bool
    RDMAWRITEClient::ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id,
                                   int remote_server_id, char *buf,
                                   uint32_t req_id, bool *new_request) {
        bool processed = false;
        switch (type) {
            case IBV_WC_SEND:
                break;
            case IBV_WC_RDMA_WRITE: {
                NOVA_ASSERT(buf[0] == 'w');
                uint32_t req_id = leveldb::DecodeFixed32(buf + 1);
                uint32_t remote_server_id = leveldb::DecodeFixed32(buf + 1 + 4);

                RequestContext &ctx = req_context_[req_id];

                char *sendbuf = rdma_broker_->GetSendBuf(remote_server_id);
                sendbuf[0] = BenchRequestType::BENCH_PERSIST;
                rdma_broker_->PostSend(sendbuf, 1, remote_server_id, req_id);
                processed = true;

                NOVA_LOG(DEBUG)
                    << fmt::format(
                            "client[{}]: persist req:{} to server {}",
                            thread_id_, req_id, remote_server_id);
            }
                break;
            case IBV_WC_RDMA_READ:
                break;
            case IBV_WC_RECV:
            case IBV_WC_RECV_RDMA_WITH_IMM:
                if (req_id < lower_req_id_ || req_id >= upper_req_id_) {
                    return false;
                }

                NOVA_LOG(DEBUG)
                    << fmt::format(
                            "client[{}]: response req:{} from server {}",
                            thread_id_, req_id, remote_server_id);

                NOVA_ASSERT(req_context_.find(req_id) != req_context_.end());
                RequestContext &ctx = req_context_[req_id];
                if (buf[0] == BenchRequestType::BENCH_ALLOCATE_RESPONSE) {
                    uint64_t remote_offset = leveldb::DecodeFixed64(buf + 1);
                    char *sendbuf = rdma_broker_->GetSendBuf(remote_server_id);
                    rdma_broker_->PostWrite(ctx.local_buf,
                                           write_size_kb_ *
                                           1024,
                                            remote_server_id,
                                            remote_offset,
                                            false, req_id);

                    NOVA_LOG(DEBUG)
                        << fmt::format(
                                "client[{}]: write req:{} to server {}",
                                thread_id_, req_id, remote_server_id);

                    sendbuf[0] = 'w';
                    leveldb::EncodeFixed32(sendbuf + 1, req_id);
                    leveldb::EncodeFixed32(sendbuf + 5, remote_server_id);
                    processed = true;
                } else if (buf[0] == BenchRequestType::BENCH_PERSIST_RESPONSE) {
                    ctx.persisted += 1;
                    processed = true;
                }
                break;
        }
        return processed;
    }
}