
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_cc_client.h"
#include "nova/nova_config.h"
#include "nova/nova_common.h"

#include <fmt/core.h>
#include "db/filename.h"

namespace leveldb {

    using namespace rdmaio;

    void NovaCCClient::IncrementReqId() {
        current_req_id_++;
        if (current_req_id_ == upper_req_id_) {
            current_req_id_ = lower_req_id_;
        }
    }

    uint32_t NovaCCClient::GetCurrentReqId() {
        return current_req_id_;
    }

    uint32_t NovaCCClient::InitiateDeleteTables(uint32_t server_id,
                                                const std::vector<SSTableRTablePair> &rtable_ids) {
        uint32_t req_id = current_req_id_;

        if (server_id == nova::NovaConfig::config->my_server_id) {
            for (int i = 0; i < rtable_ids.size(); i++) {
                leveldb::NovaRTable *rtable = rtable_manager_->rtable(
                        rtable_ids[i].rtable_id);
                rtable->DeleteSSTable(rtable_ids[i].sstable_id);
            }
            return 0;
        }

        RDMA_LOG(DEBUG)
            << fmt::format("dcclient[{}]: Delete SSTables server:{} n:{}",
                           cc_client_id_, server_id, rtable_ids.size());

        char *send_buf = rdma_store_->GetSendBuf(server_id);
        uint32_t msg_size = 1;
        send_buf[0] = CCRequestType::CC_DELETE_TABLES;
        EncodeFixed32(send_buf + msg_size, rtable_ids.size());
        msg_size += 4;
        for (auto &pair : rtable_ids) {
            msg_size += EncodeStr(send_buf + msg_size, pair.sstable_id);
            EncodeFixed32(send_buf + msg_size, pair.rtable_id);
            msg_size += 4;
        }
        rdma_store_->PostSend(send_buf, msg_size, server_id, req_id);

        // Does not need to send immediately.
        return 0;
    }

    uint32_t NovaCCClient::InitiateRTableReadDataBlock(
            const leveldb::RTableHandle &rtable_handle, uint64_t offset,
            uint32_t size, char *result) {
        if (rtable_handle.server_id == nova::NovaConfig::config->my_server_id) {
            rtable_manager_->ReadDataBlock(rtable_handle, offset, size, result);
            return 0;
        }

        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.req_type = CCRequestType::CC_RTABLE_READ_BLOCKS;
        context.backing_mem = result;
        context.size = size;
        context.done = false;

        nova::MarkCharAsWaitingForRDMAWRITE(result, context.size);

        char *send_buf = rdma_store_->GetSendBuf(rtable_handle.server_id);
        uint32_t msg_size = 1;
        send_buf[0] = CCRequestType::CC_RTABLE_READ_BLOCKS;
        EncodeFixed32(send_buf + msg_size, rtable_handle.rtable_id);
        msg_size += 4;
        EncodeFixed64(send_buf + msg_size, offset);
        msg_size += 8;
        EncodeFixed32(send_buf + msg_size, size);
        msg_size += 4;
        EncodeFixed64(send_buf + msg_size, (uint64_t) result);
        msg_size += 8;

        rdma_store_->PostSend(send_buf, msg_size, rtable_handle.server_id,
                              req_id);
        request_context_[req_id] = context;
        IncrementReqId();

        // Flush immediately.
        rdma_store_->FlushPendingSends(rtable_handle.server_id);

        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Read RTable server:{} rtable:{} offset:{} size:{} off:{} size:{} req:{}",
                    cc_client_id_, rtable_handle.server_id,
                    rtable_handle.rtable_id, rtable_handle.offset,
                    rtable_handle.size, offset, size, req_id);
        return req_id;
    }

    uint32_t NovaCCClient::InitiateRTableWriteDataBlocks(uint32_t server_id,
                                                         uint32_t thread_id,
                                                         uint32_t *rtable_id,
                                                         char *buf,
                                                         const std::string &dbname,
                                                         uint64_t file_number,
                                                         uint32_t size) {
        if (server_id == nova::NovaConfig::config->my_server_id) {
            std::string sstable_id = TableFileName(dbname, file_number);
            NovaRTable *active_rtable = rtable_manager_->active_rtable(
                    thread_id);
            uint64_t offset = active_rtable->AllocateBuf(
                    sstable_id, size);
            if (offset == UINT64_MAX) {
                active_rtable = rtable_manager_->CreateNewRTable(thread_id);
                offset = active_rtable->AllocateBuf(sstable_id, size);
            }
            memcpy((char *) (offset), buf, size);
            active_rtable->MarkOffsetAsWritten(offset);
            *rtable_id = active_rtable->rtable_id();
            return 0;
        }

        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.done = false;
        context.req_type = CCRequestType::CC_RTABLE_WRITE_SSTABLE;

        char *send_buf = rdma_store_->GetSendBuf(server_id);
        uint32_t msg_size = 1;
        send_buf[0] = CCRequestType::CC_RTABLE_WRITE_SSTABLE;
        msg_size += EncodeStr(send_buf + msg_size, dbname);
        EncodeFixed64(send_buf + msg_size, file_number);
        msg_size += 8;
        EncodeFixed32(send_buf + msg_size, size);
        msg_size += 4;
        rdma_store_->PostSend(send_buf, msg_size, server_id, req_id);
        context.backing_mem = buf;
        context.size = size;
        request_context_[req_id] = context;
        IncrementReqId();

        rdma_store_->FlushPendingSends(server_id);
        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Write RTable server:{} t:{} db:{} fn:{} size:{} req:{}",
                    cc_client_id_, server_id, thread_id, dbname, file_number,
                    size, req_id);
        return req_id;
    }

    uint32_t NovaCCClient::InitiatePersist(uint32_t server_id,
                                           const std::vector<SSTableRTablePair> &rtable_ids) {
        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.done = false;
        context.req_type = CCRequestType::CC_RTABLE_PERSIST;

        if (server_id == nova::NovaConfig::config->my_server_id) {
            context.done = true;
            for (int i = 0; i < rtable_ids.size(); i++) {
                leveldb::NovaRTable *rtable = rtable_manager_->rtable(
                        rtable_ids[i].rtable_id);
                rtable->Persist();

                leveldb::BlockHandle h = rtable->Handle(
                        rtable_ids[i].sstable_id);
                leveldb::RTableHandle rh = {};
                rh.server_id = nova::NovaConfig::config->my_server_id;
                rh.rtable_id = rtable_ids[i].rtable_id;
                rh.offset = h.offset();
                rh.size = h.size();
                context.size += h.size();
                context.rtable_handles.push_back(rh);
            }
            request_context_[req_id] = context;
            IncrementReqId();
            return req_id;
        }

        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Persist server:{} n:{} req:{}",
                    cc_client_id_, server_id, rtable_ids.size(), req_id);


        char *send_buf = rdma_store_->GetSendBuf(server_id);
        uint32_t msg_size = 1;
        send_buf[0] = CCRequestType::CC_RTABLE_PERSIST;
        EncodeFixed32(send_buf + msg_size, rtable_ids.size());
        msg_size += 4;
        for (auto &pair : rtable_ids) {
            msg_size += EncodeStr(send_buf + msg_size, pair.sstable_id);
            EncodeFixed32(send_buf + msg_size, pair.rtable_id);
            msg_size += 4;
        }

        rdma_store_->PostSend(send_buf, msg_size, server_id, req_id);
        request_context_[req_id] = context;
        IncrementReqId();

        rdma_store_->FlushPendingSends(server_id);
        return req_id;
    }

    uint32_t NovaCCClient::InitiateReplicateLogRecords(
            const std::string &log_file_name, uint64_t thread_id,
            const leveldb::Slice &slice) {
        // Synchronous replication.
        rdma_log_writer_->AddRecord(log_file_name, thread_id, slice);
        return 0;
    }

    uint32_t
    NovaCCClient::InitiateCloseLogFile(const std::string &log_file_name) {
        rdma_log_writer_->CloseLogFile(log_file_name);
        return 0;
    }

    bool NovaCCClient::IsDone(uint32_t req_id, CCResponse *response,
                              uint64_t *timeout) {
        // Poll both queues.
        rdma_store_->PollRQ();
        rdma_store_->PollSQ();
        cc_server_->PullAsyncCQ();

        if (req_id == 0) {
            // local bypass.
            return true;
        }

        auto context_it = request_context_.find(req_id);
        if (context_it == request_context_.end()) {
            return true;
        }

        if (context_it->second.done) {
            if (response) {
                response->rtable_id = context_it->second.rtable_id;
                response->rtable_handles = context_it->second.rtable_handles;
            }
            request_context_.erase(req_id);
            return true;
        }

        if (timeout) {
            *timeout = 0;
            if (context_it->second.req_type ==
                CCRequestType::CC_RTABLE_READ_BLOCKS) {
                *timeout = context_it->second.size / 100;
            } else if (context_it->second.req_type ==
                       CCRequestType::CC_RTABLE_WRITE_SSTABLE) {
                *timeout = context_it->second.size / 7000;
            } else if (context_it->second.req_type ==
                       CCRequestType::CC_RTABLE_PERSIST) {
                *timeout = context_it->second.size / 100;
            }
        }
        return false;
    }

    bool
    NovaCCClient::OnRecv(ibv_wc_opcode type, uint64_t wr_id,
                         int remote_server_id,
                         char *buf,
                         uint32_t imm_data) {
        bool processed = false;

        uint32_t req_id = imm_data;
        switch (type) {
            case IBV_WC_SEND:
                break;
            case IBV_WC_RDMA_WRITE:
                if (!rdma_log_writer_->AckWriteSuccess(remote_server_id,
                                                       wr_id)) {
                    for (auto it = requests_.begin();
                         it != requests_.end(); it++) {
                        if (it->remote_server_id == remote_server_id &&
                            it->wr_id == wr_id) {
                            auto req_it = request_context_.find(it->req_id);
                            RDMA_ASSERT(req_it != request_context_.end());
                            req_it->second.done = true;
                            it = requests_.erase(it);

                            RDMA_LOG(DEBUG) << fmt::format(
                                        "dcclient[{}]: Write RTable complete req:{}",
                                        cc_client_id_, req_id);
                            processed = true;
                            break;
                        }
                    }
                } else {
                    RDMA_LOG(DEBUG) << fmt::format(
                                "dcclient[{}]: Log record replicated req:{} wr_id:{} first:{}",
                                cc_client_id_, req_id, wr_id, buf[0]);
                    processed = true;
                }
                break;
            case IBV_WC_RDMA_READ:
                break;
            case IBV_WC_RECV:
            case IBV_WC_RECV_RDMA_WITH_IMM:
                auto context_it = request_context_.find(req_id);
                if (context_it != request_context_.end()) {
                    // I sent this request a while ago and now it is complete.
                    auto &context = context_it->second;
                    if (buf[0] == CC_RTABLE_WRITE_SSTABLE_RESPONSE) {
                        RDMA_ASSERT(context.req_type ==
                                    CCRequestType::CC_RTABLE_WRITE_SSTABLE);
                        // RTable handle.
                        uint32_t rtable_id = DecodeFixed32(buf + 1);
                        uint64_t rtable_offset = leveldb::DecodeFixed64(
                                buf + 5);
                        uint64_t wr_id = rdma_store_->PostWrite(
                                context.backing_mem,
                                context.size, remote_server_id,
                                rtable_offset, false, req_id);
                        context.done = false;
                        context.rtable_id = rtable_id;
                        requests_.push_back({
                                                    .remote_server_id = remote_server_id,
                                                    .wr_id = wr_id,
                                                    .req_id = req_id
                                            });
                        RDMA_LOG(DEBUG) << fmt::format(
                                    "dcclient[{}]: Write RTable received off id:{} offset:{} req:{}",
                                    cc_client_id_, rtable_id, rtable_offset,
                                    req_id);
                        processed = true;
                    } else if (context.req_type ==
                               CCRequestType::CC_RTABLE_READ_BLOCKS) {
                        // Waiting for WRITEs.
                        if (nova::IsRDMAWRITEComplete(context.backing_mem,
                                                      context.size)) {
                            RDMA_ASSERT(buf[0] == '~') << buf[0];
                            RDMA_LOG(DEBUG) << fmt::format(
                                        "dcclient[{}]: Read RTable blocks complete size:{} req:{}",
                                        cc_client_id_, context.size, req_id);

                            context.done = true;
                            processed = true;
                        } else {
                            context.done = false;
                        }
                    } else if (buf[0] ==
                               CCRequestType::CC_RTABLE_PERSIST_RESPONSE) {
                        RDMA_ASSERT(context.req_type ==
                                    CCRequestType::CC_RTABLE_PERSIST);
                        uint32_t msg_size = 1;
                        uint32_t rtable_handles = DecodeFixed32(buf + msg_size);
                        msg_size += 4;
                        std::string rids;
                        for (int i = 0; i < rtable_handles; i++) {
                            RTableHandle rh = {};
                            rh.DecodeHandle(buf + msg_size);
                            context.rtable_handles.push_back(rh);
                            msg_size += RTableHandle::HandleSize();
                            rids += fmt::format("{},", rh.rtable_id);
                        }
                        context.done = true;
                        RDMA_LOG(DEBUG) << fmt::format(
                                    "dcclient[{}]: Persist RTable received handles:{} rids:{} req:{}",
                                    cc_client_id_, rtable_handles, rids,
                                    req_id);
                        processed = true;
                    }
                }
                if (buf[0] ==
                    CCRequestType::CC_ALLOCATE_LOG_BUFFER_SUCC) {
                    uint64_t base = leveldb::DecodeFixed64(buf + 1);
                    uint64_t size = leveldb::DecodeFixed64(buf + 9);
                    rdma_log_writer_->AckAllocLogBuf(remote_server_id, base,
                                                     size);

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dcclient[{}]: Allocate log buffer success req:{}",
                                cc_client_id_, req_id);
                    processed = true;
                }
                break;
        }
        return processed;
    }

}