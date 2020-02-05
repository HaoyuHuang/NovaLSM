
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <fmt/core.h>

#include "db/filename.h"
#include "nova_cc_server.h"
#include "cc/nova_cc_client.h"

namespace nova {
    namespace {
        uint64_t to_req_id(uint32_t remote_sid, uint32_t dc_req_id) {
            uint64_t req_id = 0;
            req_id = ((uint64_t) remote_sid) << 32;
            return req_id + dc_req_id;
        }

        uint64_t to_dc_req_id(uint64_t req_id) {
            return (uint32_t) (req_id);
        }
    }

    NovaCCServer::NovaCCServer(rdmaio::RdmaCtrl *rdma_ctrl,
                               NovaMemManager *mem_manager,
                               leveldb::NovaRTableManager *rtable_manager,
                               LogFileManager *log_manager, uint32_t thread_id,
                               bool is_compaction_thread)
            : rdma_ctrl_(rdma_ctrl),
              mem_manager_(mem_manager),
              rtable_manager_(rtable_manager),
              log_manager_(log_manager), thread_id_(thread_id) {
        if (is_compaction_thread) {
            current_rtable_ = rtable_manager_->CreateNewRTable(thread_id_);
        }
    }

    // No need to flush RDMA requests since Flush will be done after all requests are processed in a receive queue.
    bool
    NovaCCServer::ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id,
                                int remote_server_id, char *buf,
                                uint32_t imm_data) {
        bool processed = false;
        switch (type) {
            case IBV_WC_SEND:
                break;
            case IBV_WC_RDMA_WRITE:
                if (buf[0] == leveldb::CCRequestType::CC_RTABLE_READ_BLOCKS) {
                    uint64_t written_wr_id = leveldb::DecodeFixed64(buf + 1);
                    if (written_wr_id == wr_id) {
                        uint32_t size = leveldb::DecodeFixed32(buf + 9);
                        uint64_t allocated_buf_int = leveldb::DecodeFixed64(
                                buf + 13);
                        char *allocated_buf = (char *) (allocated_buf_int);
                        uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                                  size);
                        mem_manager_->FreeItem(thread_id_, allocated_buf, scid);
                        processed = true;

                        RDMA_LOG(DEBUG) << fmt::format(
                                    "dc[{}]: imm:{} type:{} allocated buf:{} size:{} wr:{}.",
                                    thread_id_,
                                    imm_data,
                                    buf[0], allocated_buf_int, size, wr_id);
                    }
                }
                break;
            case IBV_WC_RDMA_READ:
                break;
            case IBV_WC_RECV:
            case IBV_WC_RECV_RDMA_WITH_IMM:
                uint32_t dc_req_id = imm_data;
                uint64_t req_id = to_req_id(remote_server_id, dc_req_id);

                auto context_it = request_context_map_.find(req_id);
                if (context_it != request_context_map_.end()) {
                    auto &context = context_it->second;
                    // Waiting for writes.
                    if (context.request_type ==
                        leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE_RESPONSE) {
                        if (IsRDMAWRITEComplete((char *) context.rtable_offset,
                                                context.size)) {
                            RDMA_ASSERT(buf[0] == '~');
                            RDMA_LOG(DEBUG) << fmt::format(
                                        "dc[{}]: Write RTable complete id:{} offset:{} creq_id:{} req_id:{}",
                                        thread_id_, context.rtable_id,
                                        context.rtable_offset, dc_req_id,
                                        req_id);

                            rtable_manager_->rtable(
                                    context.rtable_id)->MarkOffsetAsWritten(
                                    context.rtable_offset);
                            processed = true;
                            request_context_map_.erase(req_id);
                        }
                    }
                }

                if (buf[0] == leveldb::CCRequestType::CC_DELETE_TABLES) {
                    uint32_t msg_size = 1;
                    uint32_t nrtables = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;

                    for (int i = 0; i < nrtables; i++) {
                        std::string sstable_id;
                        msg_size += leveldb::DecodeStr(buf + msg_size,
                                                       &sstable_id);
                        uint32_t rtableid = leveldb::DecodeFixed32(
                                buf + msg_size);
                        msg_size += 4;
                        rtable_manager_->rtable(rtableid)->DeleteSSTable(
                                sstable_id);
                    }

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: Delete SSTables. nsstables:{}",
                                thread_id_, nrtables);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_RTABLE_READ_BLOCKS) {
                    uint32_t msg_size = 1;
                    uint32_t rtable_id = 0;
                    uint64_t offset = 0;
                    uint32_t size = 0;
                    uint64_t cc_mr_offset = 0;

                    rtable_id = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    offset = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;
                    size = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    cc_mr_offset = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;

                    leveldb::RTableHandle rtable_handle = {};
                    rtable_handle.server_id = nova::NovaConfig::config->my_server_id;
                    rtable_handle.rtable_id = rtable_id;
                    rtable_handle.offset = offset;
                    rtable_handle.size = size;

                    uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                              size);
                    char *rdma_buf = mem_manager_->ItemAlloc(thread_id_, scid);
                    RDMA_ASSERT(rdma_buf);

                    rtable_manager_->ReadDataBlock(rtable_handle,
                                                   rtable_handle.offset,
                                                   rtable_handle.size,
                                                   rdma_buf);

                    char *sendbuf = rdma_store_->GetSendBuf(remote_server_id);
                    uint64_t wr_id = rdma_store_->PostWrite(rdma_buf, size,
                                                            remote_server_id,
                                                            cc_mr_offset, false,
                                                            dc_req_id);

                    sendbuf[0] = leveldb::CCRequestType::CC_RTABLE_READ_BLOCKS;
                    leveldb::EncodeFixed64(sendbuf + 1, wr_id);
                    leveldb::EncodeFixed32(sendbuf + 9, size);
                    leveldb::EncodeFixed64(sendbuf + 13, (uint64_t) (rdma_buf));

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc{}: Read blocks of RTable {} offset:{} size:{} cc_mr_offset:{} last:{}",
                                thread_id_, rtable_id, offset, size,
                                cc_mr_offset, rdma_buf[rtable_handle.size - 1]);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE) {
                    uint32_t msg_size = 1;
                    std::string dbname;
                    uint64_t file_number;
                    uint32_t size;

                    msg_size += leveldb::DecodeStr(buf + msg_size, &dbname);
                    file_number = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;
                    size = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    std::string table_name = leveldb::TableFileName(dbname,
                                                                    file_number);
                    uint64_t rtable_off = current_rtable_->AllocateBuf(
                            table_name, size);
                    if (rtable_off == UINT64_MAX) {
                        // overflow.
                        // close.
                        current_rtable_ = rtable_manager_->CreateNewRTable(
                                thread_id_);
                        rtable_off = current_rtable_->AllocateBuf(table_name,
                                                                  size);
                    }
                    char *sendbuf = rdma_store_->GetSendBuf(remote_server_id);
                    sendbuf[0] =
                            leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE_RESPONSE;
                    leveldb::EncodeFixed32(sendbuf + 1,
                                           current_rtable_->rtable_id());
                    leveldb::EncodeFixed64(sendbuf + 5, rtable_off);
                    rdma_store_->PostSend(sendbuf, 13, remote_server_id,
                                          dc_req_id);

                    MarkCharAsWaitingForRDMAWRITE((char *) rtable_off, size);

                    RequestContext context = {};
                    context.request_type = leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE_RESPONSE;
                    context.rtable_id = current_rtable_->rtable_id();
                    context.rtable_offset = rtable_off;
                    context.size = size;
                    request_context_map_[req_id] = context;

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc{}: Allocate buf for RTable Write db:{} fn:{} size:{} rtable_id:{} rtable_off:{}",
                                thread_id_, dbname, file_number, size,
                                current_rtable_->rtable_id(), rtable_off);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_RTABLE_PERSIST) {
                    uint32_t msg_size = 1;
                    uint32_t nrtables = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;

                    leveldb::RTableHandle rtable_handles[nrtables];
                    for (int i = 0; i < nrtables; i++) {
                        std::string sstable_id;
                        msg_size += leveldb::DecodeStr(buf + msg_size,
                                                       &sstable_id);
                        uint32_t rtableid = leveldb::DecodeFixed32(
                                buf + msg_size);
                        msg_size += 4;
                        leveldb::NovaRTable *rtable = rtable_manager_->rtable(
                                rtableid);
                        rtable->Persist();

                        leveldb::BlockHandle &h = rtable->Handle(sstable_id);
                        leveldb::RTableHandle rh = {};
                        rh.server_id = NovaConfig::config->my_server_id;
                        rh.rtable_id = rtableid;
                        rh.offset = h.offset();
                        rh.size = h.size();
                        rtable_handles[i] = rh;
                    }

                    char *sendbuf = rdma_store_->GetSendBuf(remote_server_id);
                    sendbuf[0] = leveldb::CCRequestType::CC_RTABLE_PERSIST_RESPONSE;
                    msg_size = 1;
                    leveldb::EncodeFixed32(sendbuf + msg_size, nrtables);
                    msg_size += 4;
                    for (int i = 0; i < nrtables; i++) {
                        rtable_handles[i].EncodeHandle(sendbuf + msg_size);
                        msg_size += leveldb::RTableHandle::HandleSize();
                    }

                    rdma_store_->PostSend(sendbuf, msg_size, remote_server_id,
                                          dc_req_id);
                    RDMA_LOG(DEBUG)
                        << fmt::format("dc{}: Persist RTables n:{}", thread_id_,
                                       nrtables);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_ALLOCATE_LOG_BUFFER) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    std::string log_file(buf + 5, size);
                    uint32_t slabclassid = mem_manager_->slabclassid(thread_id_,
                                                                     nova::NovaConfig::config->log_buf_size);
                    char *buf = mem_manager_->ItemAlloc(thread_id_,
                                                        slabclassid);
                    RDMA_ASSERT(buf) << "Running out of memory";
                    log_manager_->Add(thread_id_, log_file, buf);
                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
                    send_buf[0] = leveldb::CCRequestType::CC_ALLOCATE_LOG_BUFFER_SUCC;
                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) buf);
                    leveldb::EncodeFixed64(send_buf + 9,
                                           NovaConfig::config->log_buf_size);
                    rdma_store_->PostSend(send_buf, 1 + 8 + 8, remote_server_id,
                                          0);
                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: Allocate log buffer for file {}.",
                                thread_id_, log_file);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_DELETE_LOG_FILE) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    std::string log_file(buf + 5, size);
                    log_manager_->DeleteLogBuf(log_file);
                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: Delete log buffer for file {}.",
                                thread_id_, log_file);
                    processed = true;
                }
                break;
        }
        return processed;
    }
}