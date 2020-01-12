
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_dc_client.h"
#include "nova/nova_config.h"
#include "nova/nova_common.h"

namespace leveldb {

    uint32_t NovaDCClient::InitiateFlushSSTable(const std::string &dbname,
                                                uint64_t file_number,
                                                const leveldb::FileMetaData &meta,
                                                char *backing_mem) {
        uint32_t dc_id = HomeDCNode(meta);
        char *sendbuf = rdma_store_->GetSendBuf(dc_id);
        char *buf = sendbuf;
        leveldb::EncodeFixed32(buf, dbname.size());
        buf += 4;
        memcpy(buf, dbname.c_str(), dbname.size());
        buf += dbname.size();
        leveldb::EncodeFixed32(buf, file_number);
        buf += 4;
        leveldb::EncodeFixed32(buf, meta.file_size);
        rdma_store_->PostSend(sendbuf, 4 + dbname.size() + 4 + 4, dc_id,
                              current_req_id_);
        DCRequestContext context = {};
        uint32_t req_id = current_req_id_;
        context.done = false;
        context.meta = meta;
        context.file_number = file_number;
        context.sstable_backing_mem = backing_mem;
        context.dbname = dbname;
        request_context_[current_req_id_] = context;
        current_req_id_++;
        return req_id;
    }

    uint32_t NovaDCClient::HomeDCNode(const leveldb::FileMetaData &meta) {
        uint64_t hash = nova::keyhash(meta.smallest.user_key().data(),
                                      meta.smallest.user_key().size());
        nova::Fragment *frag = nova::NovaDCConfig::home_fragment(hash);
        RDMA_ASSERT(frag != nullptr);
        return frag->server_ids[0];
    }

    uint32_t NovaDCClient::InitiateReadSSTable(const std::string &dbname,
                                               uint64_t file_number,
                                               const leveldb::FileMetaData &meta,
                                               char *result) {
        // The request contains dbname, file number, and remote offset to accept the read SSTable.
        // DC issues a WRITE_IMM to write the read SSTable into the remote offset providing the request id.
        uint32_t dc_id = HomeDCNode(meta);
        char *sendbuf = rdma_store_->GetSendBuf(dc_id);
        uint32_t msg_size = 0;
        msg_size += leveldb::EncodeStr(sendbuf, dbname);
        leveldb::EncodeFixed32(sendbuf + msg_size, file_number);
        msg_size += 4;
        leveldb::EncodeFixed64(sendbuf + msg_size, (uint64_t) result);
        rdma_store_->PostSend(sendbuf, msg_size, dc_id,
                              current_req_id_);

        DCRequestContext context = {};
        uint32_t req_id = current_req_id_;
        context.done = false;
        request_context_[current_req_id_] = context;
        current_req_id_++;
        return req_id;
    }

    uint32_t NovaDCClient::InitiateReadBlock(const std::string &dbname,
                                             uint64_t file_number,
                                             const leveldb::FileMetaData &meta,
                                             const leveldb::DCBlockHandle &block_handle,
                                             char *result) {
        std::vector<leveldb::DCBlockHandle> handles;
        handles.emplace_back(block_handle);
        return InitiateReadBlocks(dbname, file_number, meta, handles, result);
    }

    uint32_t NovaDCClient::InitiateReadBlocks(const std::string &dbname,
                                              uint64_t file_number,
                                              const leveldb::FileMetaData &meta,
                                              const std::vector<leveldb::DCBlockHandle> &block_handls,
                                              char *result) {
        // The request contains dbname, file number, block handles, and remote offset to accept the read blocks.
        // DC issues a WRITE_IMM to write the read blocks into the remote offset providing the request id.
        uint32_t dc_id = HomeDCNode(meta);
        char *sendbuf = rdma_store_->GetSendBuf(dc_id);
        uint32_t msg_size = 0;
        msg_size += leveldb::EncodeStr(sendbuf, dbname);
        leveldb::EncodeFixed32(sendbuf + msg_size, file_number);
        msg_size += 4;
        leveldb::EncodeFixed32(sendbuf + msg_size, block_handls.size());
        msg_size += 4;
        for (const auto &handle : block_handls) {
            leveldb::EncodeFixed64(sendbuf + msg_size, handle.offset);
            msg_size += 8;
            leveldb::EncodeFixed64(sendbuf + msg_size, handle.size);
            msg_size += 8;
        }
        leveldb::EncodeFixed64(sendbuf + msg_size, (uint64_t) result);
        rdma_store_->PostSend(sendbuf, msg_size, dc_id,
                              current_req_id_);
        uint32_t req_id = current_req_id_;
        DCRequestContext context = {};
        context.done = false;
        request_context_[current_req_id_] = context;
        current_req_id_++;
        return req_id;
    }

    bool NovaDCClient::IsDone(uint32_t req_id) {
        // Poll both queues.
        rdma_store_->PollRQ();
        rdma_store_->PollSQ();
        auto context_it = request_context_.find(req_id);
        RDMA_ASSERT(context_it != request_context_.end());
        context_it->second.done = true;
        if (context_it->second.done) {
            request_context_.erase(req_id);
            return true;
        }
        return false;
    }


    void
    NovaDCClient::OnRecv(ibv_wc_opcode type, uint64_t wr_id,
                         int remote_server_id,
                         char *buf,
                         uint32_t imm_data) {
        switch (type) {
            case IBV_WC_SEND:
                break;
            case IBV_WC_RDMA_WRITE:
                rdma_log_writer_->AckWriteSuccess(remote_server_id, wr_id);
                break;
            case IBV_WC_RDMA_READ:
                break;
            case IBV_WC_RECV:
            case IBV_WC_RECV_RDMA_WITH_IMM:
                if (buf[0] == DCRequestType::DC_ALLOCATE_LOG_BUFFER) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    std::string log_file(buf + 5, size);
                    char *buf = rdma_log_writer_->AllocateLogBuf(log_file);
                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
                    send_buf[0] = DCRequestType::DC_ALLOCATE_LOG_BUFFER_SUCC;
                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) buf);
                    leveldb::EncodeFixed64(send_buf + 9,
                                           nova::NovaConfig::config->log_buf_size);
                    rdma_store_->PostSend(send_buf, 1 + 8 + 8, remote_server_id,
                                          0);
                    rdma_store_->FlushPendingSends(remote_server_id);
                } else if (buf[0] ==
                           DCRequestType::DC_ALLOCATE_LOG_BUFFER_SUCC) {
                    uint64_t base = leveldb::DecodeFixed64(buf + 1);
                    uint64_t size = leveldb::DecodeFixed64(buf + 9);
                    rdma_log_writer_->AckAllocLogBuf(remote_server_id, base,
                                                     size);
                } else if (buf[0] == DCRequestType::DC_DELETE_LOG_FILE) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    std::string log_file(buf + 5, size);
                } else if (buf[0] == DCRequestType::DC_FLUSH_SSTABLE_BUF) {
                    uint64_t remote_dc_offset = leveldb::DecodeFixed64(buf + 1);
                    auto context_it = request_context_.find(imm_data);
                    RDMA_ASSERT(context_it != request_context_.end());
                    const DCRequestContext &context = context_it->second;
                    rdma_store_->PostWrite(
                            context.sstable_backing_mem,
                            context.meta.file_size,
                            remote_server_id, remote_dc_offset,
                            false, imm_data);
                } else if (buf[0] == DCRequestType::DC_FLUSH_SSTABLE_SUCC) {
                    auto context_it = request_context_.find(imm_data);
                    RDMA_ASSERT(context_it != request_context_.end());
                    context_it->second.done = true;
                } else if (buf[0] == DCRequestType::DC_READ_BLOCKS) {
                    auto context_it = request_context_.find(imm_data);
                    RDMA_ASSERT(context_it != request_context_.end());
                    context_it->second.done = true;
                } else if (buf[0] == DCRequestType::DC_READ_SSTABLE) {
                    auto context_it = request_context_.find(imm_data);
                    RDMA_ASSERT(context_it != request_context_.end());
                    context_it->second.done = true;
                } else {
                    RDMA_ASSERT(false) << "memstore[" << rdma_store_->store_id()
                                       << "]: unknown recv from "
                                       << remote_server_id << " buf:"
                                       << buf;
                }
                break;
        }
    }

}