
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_cc_client.h"
#include "nova/nova_config.h"
#include "nova/nova_common.h"

#include <fmt/core.h>

namespace leveldb {

    using namespace rdmaio;

    void NovaCCClient::IncrementReqId() {
        current_req_id_++;
        if (current_req_id_ == 0) {
            current_req_id_ = 1;
        }
    }

    uint32_t NovaCCClient::GetCurrentReqId() {
        return current_req_id_;
    }

    uint32_t NovaCCClient::InitiateDeleteTables(const std::string &dbname,
                                                const std::vector<uint64_t> &filenumbers) {
        uint32_t sid;
        uint32_t dbid;
        nova::ParseDBIndexFromDBName(dbname, &sid, &dbid);

        nova::CCFragment *frag = nova::NovaCCConfig::cc_config->db_fragment[dbid];
        for (int replica_id : frag->cc_server_ids) {
            if (replica_id == nova::NovaConfig::config->my_server_id) {
                continue;
            }

            char *send_buf = rdma_store_->GetSendBuf(replica_id);
            uint32_t msg_size = 1;
            send_buf[0] = CCRequestType::CC_DELETE_TABLES;
            msg_size += EncodeStr(send_buf + msg_size, dbname);
            EncodeFixed32(send_buf + msg_size, filenumbers.size());
            msg_size += 4;
            for (uint64_t fn : filenumbers) {
                EncodeFixed64(send_buf + msg_size, fn);
                msg_size += 8;
            }
            rdma_store_->PostSend(send_buf, msg_size, replica_id, 0);
        }
        return current_req_id_;
    }

    uint32_t NovaCCClient::InitiateWRITESSTableBuffer(uint32_t remote_server_id,
                                                      char *src, uint64_t dest,
                                                      uint64_t file_size) {
        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.done = false;
        char *sendbuf = rdma_store_->GetSendBuf(remote_server_id);
        sendbuf[0] = CCRequestType::CC_WRITE_REPLICATE_SSTABLE;
        EncodeFixed32(sendbuf + 1, req_id);
        context.wr_id = rdma_store_->PostWrite(src, file_size, remote_server_id,
                                               dest, false, req_id);
        request_context_[req_id] = context;
        return req_id;
    }

    uint32_t NovaCCClient::InitiateAllocateSSTableBuffer(
            uint32_t remote_server_id, const std::string &dbname,
            uint64_t file_number, uint64_t file_size) {
        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.done = false;
        uint32_t msg_size = 1;
        char *sendbuf = rdma_store_->GetSendBuf(remote_server_id);
        sendbuf[0] = CCRequestType::CC_ALLOCATE_SSTABLE_BUFFER;
        msg_size += EncodeStr(sendbuf + msg_size, dbname);
        EncodeFixed64(sendbuf + msg_size, file_number);
        msg_size += 8;
        EncodeFixed64(sendbuf + msg_size, file_size);
        msg_size += 8;

        rdma_store_->PostSend(sendbuf, msg_size, remote_server_id, req_id);
        request_context_[req_id] = context;
        return req_id;
    }

    uint32_t NovaCCClient::InitiateReleaseSSTableBuffer(
            uint32_t remote_server_id, const std::string &dbname,
            uint64_t file_number, uint64_t file_size) {
        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.done = false;
        uint32_t msg_size = 1;
        char *sendbuf = rdma_store_->GetSendBuf(remote_server_id);
        sendbuf[0] = CCRequestType::CC_RELEASE_SSTABLE_BUFFER;
        msg_size += EncodeStr(sendbuf + msg_size, dbname);
        EncodeFixed64(sendbuf + msg_size, file_number);
        msg_size += 8;
        EncodeFixed64(sendbuf + msg_size, file_size);
        msg_size += 8;

        rdma_store_->PostSend(sendbuf, msg_size, remote_server_id, req_id);
        request_context_[req_id] = context;
        return req_id;
    }

    uint32_t NovaCCClient::InitiateFlushSSTable(const std::string &dbname,
                                                uint64_t file_number,
                                                const leveldb::FileMetaData &meta,
                                                char *backing_mem) {
        uint32_t req_id = current_req_id_;
        uint32_t dc_id = HomeCCNode(meta);
        char *sendbuf = rdma_store_->GetSendBuf(dc_id);
        char *buf = sendbuf;
        buf[0] = CCRequestType::CC_FLUSH_SSTABLE;
        buf++;
        buf += leveldb::EncodeStr(buf, dbname);
        leveldb::EncodeFixed32(buf, file_number);
        buf += 4;
        leveldb::EncodeFixed32(buf, meta.file_size);
        uint32_t msg_size = 1 + 4 + dbname.size() + 4 + 4;
        rdma_store_->PostSend(sendbuf, msg_size, dc_id, req_id);

        CCRequestContext context = {};
        context.req_type = CCRequestType::CC_FLUSH_SSTABLE;
        context.done = false;
        context.remote_server_id = dc_id;
        context.size = meta.file_size;
        context.file_number = file_number;
        context.backing_mem = backing_mem;
        context.dbname = dbname;
        request_context_[current_req_id_] = context;
        IncrementReqId();
        rdma_store_->FlushPendingSends(dc_id);

        RDMA_LOG(DEBUG) << fmt::format(
                    "dcclient[{}]: req:{} Flush SSTable db:{} fn:{} smallest key:{} largest key:{} size:{} to DC node {}.",
                    cc_client_id_, req_id, dbname,
                    file_number,
                    meta.smallest.user_key().ToString(),
                    meta.largest.user_key().ToString(),
                    meta.file_size,
                    dc_id);
        return req_id;
    }

    uint32_t NovaCCClient::HomeCCNode(const leveldb::FileMetaData &meta) {
        uint64_t key = nova::keyhash(meta.smallest.user_key().data(),
                                     meta.smallest.user_key().size());
        nova::DCFragment *frag = nova::NovaDCConfig::home_fragment(key);
        RDMA_ASSERT(frag != nullptr);
        return frag->dc_server_id;
    }

    uint32_t NovaCCClient::InitiateReadSSTable(const std::string &dbname,
                                               uint64_t file_number,
                                               const leveldb::FileMetaData &meta,
                                               char *result) {
        // The request contains dbname, file number, and remote offset to accept the read SSTable.
        // DC issues a WRITE_IMM to write the read SSTable into the remote offset providing the request id.
        uint32_t req_id = current_req_id_;
        uint32_t dc_id = HomeCCNode(meta);
        char *sendbuf = rdma_store_->GetSendBuf(dc_id);
        sendbuf[0] = CCRequestType::CC_READ_SSTABLE;
        uint32_t msg_size = 1;
        msg_size += leveldb::EncodeStr(sendbuf + msg_size, dbname);
        leveldb::EncodeFixed32(sendbuf + msg_size, file_number);
        msg_size += 4;
        leveldb::EncodeFixed64(sendbuf + msg_size, meta.file_size);
        msg_size += 8;
        leveldb::EncodeFixed64(sendbuf + msg_size, (uint64_t) result);
        msg_size += 8;
        rdma_store_->PostSend(sendbuf, msg_size, dc_id, req_id);

        CCRequestContext context = {};
        context.req_type = CCRequestType::CC_READ_SSTABLE;
        context.remote_server_id = dc_id;
        context.size = meta.file_size;
        context.dbname = dbname;
        context.file_number = file_number;
        context.backing_mem = result;
        context.done = false;
        request_context_[req_id] = context;
        IncrementReqId();
        rdma_store_->FlushPendingSends(dc_id);

        RDMA_LOG(DEBUG) << fmt::format(
                    "dcclient[{}]: req:{} Read SSTable db:{} fn:{} size:{} from DC node {}.",
                    cc_client_id_, req_id, dbname, file_number, meta.file_size,
                    dc_id);
        return req_id;
    }

    uint32_t NovaCCClient::InitiateReadBlock(const std::string &dbname,
                                             uint64_t file_number,
                                             const leveldb::FileMetaData &meta,
                                             const leveldb::CCBlockHandle &block_handle,
                                             char *result) {
        std::vector<leveldb::CCBlockHandle> handles;
        handles.emplace_back(block_handle);
        return InitiateReadBlocks(dbname, file_number, meta, handles, result);
    }

    uint32_t NovaCCClient::InitiateDeleteFiles(const std::string &dbname,
                                               const std::vector<FileMetaData> &filenames) {
        // Does not wait for response.
        std::map<uint32_t, std::vector<uint64_t>> dc_files;
        for (const auto &file : filenames) {
            uint32_t dc_id = HomeCCNode(file);
            dc_files[dc_id].push_back(file.number);
        }

        std::string delete_fns;
        for (auto it = dc_files.begin(); it != dc_files.end(); it++) {
            uint32_t req_id = current_req_id_;
            char *sendbuf = rdma_store_->GetSendBuf(it->first);
            sendbuf[0] = CCRequestType::CC_DELETE_TABLES;
            uint32_t msg_size = 1;
            msg_size += leveldb::EncodeStr(sendbuf + msg_size, dbname);
            leveldb::EncodeFixed32(sendbuf + msg_size, it->second.size());
            msg_size += 4;
            for (int i = 0; i < it->second.size(); i++) {
                leveldb::EncodeFixed64(sendbuf + msg_size, it->second[i]);
                msg_size += 8;
                delete_fns.append(std::to_string(it->second[i]));
                delete_fns.append(",");
            }
            rdma_store_->PostSend(sendbuf, msg_size, it->first,
                                  req_id);
            RDMA_LOG(DEBUG) << fmt::format(
                        "dcclient[{}]: req:{} Delete files db:{} files {} at DC node {}.",
                        cc_client_id_, req_id, dbname, delete_fns, it->first);
            IncrementReqId();
        }
        return 0;
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

    uint32_t NovaCCClient::InitiateReadBlocks(const std::string &dbname,
                                              uint64_t file_number,
                                              const leveldb::FileMetaData &meta,
                                              const std::vector<leveldb::CCBlockHandle> &block_handles,
                                              char *result) {
        // The request contains dbname, file number, block handles, and remote offset to accept the read blocks.
        // DC issues a WRITE_IMM to write the read blocks into the remote offset providing the request id.
        uint32_t req_id = current_req_id_;
        uint32_t dc_id = HomeCCNode(meta);
        char *sendbuf = rdma_store_->GetSendBuf(dc_id);
        sendbuf[0] = CCRequestType::CC_READ_BLOCKS;
        uint32_t msg_size = 1;
        msg_size += leveldb::EncodeStr(sendbuf + msg_size, dbname);
        leveldb::EncodeFixed32(sendbuf + msg_size, file_number);
        msg_size += 4;
        leveldb::EncodeFixed32(sendbuf + msg_size, block_handles.size());
        msg_size += 4;
        uint32_t size = 0;
        std::string handles;
        for (const auto &handle : block_handles) {
            leveldb::EncodeFixed64(sendbuf + msg_size, handle.offset);
            msg_size += 8;
            leveldb::EncodeFixed64(sendbuf + msg_size, handle.size);
            msg_size += 8;
            size += handle.size;
            handles.append(std::to_string(handle.offset));
            handles.append(":");
            handles.append(std::to_string(handle.size));
            handles.append(",");
        }
        leveldb::EncodeFixed64(sendbuf + msg_size, (uint64_t) result);
        msg_size += 8;

        rdma_store_->PostSend(sendbuf, msg_size, dc_id, req_id);
        CCRequestContext context = {};
        context.req_type = CCRequestType::CC_READ_BLOCKS;
        context.dbname = dbname;
        context.remote_server_id = dc_id;
        context.size = size;
        context.file_number = file_number;
        context.backing_mem = result;
        context.done = false;
        request_context_[req_id] = context;
        IncrementReqId();
        rdma_store_->FlushPendingSends(dc_id);

        RDMA_LOG(DEBUG) << fmt::format(
                    "dcclient[{}]: req:{} Read {} blocks from SSTable db:{} fn:{} from DC node {}. Handles:{} Size:{} Buf:{}",
                    cc_client_id_, req_id, block_handles.size(), dbname,
                    file_number, dc_id, handles, size,
                    (uint64_t) (result));
        return req_id;
    }

    bool NovaCCClient::IsDone(uint32_t req_id, CCResponse *response) {
        // Poll both queues.
        rdma_store_->PollRQ();
        rdma_store_->PollSQ();

        auto context_it = request_context_.find(req_id);
        RDMA_ASSERT(context_it != request_context_.end());
        if (context_it->second.done) {
            if (response) {
                response->remote_sstable_buf = context_it->second.remote_sstable_buf;
            }
            request_context_.erase(req_id);
            return true;
        }
        return false;
    }

    void
    NovaCCClient::OnRecv(ibv_wc_opcode type, uint64_t wr_id,
                         int remote_server_id,
                         char *buf,
                         uint32_t imm_data) {
        uint32_t req_id = imm_data;
        switch (type) {
            case IBV_WC_SEND:
                break;
            case IBV_WC_RDMA_WRITE:
                if (buf[0] == CCRequestType::CC_WRITE_REPLICATE_SSTABLE) {
                    uint32_t req_id = leveldb::DecodeFixed32(buf + 1);
                    request_context_[req_id].done = true;
                }
                rdma_log_writer_->AckWriteSuccess(remote_server_id, wr_id);
                break;
            case IBV_WC_RDMA_READ:
                break;
            case IBV_WC_RECV:
            case IBV_WC_RECV_RDMA_WITH_IMM:
                RDMA_LOG(DEBUG)
                    << fmt::format("dcclient[{}]: received from {} imm:{}",
                                   cc_client_id_, remote_server_id, imm_data);
                auto context_it = request_context_.find(req_id);
                if (context_it != request_context_.end()) {
                    // I sent this request a while ago and now it is complete.
                    auto &context = context_it->second;
                    if (buf[0] ==
                        CCRequestType::CC_ALLOCATE_SSTABLE_BUFFER_SUCC) {
                        uint64_t mr = leveldb::DecodeFixed64(buf + 1);
                        context.remote_sstable_buf = mr;
                        context.done = true;
                    }
//                    else if (context.req_type == CCRequestType::CC_READ_BLOCKS ||
//                               context.req_type ==
//                               CCRequestType::CC_READ_SSTABLE ||
//                               buf[0] == CCRequestType::CC_FLUSH_SSTABLE_SUCC) {
//                        context.done = true;
//                    } else if (buf[0] == CCRequestType::CC_FLUSH_SSTABLE_BUF) {
//                        uint64_t remote_dc_offset = leveldb::DecodeFixed64(
//                                buf + 1);
//
//                        rdma_store_->PostWrite(
//                                context.backing_mem,
//                                context.size,
//                                remote_server_id, remote_dc_offset,
//                                false, req_id);
//                    } else {
//                        RDMA_ASSERT(false)
//                            << fmt::format("Unknown request context {}.",
//                                           context.req_type);
//                    }
                }

                if (buf[0] ==
                    CCRequestType::CC_ALLOCATE_LOG_BUFFER_SUCC) {
                    uint64_t base = leveldb::DecodeFixed64(buf + 1);
                    uint64_t size = leveldb::DecodeFixed64(buf + 9);
                    rdma_log_writer_->AckAllocLogBuf(remote_server_id, base,
                                                     size);
                }
//                else {
//                    RDMA_ASSERT(false) << fmt::format(
//                                "dcclient[{}]: unknown recv from {} imm:{} buf:{} ",
//                                cc_client_id_, remote_server_id, imm_data,
//                                buf[0]);
//                }
                break;
        }
    }

}