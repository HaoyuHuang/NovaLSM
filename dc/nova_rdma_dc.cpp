
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <fmt/core.h>

#include "nova_rdma_dc.h"
#include "nova_dc_client.h"

namespace nova {
#define RDMA_POLL_MIN_TIMEOUT_US 1000
#define RDMA_POLL_MAX_TIMEOUT_US 10000

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

    NovaRDMADiskComponent::NovaRDMADiskComponent(rdmaio::RdmaCtrl *rdma_ctrl,
                                                 NovaMemManager *mem_manager,
                                                 leveldb::NovaDiskComponent *dc,
                                                 LogFileManager *log_manager)
            : rdma_ctrl_(rdma_ctrl),
              mem_manager_(mem_manager), dc_(dc),
              log_manager_(log_manager) {
    }

    // No need to flush RDMA requests since Flush will be done after all requests are processed in a receive queue.
    void
    NovaRDMADiskComponent::ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id,
                                         int remote_server_id, char *buf,
                                         uint32_t imm_data) {
        switch (type) {
            case IBV_WC_SEND:
                break;
            case IBV_WC_RDMA_WRITE:
                if (buf[0] == leveldb::DCRequestType::DC_READ_BLOCKS ||
                    buf[0] == leveldb::DCRequestType::DC_READ_SSTABLE) {
                    uint64_t allocated_buf_int = leveldb::DecodeFixed64(
                            buf + 1);
                    char *allocated_buf = (char *) (allocated_buf_int);
                    uint64_t size = leveldb::DecodeFixed64(
                            buf + 1 + 8);
                    uint32_t scid = mem_manager_->slabclassid(thread_id_, size);
                    mem_manager_->FreeItem(thread_id_, allocated_buf, scid);

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: imm:{} type:{} allocated buf:{} size:{}.",
                                thread_id_,
                                imm_data,
                                buf[0], allocated_buf_int, size);
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
                    // I sent this request a while ago and now it is complete.
                    auto &context = context_it->second;
                    if (context.request_type ==
                        leveldb::DCRequestType::DC_FLUSH_SSTABLE) {
                        // CC has written the SSTable to the provided buffer.
                        // NOW I can flush the buffer to disk and let CC know the SSTable if flushed.
                        leveldb::Slice footer_input(
                                context.buf + context.sstable_size -
                                leveldb::Footer::kEncodedLength,
                                leveldb::Footer::kEncodedLength);
                        leveldb::Footer footer;
                        leveldb::Status s = footer.DecodeFrom(&footer_input);
                        RDMA_ASSERT(s.ok()) << s.ToString();


                        dc_->FlushSSTable(context.db_name,
                                          context.file_number,
                                          context.buf,
                                          context.sstable_size);

                        uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                                  context.sstable_size);
                        mem_manager_->FreeItem(thread_id_, context.buf, scid);
                        char *sendbuf = rdma_store_->GetSendBuf(
                                context.remote_server_id);
                        sendbuf[0] = leveldb::DCRequestType::DC_FLUSH_SSTABLE_SUCC;
                        rdma_store_->PostSend(sendbuf, 1,
                                              context.remote_server_id,
                                              dc_req_id);
                        RDMA_LOG(DEBUG) << fmt::format(
                                    "dc[{}]: imm:{} ### Flushed SSTable db:{} fn:{} size:{}.",
                                    thread_id_,
                                    dc_req_id,
                                    context.db_name,
                                    context.file_number,
                                    context.sstable_size);
                    } else {
                        RDMA_ASSERT(false)
                            << fmt::format("Unknown request context {}.",
                                           context.request_type);
                    }
                    return;
                }


                // Inspect the buf.
                if (buf[0] == leveldb::DCRequestType::DC_ALLOCATE_LOG_BUFFER) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    std::string log_file(buf + 5, size);
                    uint32_t slabclassid = mem_manager_->slabclassid(thread_id_,
                                                                     nova::NovaConfig::config->log_buf_size);
                    char *buf = mem_manager_->ItemAlloc(thread_id_,
                                                        slabclassid);
                    log_manager_->Add(log_file, buf);
                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
                    send_buf[0] = RequestType::ALLOCATE_LOG_BUFFER_SUCC;
                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) buf);
                    leveldb::EncodeFixed64(send_buf + 9,
                                           NovaConfig::config->log_buf_size);
                    rdma_store_->PostSend(send_buf, 1 + 8 + 8, remote_server_id,
                                          0);
                } else if (buf[0] == RequestType::DELETE_LOG_FILE) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    std::string log_file(buf + 5, size);
                    log_manager_->DeleteLogBuf(thread_id_, log_file);
                } else if (buf[0] ==
                           leveldb::DCRequestType::DC_DELETE_LOG_FILE_SUCC) {

                } else if (buf[0] == leveldb::DCRequestType::DC_DELETE_TABLES) {
                    char *input = buf + 1;
                    std::string dbname;
                    uint32_t read_index = 0;
                    read_index += leveldb::DecodeStr(input, &dbname);
                    uint32_t nfiles = leveldb::DecodeFixed32(
                            input + read_index);
                    read_index += 4;
                    for (int i = 0; i < nfiles; i++) {
                        uint64_t tableid = leveldb::DecodeFixed64(
                                input + read_index);
                        read_index += 8;
                        dc_->DeleteTable(dbname, tableid);
                    }
                } else if (buf[0] == leveldb::DCRequestType::DC_FLUSH_SSTABLE) {
                    // The request contains dbname, file number, and sstable size.
                    // Return the allocated buffer offset.
                    char *input = buf + 1;
                    std::string dbname;
                    uint32_t read_index = 0;
                    read_index += leveldb::DecodeStr(input, &dbname);
                    uint32_t file_number = leveldb::DecodeFixed32(
                            input + read_index);
                    read_index += 4;
                    uint32_t sstable_size = leveldb::DecodeFixed32(
                            input + read_index);

                    uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                              sstable_size);
                    char *rdma_buf = mem_manager_->ItemAlloc(thread_id_, scid);
                    RDMA_ASSERT(rdma_buf != nullptr);

                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
                    RDMA_ASSERT(send_buf != nullptr);
                    send_buf[0] = leveldb::DCRequestType::DC_FLUSH_SSTABLE_BUF;
                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) (rdma_buf));

                    RequestContext context = {
                            .request_type = leveldb::DCRequestType::DC_FLUSH_SSTABLE,
                            .remote_server_id = (uint32_t) remote_server_id,
                            .db_name = dbname,
                            .file_number = file_number,
                            .buf = rdma_buf,
                            .sstable_size = sstable_size
                    };
                    request_context_map_[req_id] = context;
                    rdma_store_->PostSend(send_buf, 1 + 8, remote_server_id,
                                          dc_req_id);

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: imm:{} Flush SSTable db:{} fn:{} DC Buf: {}.",
                                thread_id_,
                                imm_data,
                                dbname,
                                file_number,
                                std::to_string((uint64_t) (buf)));
                } else if (buf[0] == leveldb::DCRequestType::DC_READ_BLOCKS) {
                    // The request contains dbname, file number, block handles, and remote offset to accept the read blocks.
                    // It issues a WRITE_IMM to write the read blocks into the remote offset providing the request id.
                    uint32_t read_index = 1;
                    char *input = buf;
                    std::string dbname;
                    read_index += leveldb::DecodeStr(input + read_index,
                                                     &dbname);
                    uint32_t file_number = leveldb::DecodeFixed32(
                            input + read_index);
                    read_index += 4;
                    uint32_t nblocks = leveldb::DecodeFixed32(
                            input + read_index);
                    read_index += 4;
                    std::vector<leveldb::DCBlockHandle> blocks;
                    uint32_t total_size = 0;
                    std::string handles;
                    for (int i = 0; i < nblocks; i++) {
                        leveldb::DCBlockHandle handle;
                        handle.offset = leveldb::DecodeFixed64(
                                input + read_index);
                        read_index += 8;
                        handle.size = leveldb::DecodeFixed64(
                                input + read_index);
                        read_index += 8;
                        total_size += handle.size;
                        blocks.push_back(handle);

                        handles.append(std::to_string(handle.offset));
                        handles.append(":");
                        handles.append(std::to_string(handle.size));
                        handles.append(",");
                    }
                    uint64_t remote_offset = leveldb::DecodeFixed64(
                            input + read_index);

                    uint32_t scid = mem_manager_->slabclassid(thread_id_, total_size);
                    char *rdma_buf = mem_manager_->ItemAlloc(thread_id_, scid);
                    RDMA_ASSERT(rdma_buf != nullptr);
                    uint64_t read_size = dc_->ReadBlocks(dbname,
                                                         file_number, blocks,
                                                         rdma_buf);
                    RDMA_ASSERT(total_size == read_size);

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: imm:{} Read {} blocks from SSTable db:{} fn:{} CCbuf:{} DCbuf:{}. Handles:{}. Size:{}.",
                                thread_id_,
                                imm_data,
                                nblocks,
                                dbname,
                                file_number, (uint64_t) (remote_offset),
                                (uint64_t) (buf),
                                handles, total_size);

                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
                    RDMA_ASSERT(send_buf != nullptr);
                    send_buf[0] = leveldb::DCRequestType::DC_READ_BLOCKS;
                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) (rdma_buf));
                    leveldb::EncodeFixed64(send_buf + 1 + 8, total_size);
                    rdma_store_->PostWrite(rdma_buf, total_size,
                                           remote_server_id,
                                           remote_offset, false, dc_req_id);
                } else if (buf[0] == leveldb::DCRequestType::DC_READ_SSTABLE) {
                    // The request contains dbname, file number, and remote offset to accept the read SSTable.
                    // It issues a WRITE_IMM to write the read SSTable into the remote offset providing the request id.
                    char *input = buf + 1;
                    std::string dbname;
                    uint32_t read_index = 4;
                    read_index += leveldb::DecodeStr(input, &dbname);
                    uint32_t file_number = leveldb::DecodeFixed32(
                            input + read_index);
                    read_index += 4;
                    uint64_t remote_offset = leveldb::DecodeFixed64(
                            input + read_index);
                    read_index += 8;
                    uint64_t tablesize = dc_->TableSize(dbname,
                                                        file_number);

                    uint32_t scid = mem_manager_->slabclassid(thread_id_, tablesize);
                    char *buf = mem_manager_->ItemAlloc(thread_id_, scid);
                    RDMA_ASSERT(buf != nullptr);
                    dc_->ReadSSTable(dbname, file_number, buf, tablesize);

                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
                    RDMA_ASSERT(send_buf != nullptr);
                    send_buf[0] = leveldb::DCRequestType::DC_READ_SSTABLE;
                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) (buf));
                    leveldb::EncodeFixed64(send_buf + 1 + 8, tablesize);
                    rdma_store_->PostWrite(buf, tablesize, remote_server_id,
                                           remote_offset, false, dc_req_id);

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: imm:{} Read SSTable db:{} fn:{}.",
                                thread_id_,
                                imm_data,
                                dbname, file_number);
                }
                break;
        }
    }

    void NovaRDMADiskComponent::Start() {
        RDMA_LOG(INFO) << "Async worker started";

        if (NovaConfig::config->enable_rdma) {
            rdma_store_->Init(rdma_ctrl_);
        }
        bool should_sleep = true;
        uint32_t timeout = RDMA_POLL_MIN_TIMEOUT_US;
        uint32_t n = 0;
        while (is_running_) {
            if (should_sleep) {
                usleep(timeout);
            }
            n = rdma_store_->PollSQ();
            n += rdma_store_->PollRQ();
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
}