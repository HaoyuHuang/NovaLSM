
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
                               LogFileManager *log_manager)
            : rdma_ctrl_(rdma_ctrl),
              mem_manager_(mem_manager),
              rtable_manager_(rtable_manager),
              log_manager_(log_manager) {
        current_rtable_ = rtable_manager_->CreateNewRTable(thread_id_);
    }

    // No need to flush RDMA requests since Flush will be done after all requests are processed in a receive queue.
    void
    NovaCCServer::ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id,
                                int remote_server_id, char *buf,
                                uint32_t imm_data) {
        switch (type) {
            case IBV_WC_SEND:
                break;
            case IBV_WC_RDMA_WRITE:
                if (buf[0] == leveldb::CCRequestType::CC_RTABLE_READ_BLOCKS ||
                    buf[0] == leveldb::CCRequestType::CC_RTABLE_READ_SSTABLE) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    uint64_t allocated_buf_int = leveldb::DecodeFixed64(
                            buf + 5);
                    char *allocated_buf = (char *) (allocated_buf_int);
                    uint32_t scid = mem_manager_->slabclassid(thread_id_, size);
                    mem_manager_->FreeItem(thread_id_, allocated_buf, scid);

                    RDMA_LOG(DEBUG) << fmt::format(
                                "Free memory for read block tid:{} size:{} scid:{}",
                                thread_id_, size, scid);

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
                    auto &context = context_it->second;
                    if (context.request_type ==
                        leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE) {
                        context.rtable_id;
                        context.rtable_offset;
                        rtable_manager_->rtable(
                                context.rtable_id)->MarkOffsetAsWritten(
                                context.rtable_offset);
                    }
                }
//                    // I sent this request a while ago and now it is complete.
//                    auto &context = context_it->second;
//                    if (context.request_type ==
//                        leveldb::CCRequestType::CC_FLUSH_SSTABLE) {
//                        // CC has written the SSTable to the provided buffer.
//                        // NOW I can flush the buffer to disk and let CC know the SSTable if flushed.
//                        dc_->FlushSSTable(context.db_name,
//                                          context.file_number,
//                                          context.buf,
//                                          context.sstable_size);
//
//                        uint32_t scid = mem_manager_->slabclassid(thread_id_,
//                                                                  context.sstable_size);
//                        mem_manager_->FreeItem(thread_id_, context.buf, scid);
//                        RDMA_LOG(DEBUG) << fmt::format(
//                                    "Free memory for tid:{} db:{} fn:{} size:{}",
//                                    thread_id_, context.db_name,
//                                    context.file_number, context.sstable_size);
//
//                        char *sendbuf = rdma_store_->GetSendBuf(
//                                context.remote_server_id);
//                        sendbuf[0] = leveldb::CCRequestType::CC_FLUSH_SSTABLE_SUCC;
//                        rdma_store_->PostSend(sendbuf, 1,
//                                              context.remote_server_id,
//                                              dc_req_id);
//                        RDMA_LOG(DEBUG) << fmt::format(
//                                    "dc[{}]: imm:{} ### Flushed SSTable db:{} fn:{} size:{}.",
//                                    thread_id_,
//                                    dc_req_id,
//                                    context.db_name,
//                                    context.file_number,
//                                    context.sstable_size);
//                    } else {
//                        RDMA_ASSERT(false)
//                            << fmt::format("Unknown request context {}.",
//                                           context.request_type);
//                    }
//                    return;
//                }



                if (buf[0] == leveldb::CCRequestType::CC_DELETE_TABLES) {
                    std::string dbname;
                    uint32_t msg_size = 1;
                    msg_size += leveldb::DecodeStr(buf + msg_size, &dbname);
                    uint32_t nfiles = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;

                    std::vector<uint64_t> files;
                    for (int i = 0; i < nfiles; i++) {
                        files.push_back(leveldb::DecodeFixed64(buf + msg_size));
                        msg_size += 8;
                    }
                    rtable_manager_->DeleteSSTable(dbname, files);

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: Allocate sstable buffer. db:{} fn:{}",
                                thread_id_, dbname, nfiles);
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

                    leveldb::RTableHandle rtable_handle;
                    rtable_handle.server_id = nova::NovaConfig::config->my_server_id;
                    rtable_handle.rtable_id = rtable_id;
                    rtable_handle.offset = offset;
                    rtable_handle.size = size;
                    uint32_t scid = mem_manager_->slabclassid(thread_id_, size);
                    char *rdma_buf = mem_manager_->ItemAlloc(thread_id_, scid);
                    RDMA_ASSERT(rdma_buf);

                    rtable_manager_->ReadDataBlock(rtable_handle, rdma_buf);

                    char *sendbuf = rdma_store_->GetSendBuf(remote_server_id);
                    sendbuf[0] = leveldb::CCRequestType::CC_RTABLE_READ_BLOCKS;
                    leveldb::EncodeFixed32(sendbuf + 1, size);
                    leveldb::EncodeFixed64(sendbuf + 5, (uint64_t) (rdma_buf));
                    rdma_store_->PostWrite(rdma_buf, size, remote_server_id,
                                           cc_mr_offset, false, dc_req_id);
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_RTABLE_READ_SSTABLE) {
                    uint32_t msg_size = 1;
                    std::string dbname;
                    uint64_t file_number;
                    uint32_t size;
                    uint64_t cc_mr_offset;

                    msg_size += leveldb::DecodeStr(buf + msg_size, &dbname);
                    file_number = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;
                    size = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    cc_mr_offset = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;

                    uint32_t scid = mem_manager_->slabclassid(thread_id_, size);
                    char *rdma_buf = mem_manager_->ItemAlloc(thread_id_, scid);
                    RDMA_ASSERT(rdma_buf);

                    rtable_manager_->ReadDataBlocksOfSSTable(dbname,
                                                             file_number,
                                                             rdma_buf);
                    char *sendbuf = rdma_store_->GetSendBuf(remote_server_id);
                    sendbuf[0] = leveldb::CCRequestType::CC_RTABLE_READ_SSTABLE;
                    leveldb::EncodeFixed32(sendbuf + 1, size);
                    leveldb::EncodeFixed64(sendbuf + 5, (uint64_t) (rdma_buf));
                    rdma_store_->PostWrite(rdma_buf, size, remote_server_id,
                                           cc_mr_offset, false, dc_req_id);

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
                    leveldb::EncodeFixed32(sendbuf,
                                           current_rtable_->rtable_id());
                    leveldb::EncodeFixed64(sendbuf + 4, rtable_off);
                    rdma_store_->PostSend(sendbuf, 12, remote_server_id,
                                          dc_req_id);

                    RequestContext context = {};
                    context.request_type = leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE;
                    context.rtable_id = current_rtable_->rtable_id();
                    context.rtable_offset = rtable_off;

                    request_context_map_[req_id] = context;
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
                        leveldb::RTableHandle rh;
                        rh.server_id = NovaConfig::config->my_server_id;
                        rh.rtable_id = rtableid;
                        rh.offset = h.offset();
                        rh.size = h.size();
                        rtable_handles[i] = rh;
                    }

                    char *sendbuf = rdma_store_->GetSendBuf(remote_server_id);
                    msg_size = 0;
                    leveldb::EncodeFixed32(sendbuf + msg_size, nrtables);
                    for (int i = 0; i < nrtables; i++) {
                        rtable_handles[i].EncodeHandle(sendbuf + msg_size);
                        msg_size += leveldb::RTableHandle::HandleSize();
                    }

                    rdma_store_->PostSend(sendbuf, msg_size, remote_server_id,
                                          dc_req_id);

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
                    send_buf[0] = RequestType::ALLOCATE_LOG_BUFFER_SUCC;
                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) buf);
                    leveldb::EncodeFixed64(send_buf + 9,
                                           NovaConfig::config->log_buf_size);
                    rdma_store_->PostSend(send_buf, 1 + 8 + 8, remote_server_id,
                                          0);
                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: Allocate log buffer for file {}.",
                                thread_id_, log_file);
                } else if (buf[0] == RequestType::DELETE_LOG_FILE) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    std::string log_file(buf + 5, size);
                    log_manager_->DeleteLogBuf(log_file);
                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: Delete log buffer for file {}.",
                                thread_id_, log_file);
                }
//                else if (buf[0] == leveldb::CCRequestType::CC_DELETE_TABLES) {
//                    char *input = buf;
//                    std::string dbname;
//                    uint32_t read_index = 1;
//                    read_index += leveldb::DecodeStr(input + read_index,
//                                                     &dbname);
//                    uint32_t nfiles = leveldb::DecodeFixed32(
//                            input + read_index);
//                    read_index += 4;
//                    for (int i = 0; i < nfiles; i++) {
//                        uint64_t tableid = leveldb::DecodeFixed64(
//                                input + read_index);
//                        read_index += 8;
//                        dc_->DeleteTable(dbname, tableid);
//                    }
//                }
//                else if (buf[0] == leveldb::CCRequestType::CC_FLUSH_SSTABLE) {
//                    // The request contains dbname, file number, and sstable size.
//                    // Return the allocated buffer offset.
//                    char *input = buf + 1;
//                    std::string dbname;
//                    uint32_t read_index = 0;
//                    read_index += leveldb::DecodeStr(input, &dbname);
//                    uint32_t file_number = leveldb::DecodeFixed32(
//                            input + read_index);
//                    read_index += 4;
//                    uint32_t sstable_size = leveldb::DecodeFixed32(
//                            input + read_index);
//
//                    uint32_t scid = mem_manager_->slabclassid(thread_id_,
//                                                              sstable_size);
//                    char *rdma_buf = mem_manager_->ItemAlloc(thread_id_, scid);
//                    RDMA_LOG(DEBUG) << fmt::format(
//                                "Allocating memory for tid:{} db:{} fn:{} size:{}",
//                                thread_id_, dbname, file_number, sstable_size);
//                    RDMA_ASSERT(rdma_buf) << "Running out of memory";
//
//                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
//                    RDMA_ASSERT(send_buf != nullptr);
//                    send_buf[0] = leveldb::CCRequestType::CC_FLUSH_SSTABLE_BUF;
//                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) (rdma_buf));
//
//                    RequestContext context = {
//                            .request_type = leveldb::CCRequestType::CC_FLUSH_SSTABLE,
//                            .remote_server_id = (uint32_t) remote_server_id,
//                            .db_name = dbname,
//                            .file_number = file_number,
//                            .buf = rdma_buf,
//                            .sstable_size = sstable_size
//                    };
//                    request_context_map_[req_id] = context;
//                    rdma_store_->PostSend(send_buf, 1 + 8, remote_server_id,
//                                          dc_req_id);
//
//                    RDMA_LOG(DEBUG) << fmt::format(
//                                "dc[{}]: imm:{} Flush SSTable db:{} fn:{} DC Buf: {}.",
//                                thread_id_,
//                                imm_data,
//                                dbname,
//                                file_number,
//                                std::to_string((uint64_t) (buf)));
//                } else if (buf[0] == leveldb::CCRequestType::CC_READ_BLOCKS) {
//                    // The request contains dbname, file number, block handles, and remote offset to accept the read blocks.
//                    // It issues a WRITE_IMM to write the read blocks into the remote offset providing the request id.
//                    uint32_t read_index = 1;
//                    char *input = buf;
//                    std::string dbname;
//                    read_index += leveldb::DecodeStr(input + read_index,
//                                                     &dbname);
//                    uint32_t file_number = leveldb::DecodeFixed32(
//                            input + read_index);
//                    read_index += 4;
//                    uint32_t nblocks = leveldb::DecodeFixed32(
//                            input + read_index);
//                    read_index += 4;
//                    std::vector<leveldb::CCBlockHandle> blocks;
//                    uint32_t total_size = 0;
//                    std::string handles;
//                    for (int i = 0; i < nblocks; i++) {
//                        leveldb::CCBlockHandle handle;
//                        handle.offset = leveldb::DecodeFixed64(
//                                input + read_index);
//                        read_index += 8;
//                        handle.size = leveldb::DecodeFixed64(
//                                input + read_index);
//                        read_index += 8;
//                        total_size += handle.size;
//                        blocks.push_back(handle);
//
//                        handles.append(std::to_string(handle.offset));
//                        handles.append(":");
//                        handles.append(std::to_string(handle.size));
//                        handles.append(",");
//                    }
//                    uint64_t remote_offset = leveldb::DecodeFixed64(
//                            input + read_index);
//
//                    uint32_t scid = mem_manager_->slabclassid(thread_id_,
//                                                              total_size);
//                    char *rdma_buf = mem_manager_->ItemAlloc(thread_id_, scid);
//
//                    RDMA_LOG(DEBUG) << fmt::format(
//                                "Allocating memory for read block tid:{} db:{} fn:{} size:{}",
//                                thread_id_, dbname, file_number, total_size);
//
//                    RDMA_ASSERT(rdma_buf) << "Running out of memory";
//
//                    uint64_t read_size = dc_->ReadBlocks(dbname,
//                                                         file_number, blocks,
//                                                         rdma_buf);
//                    RDMA_ASSERT(total_size == read_size);
//
//                    RDMA_LOG(DEBUG) << fmt::format(
//                                "dc[{}]: imm:{} Read {} blocks from SSTable db:{} fn:{} CCbuf:{} DCbuf:{}. Handles:{}. Size:{}.",
//                                thread_id_,
//                                imm_data,
//                                nblocks,
//                                dbname,
//                                file_number, (uint64_t) (remote_offset),
//                                (uint64_t) (buf),
//                                handles, total_size);
//
//                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
//                    RDMA_ASSERT(send_buf != nullptr);
//                    send_buf[0] = leveldb::CCRequestType::CC_READ_BLOCKS;
//                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) (rdma_buf));
//                    leveldb::EncodeFixed64(send_buf + 1 + 8, total_size);
//                    rdma_store_->PostWrite(rdma_buf, total_size,
//                                           remote_server_id,
//                                           remote_offset, false, dc_req_id);
//                } else if (buf[0] == leveldb::CCRequestType::CC_READ_SSTABLE) {
//                    // The request contains dbname, file number, and remote offset to accept the read SSTable.
//                    // It issues a WRITE_IMM to write the read SSTable into the remote offset providing the request id.
//                    char *input = buf;
//                    std::string dbname;
//                    uint32_t read_index = 1;
//                    read_index += leveldb::DecodeStr(input + read_index,
//                                                     &dbname);
//                    uint32_t file_number = leveldb::DecodeFixed32(
//                            input + read_index);
//                    read_index += 4;
//                    uint64_t file_size = leveldb::DecodeFixed64(
//                            input + read_index);
//                    read_index += 8;
//                    uint64_t remote_offset = leveldb::DecodeFixed64(
//                            input + read_index);
//                    read_index += 8;
//                    uint64_t tablesize = dc_->TableSize(dbname,
//                                                        file_number);
//
//                    RDMA_LOG(DEBUG) << fmt::format(
//                                "dc[{}]: Read SSTable db:{} fn:{} size:{} offset:{}",
//                                thread_id_, dbname, file_number, file_size,
//                                remote_offset);
//
//                    RDMA_ASSERT(tablesize == file_size) << fmt::format(
//                                "dc[{}]: Read SSTable size mismatch. db:{} fn:{} DC:{} CC:{}",
//                                thread_id_, dbname, file_number, tablesize,
//                                file_size);
//
//                    uint32_t scid = mem_manager_->slabclassid(thread_id_,
//                                                              tablesize);
//                    char *rdma_buf = mem_manager_->ItemAlloc(thread_id_, scid);
//                    RDMA_ASSERT(rdma_buf) << "Running out of memory";
//                    dc_->ReadSSTable(dbname, file_number, rdma_buf, tablesize);
//
//                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
//                    RDMA_ASSERT(send_buf != nullptr);
//                    send_buf[0] = leveldb::CCRequestType::CC_READ_SSTABLE;
//                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) (rdma_buf));
//                    leveldb::EncodeFixed64(send_buf + 1 + 8, tablesize);
//                    rdma_store_->PostWrite(rdma_buf, tablesize,
//                                           remote_server_id,
//                                           remote_offset, false, dc_req_id);
//
//                    RDMA_LOG(DEBUG) << fmt::format(
//                                "dc[{}]: imm:{} Read SSTable db:{} fn:{}.",
//                                thread_id_,
//                                imm_data,
//                                dbname, file_number);
//                }

                // Inspect the buf.
                break;
        }
    }
}