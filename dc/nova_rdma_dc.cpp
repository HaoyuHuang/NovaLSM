
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_rdma_dc.h"
#include "nova_dc_client.h"

namespace nova {
#define RDMA_POLL_MIN_TIMEOUT_US 1000
#define RDMA_POLL_MAX_TIMEOUT_US 10000

    NovaRDMADiskComponent::NovaRDMADiskComponent(rdmaio::RdmaCtrl *rdma_ctrl,
                                                 NovaMemManager *mem_manager,
                                                 leveldb::NovaDiskComponent *dc,
                                                 LogFileManager *log_manager)
            : rdma_ctrl_(rdma_ctrl),
              mem_manager_(mem_manager), dc_(dc),
              log_manager_(log_manager) {
    }

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
                    char *buf = (char *) leveldb::DecodeFixed64(buf + 1);
                    uint64_t size = leveldb::DecodeFixed64(buf + 1 + 8);
                    uint32_t scid = mem_manager_->slabclassid(size);
                    mem_manager_->FreeItem(buf, scid);
                }
                break;
            case IBV_WC_RDMA_READ:
                break;
            case IBV_WC_RECV:
            case IBV_WC_RECV_RDMA_WITH_IMM:
                uint32_t req_id = imm_data;
                auto context = request_context_map_.find(req_id);
                if (context != request_context_map_.end()) {
                    if (context->second.request_type ==
                        leveldb::DCRequestType::DC_FLUSH_SSTABLE) {
                        // CC has written the SSTable to the provided buffer.
                        // NOW I can flush the buffer to disk and let CC know the SSTable if flushed.
                        dc_->FlushSSTable(context->second.db_name,
                                          context->second.file_number,
                                          context->second.buf,
                                          context->second.sstable_size);
                        uint32_t scid = mem_manager_->slabclassid(
                                context->second.sstable_size);
                        mem_manager_->FreeItem(context->second.buf, scid);
                        char *sendbuf = rdma_store_->GetSendBuf(
                                remote_server_id);
                        sendbuf[0] = leveldb::DCRequestType::DC_FLUSH_SSTABLE_SUCC;
                        rdma_store_->PostSend(sendbuf, 1, remote_server_id,
                                              req_id);
                        request_context_map_.erase(req_id);
                    }
                    break;
                }

                // Inspect the buf.
                if (buf[0] == leveldb::DCRequestType::DC_ALLOCATE_LOG_BUFFER) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    std::string log_file(buf + 5, size);
                    uint32_t slabclassid = mem_manager_->slabclassid(
                            nova::NovaConfig::config->log_buf_size);
                    char *buf = mem_manager_->ItemAlloc(slabclassid);
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
                    log_manager_->DeleteLogBuf(log_file);
                } else if (buf[0] ==
                           leveldb::DCRequestType::DC_DELETE_LOG_FILE_SUCC) {

                } else if (buf[0] == leveldb::DCRequestType::DC_DELETE_TABLES) {
                    char *input = buf + 1;
                    std::string dbname;
                    uint32_t read_index = 0;
                    read_index += leveldb::DecodeStr(input, &dbname);
                    uint32_t nfiles = leveldb::DecodeFixed32(
                            input + read_index);

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

                    uint32_t scid = mem_manager_->slabclassid(sstable_size);
                    char *buf = mem_manager_->ItemAlloc(scid);
                    RDMA_ASSERT(buf != nullptr);

                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
                    RDMA_ASSERT(send_buf != nullptr);
                    send_buf[0] = leveldb::DCRequestType::DC_FLUSH_SSTABLE_BUF;
                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) (send_buf));

                    RequestContext context = {
                            .request_type = leveldb::DCRequestType::DC_FLUSH_SSTABLE,
                            .db_name = dbname,
                            .file_number = file_number,
                            .buf = buf,
                            .sstable_size = sstable_size
                    };
                    request_context_map_[req_id] = context;
                    rdma_store_->PostSend(send_buf, 1 + 8, remote_server_id,
                                          req_id);
                } else if (buf[0] == leveldb::DCRequestType::DC_READ_BLOCKS) {
                    // The request contains dbname, file number, block handles, and remote offset to accept the read blocks.
                    // It issues a WRITE_IMM to write the read blocks into the remote offset providing the request id.
                    uint32_t read_index = 1;
                    char *input = buf + read_index;
                    std::string dbname;

                    read_index += leveldb::DecodeStr(input, &dbname);
                    uint32_t file_number = leveldb::DecodeFixed32(input);
                    read_index += 4;
                    uint32_t nblocks = leveldb::DecodeFixed32(
                            input + read_index);
                    std::vector<leveldb::DCBlockHandle> blocks;
                    uint32_t total_size = 0;
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
                    }
                    uint64_t remote_offset = leveldb::DecodeFixed64(
                            input + read_index);

                    uint32_t scid = mem_manager_->slabclassid(total_size);
                    char *buf = mem_manager_->ItemAlloc(scid);
                    RDMA_ASSERT(buf != nullptr);
                    uint64_t read_size = dc_->ReadBlocks(dbname,
                                                         file_number, blocks,
                                                         buf);
                    RDMA_ASSERT(total_size == read_size);

                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
                    RDMA_ASSERT(send_buf != nullptr);
                    send_buf[0] = leveldb::DCRequestType::DC_READ_BLOCKS;
                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) (buf));
                    leveldb::EncodeFixed64(send_buf + 1 + 8, total_size);
                    rdma_store_->PostWrite(buf, total_size, remote_server_id,
                                           remote_offset, false, req_id);
                } else if (buf[0] == leveldb::DCRequestType::DC_READ_SSTABLE) {
                    // The request contains dbname, file number, and remote offset to accept the read SSTable.
                    // It issues a WRITE_IMM to write the read SSTable into the remote offset providing the request id.
                    leveldb::Slice input(buf + 1);
                    leveldb::Slice dbname;
                    RDMA_ASSERT(
                            leveldb::GetLengthPrefixedSlice(&input, &dbname));
                    uint32_t file_number = leveldb::DecodeFixed32(input.data());
                    uint32_t read_index = 4;
                    uint64_t remote_offset = leveldb::DecodeFixed64(
                            input.data() + read_index);

                    std::string dbname_str = dbname.ToString();
                    uint64_t tablesize = dc_->TableSize(dbname_str,
                                                        file_number);

                    uint32_t scid = mem_manager_->slabclassid(tablesize);
                    char *buf = mem_manager_->ItemAlloc(scid);
                    RDMA_ASSERT(buf != nullptr);
                    dc_->ReadSSTable(dbname_str, file_number, buf, tablesize);

                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
                    RDMA_ASSERT(send_buf != nullptr);
                    send_buf[0] = leveldb::DCRequestType::DC_READ_SSTABLE;
                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) (buf));
                    leveldb::EncodeFixed64(send_buf + 1 + 8, tablesize);
                    rdma_store_->PostWrite(buf, tablesize, remote_server_id,
                                           remote_offset, false, req_id);
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