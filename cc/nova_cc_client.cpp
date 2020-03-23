
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

    NovaBlockCCClient::NovaBlockCCClient(uint32_t client_id) {
        sem_init(&sem_, 0, 0);
        current_rdma_worker_id_ = client_id;
    }

    uint32_t NovaBlockCCClient::InitiateReadInMemoryLogFile(char *local_buf,
                                                            uint32_t remote_server_id,
                                                            uint64_t remote_offset,
                                                            uint64_t size) {
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_READ_LOG_FILE;
        task.rdma_log_record_backing_mem = local_buf;
        task.server_id = remote_server_id;
        task.remote_dc_offset = remote_offset;
        task.size = size;
        task.sem = &sem_;

        uint32_t reqid = req_id_;
        CCResponse *response = new CCResponse;
        req_response[reqid] = response;
        task.response = response;
        AddAsyncTask(task);

        req_id_++;
        return reqid;
    }

    uint32_t NovaBlockCCClient::InitiateSetupLogRecordBuf(uint32_t cc_id,
                                                          uint32_t cc_client_worker_id,
                                                          uint32_t log_record_size,
                                                          uint32_t dc_id) {
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_REQ_SETUP_LOG_BUF;
        task.dc_id = dc_id;
        task.cc_id = cc_id;
        task.size = log_record_size;
        task.cc_client_worker_id = cc_client_worker_id;
        task.sem = &sem_;

        uint32_t reqid = req_id_;
        CCResponse *response = new CCResponse;
        req_response[reqid] = response;
        task.response = response;
        AddAsyncTask(task);

        req_id_++;
        return reqid;
    }

    uint32_t NovaBlockCCClient::InitiateCloseLogFiles(uint32_t cc_id,
                                                      uint32_t dbid,
                                                      uint32_t dc_id,
                                                      std::vector<MemTableLogFilePair> log_file_ids) {
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_REQ_DELETE_LOG_FILES;
        task.cc_id = cc_id;
        task.dbid = dbid;
        task.dc_id = dc_id;
        task.log_file_ids = log_file_ids;
        task.sem = nullptr;
        AddAsyncTask(task);
        return 0;
    }

    uint32_t NovaBlockCCClient::InitiateSyncLogRecord(uint32_t cc_id,
                                                      uint32_t cc_worker_id,
                                                      uint32_t dbid,
                                                      uint32_t memtable_id,
                                                      const std::vector<Slice> &log_records,
                                                      uint32_t dc_id,
                                                      uint64_t remote_dc_offset,
                                                      char *rdma_log_record_backing_mem) {
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_SYNC_LOG_RECORD;
        task.cc_id = cc_id;
        task.cc_client_worker_id = cc_worker_id;
        task.dbid = dbid;
        task.memtable_id = memtable_id;
//        task.log_records = log_records;
        task.dc_id = dc_id;
        task.remote_dc_offset = remote_dc_offset;
        task.rdma_log_record_backing_mem = rdma_log_record_backing_mem;
        task.sem = &sem_;

        uint32_t reqid = req_id_;
        CCResponse *response = new CCResponse;
        req_response[reqid] = response;
        task.response = response;
        AddAsyncTask(task);
        req_id_++;
        return reqid;
    }

    uint32_t NovaBlockCCClient::InitiateRTableWriteDataBlocks(
            uint32_t server_id, uint32_t thread_id, uint32_t *rtable_id,
            char *buf, const std::string &dbname, uint64_t file_number,
            uint32_t size, bool is_meta_blocks) {
        RDMA_ASSERT(server_id != nova::NovaConfig::config->my_server_id);
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_REQ_WRITE_DATA_BLOCKS;
        task.server_id = server_id;
        task.thread_id = thread_id;
        task.write_buf = buf;
        task.dbname = dbname;
        task.file_number = file_number;
        task.write_size = size;
        task.sem = &sem_;
        task.is_meta_blocks = is_meta_blocks;

        uint32_t reqid = req_id_;
        CCResponse *response = new CCResponse;
        req_response[reqid] = response;
        task.response = response;
        AddAsyncTask(task);

        req_id_++;
        return reqid;
    }

    uint32_t NovaBlockCCClient::InitiateReadDCStats(uint32_t server_id) {
        RDMA_ASSERT(server_id != nova::NovaConfig::config->my_server_id);
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_READ_DC_STATS;
        task.server_id = server_id;
        task.sem = &sem_;

        uint32_t reqid = req_id_;
        CCResponse *response = new CCResponse;
        req_response[reqid] = response;
        task.response = response;
        AddAsyncTask(task);
        req_id_++;
        return reqid;
    };

    bool NovaBlockCCClient::IsDone(uint32_t req_id,
                                   leveldb::CCResponse *response,
                                   uint64_t *timeout) {
        auto it = req_response.find(req_id);
        if (it == req_response.end()) {
            return true;
        }
        RDMA_ASSERT(response);
        *response = *it->second;
        delete it->second;
        req_response.erase(req_id);
        return true;
    }

    uint32_t NovaBlockCCClient::InitiateDeleteTables(uint32_t server_id,
                                                     const std::vector<leveldb::SSTableRTablePair> &rtable_ids) {
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_REQ_DELETE_TABLES;
        task.server_id = server_id;
        task.rtable_ids = rtable_ids;
        AddAsyncTask(task);
        return 0;
    }


    void NovaBlockCCClient::AddAsyncTask(
            const leveldb::RDMAAsyncClientRequestTask &task) {
        if (task.type == RDMAAsyncRequestType::RDMA_ASYNC_REQ_LOG_RECORD) {
            uint32_t worker_id =
                    task.memtable_identifier.memtable_id % rdma_workers_.size();
            rdma_workers_[worker_id]->AddTask(task);
            return;
        }


        current_rdma_worker_id_ =
                (current_rdma_worker_id_ + 1) % rdma_workers_.size();
        rdma_workers_[current_rdma_worker_id_]->AddTask(task);
    }

    uint32_t NovaBlockCCClient::InitiateRTableReadDataBlock(
            const leveldb::RTableHandle &rtable_handle, uint64_t offset,
            uint32_t size, char *result) {
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_REQ_READ;
        task.rtable_handle = rtable_handle;
        task.offset = offset;
        task.size = size;
        task.result = result;
        task.sem = &sem_;
        AddAsyncTask(task);

        uint32_t reqid = req_id_;
        req_id_++;
        return reqid;
    }

    uint32_t NovaBlockCCClient::InitiateReplicateLogRecords(
            MemTableIdentifier memtable_id, uint64_t thread_id,
            const std::vector<LevelDBLogRecord> &log_records) {
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_REQ_LOG_RECORD;
        task.memtable_identifier = memtable_id;
        task.thread_id = thread_id;
        task.log_records = log_records;
        task.sem = &sem_;
        AddAsyncTask(task);

        RDMA_ASSERT(sem_wait(&sem_) == 0);
        return 0;
    }

    uint32_t NovaBlockCCClient::InitiateCloseLogFile(
            MemTableIdentifier memtable_id) {
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_REQ_CLOSE_LOG;
        task.memtable_identifier = memtable_id;
        task.sem = &sem_;
        AddAsyncTask(task);

        RDMA_ASSERT(sem_wait(&sem_) == 0);
        return 0;
    }


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
        RDMA_ASSERT(server_id != nova::NovaConfig::config->my_server_id);
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
        IncrementReqId();
        // Does not need to send immediately.
        return 0;
    }

    uint32_t NovaCCClient::InitiateReadInMemoryLogFile(char *local_buf,
                                                       uint32_t remote_server_id,
                                                       uint64_t remote_offset,
                                                       uint64_t size) {
        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.req_type = CCRequestType::CC_READ_IN_MEMORY_LOG_FILE;
        context.done = false;
        char *sendbuf = rdma_store_->GetSendBuf(remote_server_id);
        leveldb::EncodeFixed32(sendbuf, req_id);
        context.wr_id = rdma_store_->PostRead(local_buf, size, remote_server_id,
                                              0, remote_offset, false);

        request_context_[req_id] = context;
        IncrementReqId();

        // Flush immediately.
        rdma_store_->FlushPendingSends(remote_server_id);

        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Read log file: dc:{} off:{} size:{} req:{}",
                    cc_client_id_, remote_server_id, remote_offset, size,
                    req_id);
        return req_id;
    }

    uint32_t NovaCCClient::InitiateRTableReadDataBlock(
            const leveldb::RTableHandle &rtable_handle, uint64_t offset,
            uint32_t size, char *result) {
        RDMA_ASSERT(rtable_handle.server_id !=
                    nova::NovaConfig::config->my_server_id);
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

    uint32_t NovaCCClient::InitiateReadDCStats(uint32_t server_id) {
        RDMA_ASSERT(server_id != nova::NovaConfig::config->my_server_id);
        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.done = false;
        context.req_type = CCRequestType::CC_DC_READ_STATS;

        char *send_buf = rdma_store_->GetSendBuf(server_id);
        uint32_t msg_size = 1;
        send_buf[0] = CCRequestType::CC_DC_READ_STATS;
        rdma_store_->PostSend(send_buf, msg_size, server_id, req_id);
        request_context_[req_id] = context;
        IncrementReqId();

        rdma_store_->FlushPendingSends(server_id);
        return req_id;
    }

    uint32_t
    NovaCCClient::InitiateSetupLogRecordBuf(uint32_t cc_id,
                                            uint32_t cc_client_worker_id,
                                            uint32_t log_record_size,
                                            uint32_t dc_id) {
        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.done = false;
        context.req_type = CCRequestType::CC_SETUP_LOG_RECORD_BUF;

        char *send_buf = rdma_store_->GetSendBuf(dc_id);
        uint32_t msg_size = 1;
        send_buf[0] = CCRequestType::CC_SETUP_LOG_RECORD_BUF;
        EncodeFixed32(send_buf + msg_size, cc_id);
        msg_size += 4;
        EncodeFixed32(send_buf + msg_size, cc_client_worker_id);
        msg_size += 4;
        EncodeFixed32(send_buf + msg_size, log_record_size);
        msg_size += 4;
        rdma_store_->PostSend(send_buf, msg_size, dc_id, req_id);
        request_context_[req_id] = context;
        IncrementReqId();

        rdma_store_->FlushPendingSends(dc_id);
        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Setup Log record buf cc:{} t:{} size:{} dc:{} req:{}",
                    cc_client_id_, cc_id, cc_client_worker_id, log_record_size,
                    dc_id, req_id);
        return req_id;
    };

    uint32_t NovaCCClient::InitiateSyncLogRecord(uint32_t cc_id,
                                                 uint32_t cc_client_worker_id,
                                                 uint32_t dbid,
                                                 uint32_t memtable_id,
                                                 const std::vector<Slice> &log_records,
                                                 uint32_t dc_id,
                                                 uint64_t remote_dc_offset,
                                                 char *rdma_log_record_backing_mem) {
        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.done = false;
        context.req_type = CCRequestType::CC_SYNC_LOG_RECORD;

        // This is an RDMA WRITE. Encode the request id into the payload.
        uint32_t msg_size = 0;
        leveldb::EncodeFixed32(rdma_log_record_backing_mem, req_id);
        msg_size += 4;
        leveldb::EncodeFixed32(rdma_log_record_backing_mem + msg_size, cc_id);
        msg_size += 4;
        leveldb::EncodeFixed32(rdma_log_record_backing_mem + msg_size,
                               cc_client_worker_id);
        msg_size += 4;
        leveldb::EncodeFixed32(rdma_log_record_backing_mem + msg_size, dbid);
        msg_size += 4;
        leveldb::EncodeFixed32(rdma_log_record_backing_mem + msg_size,
                               memtable_id);
        msg_size += 4;

        uint32_t log_records_size = 0;
        for (auto log : log_records) {
            log_records_size += log.size();
        }
        leveldb::EncodeFixed32(rdma_log_record_backing_mem + msg_size,
                               log_records_size);
        msg_size += 4;
        for (auto log : log_records) {
            memcpy(rdma_log_record_backing_mem + msg_size, log.data(),
                   log.size());
            msg_size += log.size();
        }

        RDMA_ASSERT(msg_size < nova::NovaConfig::config->log_record_size);

        uint32_t imm_data = (uint32_t) 1 << 31;
        imm_data |= cc_client_worker_id;
        rdma_store_->PostWrite(rdma_log_record_backing_mem, msg_size, dc_id,
                               remote_dc_offset, false, imm_data);
        request_context_[req_id] = context;
        IncrementReqId();
        rdma_store_->FlushPendingSends(dc_id);
        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Sync log record buf:cc:{} t:{} dbid:{} mid:{} dcid:{} dcoffset:{} req:{}",
                    cc_client_id_, cc_id, cc_client_worker_id, dbid,
                    memtable_id,
                    dc_id, remote_dc_offset, req_id);
        return req_id;
    }


    uint32_t NovaCCClient::InitiateRTableWriteDataBlocks(uint32_t server_id,
                                                         uint32_t thread_id,
                                                         uint32_t *rtable_id,
                                                         char *buf,
                                                         const std::string &dbname,
                                                         uint64_t file_number,
                                                         uint32_t size,
                                                         bool is_meta_blocks) {
        RDMA_ASSERT(server_id != nova::NovaConfig::config->my_server_id);
//        if (server_id == nova::NovaConfig::config->my_server_id) {
//            std::string sstable_id = TableFileName(dbname, file_number);
//            NovaRTable *active_rtable = rtable_manager_->active_rtable(
//                    thread_id);
//            uint64_t offset = active_rtable->AllocateBuf(
//                    sstable_id, size);
//            if (offset == UINT64_MAX) {
//                active_rtable = rtable_manager_->CreateNewRTable(thread_id);
//                offset = active_rtable->AllocateBuf(sstable_id, size);
//            }
//            memcpy((char *) (offset), buf, size);
//            active_rtable->MarkOffsetAsWritten(offset);
//            *rtable_id = active_rtable->rtable_id();
//            return 0;
//        }

        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.done = false;
        context.req_type = CCRequestType::CC_RTABLE_WRITE_SSTABLE;

        char *send_buf = rdma_store_->GetSendBuf(server_id);
        uint32_t msg_size = 2;
        send_buf[0] = CCRequestType::CC_RTABLE_WRITE_SSTABLE;
        if (is_meta_blocks) {
            send_buf[1] = 'm';
        } else {
            send_buf[1] = 'd';
        }
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

    uint32_t NovaCCClient::InitiateReplicateLogRecords(
            MemTableIdentifier memtable_id, uint64_t thread_id,
            const std::vector<LevelDBLogRecord> &log_records) {
        rdma_log_writer_->AddRecord(memtable_id, thread_id, log_records);
        return 0;
    }

    uint32_t
    NovaCCClient::InitiateCloseLogFile(MemTableIdentifier memtable_id) {
        rdma_log_writer_->CloseLogFile(memtable_id);
        return 0;
    }

    uint32_t NovaCCClient::InitiateCloseLogFiles(uint32_t cc_id,
                                                 uint32_t dbid,
                                                 uint32_t dc_id,
                                                 std::vector<MemTableLogFilePair> log_file_ids) {
        char *sendbuf = rdma_store_->GetSendBuf(dc_id);
        uint32_t msg_size = 1;
        sendbuf[0] = leveldb::CCRequestType::CC_DELETE_LOG_FILES;
        leveldb::EncodeFixed32(sendbuf + msg_size, cc_id);
        msg_size += 4;
        leveldb::EncodeFixed32(sendbuf + msg_size, dbid);
        msg_size += 4;
        leveldb::EncodeFixed32(sendbuf + msg_size, log_file_ids.size());
        msg_size += 4;

        for (MemTableLogFilePair log_file_id : log_file_ids) {
            leveldb::EncodeFixed32(sendbuf + msg_size, log_file_id.memtable_id);
            msg_size += 4;
            leveldb::EncodeFixed32(sendbuf + msg_size, log_file_id.log_file_id);
            msg_size += 4;
        }
        rdma_store_->PostSend(sendbuf, msg_size, dc_id, 0);
        RDMA_LOG(DEBUG) << fmt::format(
                    "dcclient[{}]: Close log files cc:{} db:{} dc:{}",
                    cc_client_id_, cc_id, dbid, dc_id);
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
                response->log_file_id = context_it->second.log_file_id;
                response->rdma_log_buf = context_it->second.rdma_log_buf_offset;
                response->rtable_handles = context_it->second.rtable_handles;
                response->dc_queue_depth = context_it->second.dc_queue_depth;
                response->dc_pending_read_bytes = context_it->second.dc_pending_read_bytes;
                response->dc_pending_write_bytes = context_it->second.dc_pending_write_bytes;
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
            case IBV_WC_RDMA_READ: {
                uint32_t req_id = leveldb::DecodeFixed32(buf);
                auto context_it = request_context_.find(req_id);
                RDMA_ASSERT(context_it != request_context_.end());
                RDMA_ASSERT(context_it->second.wr_id == wr_id);
                context_it->second.done = true;
                RDMA_LOG(DEBUG) << fmt::format(
                            "dcclient[{}]: Read Log file complete req:{} wr_id:{}",
                            cc_client_id_, req_id, wr_id);
                processed = true;
            }
                break;
            case IBV_WC_SEND:
                break;
            case IBV_WC_RDMA_WRITE:
                if (rdma_log_writer_->AckWriteSuccess(remote_server_id,
                                                      wr_id)) {
                    RDMA_LOG(DEBUG) << fmt::format(
                                "dcclient[{}]: Log record replicated req:{} wr_id:{} first:{}",
                                cc_client_id_, req_id, wr_id, buf[0]);
                    processed = true;
                }
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
                                    CCRequestType::CC_RTABLE_WRITE_SSTABLE);
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
                        RDMA_ASSERT(rtable_handles == 1);
                        context.done = true;
                        RDMA_LOG(DEBUG) << fmt::format(
                                    "dcclient[{}]: Persist RTable received handles:{} rids:{} req:{}",
                                    cc_client_id_, rtable_handles, rids,
                                    req_id);
                        processed = true;
                    } else if (buf[0] ==
                               CCRequestType::CC_DC_READ_STATS_RESPONSE) {
                        context.dc_queue_depth = leveldb::DecodeFixed64(
                                buf + 1);
                        context.dc_pending_read_bytes = leveldb::DecodeFixed64(
                                buf + 9);
                        context.dc_pending_write_bytes = leveldb::DecodeFixed64(
                                buf + 17);
                        context.done = true;
                        processed = true;
                    } else if (buf[0] ==
                               CCRequestType::CC_SETUP_LOG_RECORD_BUF_RESPONSE) {
                        context.rdma_log_buf_offset = leveldb::DecodeFixed64(
                                buf + 1);
                        context.done = true;
                        processed = true;
                    } else if (buf[0] ==
                               CCRequestType::CC_SYNC_LOG_RECORD_RESPONSE) {
                        context.log_file_id = leveldb::DecodeFixed32(buf + 1);
                        context.done = true;
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