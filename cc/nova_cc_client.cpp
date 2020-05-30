
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

    NovaBlockCCClient::NovaBlockCCClient(uint32_t client_id,
                                         NovaRTableManager *rtable_manager)
            : rtable_manager_(rtable_manager) {
        sem_init(&sem_, 0, 0);
        current_cc_id_ = client_id;
    }

    uint32_t NovaBlockCCClient::InitiateCompaction(uint32_t remote_server_id,
                                                   leveldb::CompactionRequest *compaction_request) {
        RDMA_ASSERT(remote_server_id != nova::NovaConfig::config->my_server_id);
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMA_ASYNC_COMPACTION;
        task.server_id = remote_server_id;
        task.compaction_request = compaction_request;
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
        if (server_id == nova::NovaConfig::config->my_server_id) {
            std::string filename;
            if (file_number == 0) {
                filename = leveldb::DescriptorFileName(dbname, 0);
            } else {
                filename = leveldb::TableFileName(dbname,
                                                  file_number,
                                                  is_meta_blocks);
            }
            leveldb::NovaRTable *rtable = rtable_manager_->OpenRTable(
                    thread_id, filename);
            RDMA_ASSERT(rtable);
            uint64_t rtable_off = rtable->AllocateBuf(filename, size,
                                                      is_meta_blocks);
            RDMA_ASSERT(rtable_off != UINT64_MAX)
                << fmt::format("{} {}", filename, size);
            char *rtable_buf = (char *) rtable_off;
            memcpy(rtable_buf, buf, size);
            RDMA_ASSERT(rtable->MarkOffsetAsWritten(rtable->rtable_id(),
                                                    rtable_off)) << server_id;
            uint64_t persisted_bytes = rtable->Persist(rtable->rtable_id());
            RDMA_ASSERT(persisted_bytes == size)
                << fmt::format("persisted bytes:{} written bytes:{}",
                               persisted_bytes, size);
            leveldb::BlockHandle h = rtable->Handle(
                    filename, is_meta_blocks);
            leveldb::RTableHandle rh = {};
            rh.server_id = nova::NovaConfig::config->my_server_id;
            rh.rtable_id = rtable->rtable_id();
            rh.offset = h.offset();
            rh.size = h.size();
            if (file_number != 0) {
                RDMA_ASSERT(h.offset() == 0 && h.size() == size);
                rtable->ForceSeal();
            }
            uint32_t reqid = req_id_;
            CCResponse *response = new CCResponse;
            req_response[reqid] = response;
            req_id_++;
            response->rtable_handles.push_back(rh);
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("Wake up local write");
            sem_post(&sem_);
            return reqid;
        }

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
        if (server_id == nova::NovaConfig::config->my_server_id) {
            uint32_t reqid = req_id_;
            CCResponse *response = new CCResponse;
            req_response[reqid] = response;
            response->dc_queue_depth = nova::dc_stats.dc_queue_depth;
            response->dc_pending_write_bytes = nova::dc_stats.dc_pending_disk_writes;
            response->dc_pending_read_bytes = nova::dc_stats.dc_pending_disk_reads;
            req_id_++;
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("Wake up local read stats");
            sem_post(&sem_);
            return reqid;
        }

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
        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Read log file: dc:{} off:{} size:{} req:{}",
                    cc_client_id_, remote_server_id, remote_offset, size,
                    req_id);
        return req_id;
    }

    uint32_t NovaBlockCCClient::InitiateQueryLogFile(uint32_t storage_server_id,
                                                     uint32_t server_id,
                                                     uint32_t dbid,
                                                     std::unordered_map<std::string, uint64_t> *logfile_offset) {
        RDMA_ASSERT(server_id != nova::NovaConfig::config->my_server_id);
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_REQ_QUERY_LOG_FILES;
        task.server_id = storage_server_id;
        task.dbid = dbid;
        task.logfile_offset = logfile_offset;
        task.sem = &sem_;

        uint32_t reqid = req_id_;
        AddAsyncTask(task);
        req_id_++;
        return reqid;
    }

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
        if (server_id == nova::NovaConfig::config->my_server_id) {
            for (int i = 0; i < rtable_ids.size(); i++) {
                leveldb::NovaRTable *rtable = rtable_manager_->FindRTable(
                        rtable_ids[i].rtable_id);
                rtable->DeleteSSTable(rtable_ids[i].rtable_id,
                                      rtable_ids[i].sstable_id);
                leveldb::FileType type;
                RDMA_ASSERT(leveldb::ParseFileName(rtable_ids[i].sstable_id,
                                                   &type));
                if (type == leveldb::FileType::kTableFile) {
                    rtable_manager_->DeleteSSTable(
                            rtable_ids[i].sstable_id + "-meta");
                }

            }
            return 0;
        }

        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_REQ_DELETE_TABLES;
        task.server_id = server_id;
        task.rtable_ids = rtable_ids;
        AddAsyncTask(task);
        return 0;
    }

    uint32_t NovaBlockCCClient::InitiateFileNameRTableMapping(uint32_t stoc_id,
                                                              const std::unordered_map<std::string, uint32_t> &fn_rtableid) {
        if (stoc_id == nova::NovaConfig::config->my_server_id) {
            rtable_manager_->OpenRTables(fn_rtableid);
            sem_post(&sem_);
            return 0;
        }
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_FILENAME_RTABLE_MAPPING;
        task.server_id = stoc_id;
        task.fn_rtableid = fn_rtableid;
        task.sem = &sem_;
        AddAsyncTask(task);
        uint32_t reqid = req_id_;
        req_id_++;
        return reqid;
    }

    uint32_t NovaCCClient::InitiateFileNameRTableMapping(uint32_t stoc_id,
                                                         const std::unordered_map<std::string, uint32_t> &fn_rtableid) {
        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.done = false;
        context.req_type = CCRequestType::CC_FILENAME_RTABLEID;

        char *send_buf = rdma_store_->GetSendBuf(stoc_id);
        uint32_t msg_size = 1;
        send_buf[0] = CCRequestType::CC_FILENAME_RTABLEID;
        msg_size += EncodeFixed32(send_buf + msg_size, fn_rtableid.size());
        for (auto &it : fn_rtableid) {
            msg_size += EncodeStr(send_buf + msg_size, it.first);
            msg_size += EncodeFixed32(send_buf + msg_size, it.second);
        }

        rdma_store_->PostSend(send_buf, msg_size, stoc_id, req_id);
        request_context_[req_id] = context;
        IncrementReqId();
        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Inform Filename RTable ID stoc:{} size:{} req:{}",
                    cc_client_id_, stoc_id, fn_rtableid.size(), req_id);
        return req_id;
    }

    void NovaBlockCCClient::AddAsyncTask(
            const leveldb::RDMAAsyncClientRequestTask &task) {
        if (task.type == RDMAAsyncRequestType::RDMA_ASYNC_REQ_LOG_RECORD) {
            uint64_t id = task.dbid;
//                    (static_cast<uint64_t >(task.dbid) << 32) |
//                          task.memtable_id;
            ccs_[id % ccs_.size()]->AddTask(task);
            return;
        }
        uint32_t seq = NovaBlockCCClient::rdma_worker_seq_id_.fetch_add(1,
                                                                        std::memory_order_relaxed) %
                       ccs_.size();
        ccs_[seq]->AddTask(task);
    }

    uint32_t NovaBlockCCClient::InitiateRTableReadDataBlock(
            const leveldb::RTableHandle &rtable_handle, uint64_t offset,
            uint32_t size, char *result, uint32_t result_size,
            std::string filename,
            bool is_foreground_reads) {
        RDMA_ASSERT(size <= result_size)
            << fmt::format("{} {} {} {}", rtable_handle.DebugString(), filename,
                           size, result_size);
        if (rtable_handle.server_id == nova::NovaConfig::config->my_server_id) {
            RTableHandle converted_handle = {};
            uint32_t rtable_id = rtable_handle.rtable_id;
            if (!filename.empty()) {
                rtable_id = rtable_manager_->OpenRTable(0,
                                                        filename)->rtable_id();
            }
            Slice output;
            converted_handle.server_id = rtable_handle.server_id;
            converted_handle.rtable_id = rtable_id;
            converted_handle.offset = offset;
            converted_handle.size = size;
            rtable_manager_->ReadDataBlock(converted_handle,
                                           converted_handle.offset,
                                           converted_handle.size,
                                           result, &output);
//            RDMA_ASSERT(output.size() == converted_handle.size);
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("Wake up local read");
            sem_post(&sem_);
            uint32_t reqid = req_id_;
            req_id_++;
            return reqid;
        }

        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_REQ_READ;
        task.server_id = rtable_handle.server_id;
        task.rtable_handle = rtable_handle;
        task.offset = offset;
        task.size = size;
        task.result = result;
        task.write_size = result_size;
        task.filename = filename;
        task.sem = &sem_;
        task.is_foreground_reads = is_foreground_reads;
        AddAsyncTask(task);

        uint32_t reqid = req_id_;
        req_id_++;
        return reqid;
    }

    uint32_t NovaBlockCCClient::InitiateReplicateLogRecords(
            const std::string &log_file_name, uint64_t thread_id,
            uint32_t db_id, uint32_t memtable_id,
            char *rdma_backing_mem,
            const std::vector<LevelDBLogRecord> &log_records,
            WriteState *replicate_log_record_states) {
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_REQ_LOG_RECORD;
        task.log_file_name = log_file_name;
        task.thread_id = thread_id;
        task.dbid = db_id;
        task.memtable_id = memtable_id;
        task.log_records = log_records;
        task.write_buf = rdma_backing_mem;
        task.replicate_log_record_states = replicate_log_record_states;
        task.sem = &sem_;
        AddAsyncTask(task);
        return 0;
    }

    uint32_t NovaBlockCCClient::InitiateCloseLogFile(
            const std::string &log_file_name, uint32_t dbid) {
        RDMAAsyncClientRequestTask task = {};
        task.type = RDMAAsyncRequestType::RDMA_ASYNC_REQ_CLOSE_LOG;
        task.log_file_name = log_file_name;
        task.dbid = dbid;
        AddAsyncTask(task);
        RDMA_LOG(DEBUG) << fmt::format("Close {}", log_file_name);
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

    uint32_t NovaCCClient::InitiateQueryLogFile(uint32_t storage_server_id,
                                                uint32_t server_id,
                                                uint32_t dbid,
                                                std::unordered_map<std::string, uint64_t> *logfile_offset) {
        RDMA_ASSERT(server_id !=
                    nova::NovaConfig::config->my_server_id);

        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.req_type = CCRequestType::CC_QUERY_LOG_FILES;
        context.logfile_offset = logfile_offset;
        context.done = false;

        char *send_buf = rdma_store_->GetSendBuf(storage_server_id);
        uint32_t msg_size = 1;
        send_buf[0] = CCRequestType::CC_QUERY_LOG_FILES;
        EncodeFixed32(send_buf + msg_size, server_id);
        msg_size += 4;
        EncodeFixed32(send_buf + msg_size, dbid);
        msg_size += 4;

        rdma_store_->PostSend(send_buf, msg_size, storage_server_id,
                              req_id);
        request_context_[req_id] = context;
        IncrementReqId();

        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Query RTable server:{} db:{} req:{}",
                    cc_client_id_, storage_server_id,
                    dbid, req_id);
        return req_id;
    }

    uint32_t NovaCCClient::InitiateCompaction(uint32_t remote_server_id,
                                              leveldb::CompactionRequest *compaction_request) {
        RDMA_ASSERT(remote_server_id !=
                    nova::NovaConfig::config->my_server_id);
        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.req_type = CCRequestType::CC_COMPACTION;
        context.compaction = compaction_request;
        context.done = false;

        char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
        uint32_t msg_size = 1;
        send_buf[0] = CCRequestType::CC_COMPACTION;
        msg_size += compaction_request->EncodeRequest(send_buf + 1);

        rdma_store_->PostSend(send_buf, msg_size, remote_server_id, req_id);
        request_context_[req_id] = context;
        IncrementReqId();

        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Initiate compaction server:{}  req:{}",
                    cc_client_id_, remote_server_id, req_id);
        return req_id;
    }

    uint32_t NovaCCClient::InitiateRTableReadDataBlock(
            const leveldb::RTableHandle &rtable_handle, uint64_t offset,
            uint32_t size, char *result, uint32_t result_size,
            std::string filename, bool is_foreground_reads) {
        RDMA_ASSERT(rtable_handle.server_id !=
                    nova::NovaConfig::config->my_server_id);
        RDMA_ASSERT(size <= result_size);
        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.req_type = CCRequestType::CC_RTABLE_READ_BLOCKS;
        context.backing_mem = result;
        context.size = size;
        context.done = false;
        context.log_file_name = filename;

        char *send_buf = rdma_store_->GetSendBuf(rtable_handle.server_id);
        uint32_t msg_size = 1;
        send_buf[0] = CCRequestType::CC_RTABLE_READ_BLOCKS;
        msg_size += EncodeBool(send_buf + msg_size, is_foreground_reads);
        msg_size += EncodeFixed32(send_buf + msg_size, rtable_handle.rtable_id);
        msg_size += EncodeFixed64(send_buf + msg_size, offset);
        msg_size += EncodeFixed32(send_buf + msg_size, size);
        msg_size += EncodeFixed64(send_buf + msg_size, (uint64_t) result);
        msg_size += EncodeStr(send_buf + msg_size, filename);
        rdma_store_->PostSend(send_buf, msg_size, rtable_handle.server_id,
                              req_id);
        request_context_[req_id] = context;
        IncrementReqId();
        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Read RTable server:{} rtable:{} offset:{} size:{} off:{} size:{} fn:{} backing_mem:{} req:{}",
                    cc_client_id_, rtable_handle.server_id,
                    rtable_handle.rtable_id, rtable_handle.offset,
                    rtable_handle.size, offset, size, filename,
                    (uint64_t) result,
                    req_id);
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
        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Write RTable server:{} t:{} db:{} fn:{} size:{} req:{}",
                    cc_client_id_, server_id, thread_id, dbname, file_number,
                    size, req_id);
        return req_id;
    }

    uint32_t NovaCCClient::InitiateReplicateLogRecords(
            const std::string &log_file_name, uint64_t thread_id,
            uint32_t db_id, uint32_t memtable_id,
            char *rdma_backing_mem,
            const std::vector<LevelDBLogRecord> &log_records,
            WriteState *replicate_log_record_states) {
        uint32_t req_id = current_req_id_;
        CCRequestContext context = {};
        context.done = false;
        context.req_type = CCRequestType::CC_REPLICATE_LOG_RECORDS;
        context.log_file_name = log_file_name;
        context.thread_id = thread_id;
        context.db_id = db_id;
        context.memtable_id = memtable_id;
        context.replicate_log_record_states = replicate_log_record_states;
        context.log_record_mem = rdma_backing_mem;
        context.log_record_size = nova::LogRecordsSize(log_records);
        request_context_[req_id] = context;

        bool success = rdma_log_writer_->AddRecord(log_file_name,
                                                   thread_id, db_id,
                                                   memtable_id,
                                                   rdma_backing_mem,
                                                   log_records,
                                                   req_id,
                                                   replicate_log_record_states);
        IncrementReqId();
        if (!success) {
            request_context_.erase(req_id);
            return 0;
        }

        RDMA_LOG(DEBUG)
            << fmt::format(
                    "dcclient[{}]: Replicate log record req:{}",
                    cc_client_id_, req_id);
        return req_id;
    }

    uint32_t
    NovaCCClient::InitiateCloseLogFile(const std::string &log_file_name,
                                       uint32_t dbid) {
        uint32_t req_id = current_req_id_;
        rdma_log_writer_->CloseLogFile(log_file_name, dbid, req_id);
        IncrementReqId();
        return 0;
    }

    bool NovaCCClient::IsDone(uint32_t req_id, CCResponse *response,
                              uint64_t *timeout) {
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
                         uint32_t imm_data, bool *generate_a_new_request) {
        RDMA_ASSERT(generate_a_new_request);
        *generate_a_new_request = false;
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
            case IBV_WC_RDMA_WRITE: {
                if (buf[0] ==
                    leveldb::CCRequestType::CC_REPLICATE_LOG_RECORDS) {
                    req_id = leveldb::DecodeFixed32(buf + 1);
                    auto context_it = request_context_.find(req_id);
                    RDMA_ASSERT(context_it != request_context_.end()) << req_id;
                    auto &context = context_it->second;
                    if (rdma_log_writer_->AckWriteSuccess(context.log_file_name,
                                                          remote_server_id,
                                                          wr_id,
                                                          context.replicate_log_record_states)) {
                        RDMA_LOG(DEBUG) << fmt::format(
                                    "dcclient[{}]: Log record replicated req:{} wr_id:{} first:{}",
                                    cc_client_id_, req_id, wr_id, buf[0]);
                        bool complete = rdma_log_writer_->CheckCompletion(
                                context.log_file_name, context.db_id,
                                context.replicate_log_record_states);
                        if (complete) {
                            context.done = true;
                        }
                        processed = true;
                    }
                }
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
                        rdma_store_->PostWrite(
                                context.backing_mem,
                                context.size, remote_server_id,
                                rtable_offset, false, req_id);
                        *generate_a_new_request = true;
                        context.done = false;
                        context.rtable_id = rtable_id;
                        RDMA_LOG(DEBUG) << fmt::format(
                                    "dcclient[{}]: Write RTable received off id:{} offset:{} req:{}",
                                    cc_client_id_, rtable_id, rtable_offset,
                                    req_id);
                        processed = true;
                    } else if (context.req_type ==
                               CCRequestType::CC_RTABLE_READ_BLOCKS) {
                        if (context.log_file_name.empty()) {
                            RDMA_ASSERT(
                                    context.backing_mem[context.size - 1] != 0)
                                << context.log_file_name;
                        }

                        // Waiting for WRITEs.
                        if (nova::IsRDMAWRITEComplete(context.backing_mem,
                                                      context.size)) {
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
                               CCRequestType::CC_ALLOCATE_LOG_BUFFER_SUCC) {
                        uint64_t base = leveldb::DecodeFixed64(buf + 1);
                        uint64_t size = leveldb::DecodeFixed64(buf + 9);
                        rdma_log_writer_->AckAllocLogBuf(context.log_file_name,
                                                         remote_server_id, base,
                                                         size,
                                                         context.log_record_mem,
                                                         context.log_record_size,
                                                         req_id,
                                                         context.replicate_log_record_states);
                        RDMA_LOG(DEBUG) << fmt::format(
                                    "dcclient[{}]: Allocate log buffer success req:{}",
                                    cc_client_id_, req_id);
                        processed = true;
                    } else if (buf[0] ==
                               CCRequestType::CC_QUERY_LOG_FILES_RESPONSE) {
                        uint32_t read_size = 1;
                        uint32_t size = leveldb::DecodeFixed32(buf + read_size);
                        read_size += 4;
                        for (int i = 0; i < size; i++) {
                            std::string log;
                            read_size += leveldb::DecodeStr(buf + read_size,
                                                            &log);
                            uint64_t offset = leveldb::DecodeFixed64(
                                    buf + read_size);
                            read_size += 8;
                            (*context.logfile_offset)[log] = offset;
                        }
                        context.done = true;
                        processed = true;
                    } else if (buf[0] ==
                               CCRequestType::CC_FILENAME_RTABLEID_RESPONSE) {
                        context.done = true;
                        processed = true;
                    } else if (buf[0] ==
                               CCRequestType::CC_COMPACTION_RESPONSE) {
                        uint32_t num_outputs = leveldb::DecodeFixed32(buf + 1);
                        Slice outputs(buf + 5,
                                      nova::NovaConfig::config->max_msg_size);
                        for (int i = 0; i < num_outputs; i++) {
                            FileMetaData *meta = new FileMetaData;
                            RDMA_ASSERT(meta->Decode(&outputs, false));
                            context.compaction->outputs.push_back(meta);
                        }
                        context.done = true;
                        processed = true;
                    }
                }
                break;
        }
        return processed;
    }

}