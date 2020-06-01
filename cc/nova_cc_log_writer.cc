
//
// Created by Haoyu Huang on 12/23/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova/nova_config.h"
#include "nova_cc_log_writer.h"


namespace leveldb {


    // Create a writer that will append data to "*dest".
// "*dest" must be initially empty.
// "*dest" must remain live while this Writer is in use.
    RDMALogWriter::RDMALogWriter(nova::NovaRDMAStore *store,
                                 MemManager *mem_manager,
                                 nova::InMemoryLogFileManager *log_manager)
            : store_(store), mem_manager_(mem_manager),
              log_manager_(log_manager) {
    }

    void RDMALogWriter::Init(const std::string &log_file_name,
                             uint64_t thread_id,
                             const std::vector<LevelDBLogRecord> &log_records,
                             char *backing_buf) {
        auto it = logfile_last_buf_.find(log_file_name);
        if (it == logfile_last_buf_.end()) {
            LogFileMetadata meta = {};
            meta.stoc_bufs = new LogFileBuf[nova::NovaConfig::config->servers.size()];
            for (int i = 0;
                 i <
                 nova::NovaConfig::config->servers.size(); i++) {
                meta.stoc_bufs[i].base = 0;
                meta.stoc_bufs[i].offset = 0;
                meta.stoc_bufs[i].size = 0;
            }
            logfile_last_buf_[log_file_name] = meta;
        }
        uint32_t size = 0;
        for (const auto &record : log_records) {
            size += nova::EncodeLogRecord(backing_buf + size, record);
        }
    }

    void RDMALogWriter::AckAllocLogBuf(const std::string &log_file_name,
                                       int stoc_server_id, uint64_t offset,
                                       uint64_t size,
                                       char *backing_mem,
                                       uint32_t log_record_size,
                                       uint32_t client_req_id,
                                       WriteState *replicate_log_record_states) {
        replicate_log_record_states[stoc_server_id].result = WriteResult::ALLOC_SUCCESS;
        auto meta = &logfile_last_buf_[log_file_name];
        meta->stoc_bufs[stoc_server_id].base = offset;
        meta->stoc_bufs[stoc_server_id].size = size;
        meta->stoc_bufs[stoc_server_id].offset = 0;
        meta->stoc_bufs[stoc_server_id].is_initializing = false;
        admission_control_->AddRequests(stoc_server_id, 1);
        char *sendbuf = store_->GetSendBuf(stoc_server_id);
        sendbuf[0] = leveldb::CCRequestType::CC_REPLICATE_LOG_RECORDS;
        leveldb::EncodeFixed32(sendbuf + 1, client_req_id);
        replicate_log_record_states[stoc_server_id].rdma_wr_id = store_->PostWrite(
                backing_mem, log_record_size,
                stoc_server_id,
                meta->stoc_bufs[stoc_server_id].base +
                meta->stoc_bufs[stoc_server_id].offset, /*is_remote_offset=*/
                false, 0);
        meta->stoc_bufs[stoc_server_id].offset += log_record_size;
        replicate_log_record_states[stoc_server_id].result = WriteResult::WAIT_FOR_WRITE;
    }

    bool RDMALogWriter::AckWriteSuccess(const std::string &log_file_name,
                                        int remote_sid, uint64_t wr_id,
                                        WriteState *replicate_log_record_states) {
        WriteState &state = replicate_log_record_states[remote_sid];
        if (state.rdma_wr_id == wr_id &&
            state.result == WriteResult::WAIT_FOR_WRITE) {
            state.result = WriteResult::WRITE_SUCESS;
            return true;
        }
        return false;
    }

    bool
    RDMALogWriter::AddRecord(const std::string &log_file_name,
                             uint64_t thread_id,
                             uint32_t dbid,
                             uint32_t memtableid,
                             char *rdma_backing_buf,
                             const std::vector<LevelDBLogRecord> &log_records,
                             uint32_t client_req_id,
                             WriteState *replicate_log_record_states) {
        nova::CCFragment *frag = nova::NovaConfig::config->db_fragment[dbid];
        if (frag->log_replica_stoc_ids.empty()) {
            return true;
        }
        Init(log_file_name, thread_id, log_records, rdma_backing_buf);
        uint32_t log_record_size = nova::LogRecordsSize(log_records);
        // If one of the log buf is intializing, return false.
        for (int i = 0; i < frag->log_replica_stoc_ids.size(); i++) {
            uint32_t stoc_server_id = nova::NovaConfig::config->dc_servers[frag->log_replica_stoc_ids[i]].server_id;
            auto &it = logfile_last_buf_[log_file_name];
            if (it.stoc_bufs[stoc_server_id].is_initializing) {
                return false;
            }
        }

        for (int i = 0; i < frag->log_replica_stoc_ids.size(); i++) {
            uint32_t stoc_server_id = nova::NovaConfig::config->dc_servers[frag->log_replica_stoc_ids[i]].server_id;
            auto &it = logfile_last_buf_[log_file_name];
            if (it.stoc_bufs[stoc_server_id].base == 0) {
                it.stoc_bufs[stoc_server_id].is_initializing = true;
                // Allocate a new buf.
                char *send_buf = store_->GetSendBuf(stoc_server_id);
                char *buf = send_buf;
                buf[0] = CCRequestType::CC_ALLOCATE_LOG_BUFFER;
                buf++;
                leveldb::EncodeFixed32(buf, log_file_name.size());
                buf += 4;
                memcpy(buf, log_file_name.data(), log_file_name.size());
                replicate_log_record_states[stoc_server_id].result = WriteResult::WAIT_FOR_ALLOC;
                store_->PostSend(
                        send_buf, 1 + 4 + log_file_name.size(),
                        stoc_server_id, client_req_id);
            } else {
                RDMA_ASSERT(!it.stoc_bufs[stoc_server_id].is_initializing);
                RDMA_ASSERT(
                        it.stoc_bufs[stoc_server_id].offset + log_record_size <=
                        it.stoc_bufs[stoc_server_id].size);
                // WRITE.
                char *sendbuf = store_->GetSendBuf(stoc_server_id);
                sendbuf[0] = leveldb::CCRequestType::CC_REPLICATE_LOG_RECORDS;
                leveldb::EncodeFixed32(sendbuf + 1, client_req_id);
                replicate_log_record_states[stoc_server_id].rdma_wr_id = store_->PostWrite(
                        rdma_backing_buf, log_record_size, stoc_server_id,
                        it.stoc_bufs[stoc_server_id].base +
                        it.stoc_bufs[stoc_server_id].offset,
                        false, 0);
                it.stoc_bufs[stoc_server_id].offset += log_record_size;
                replicate_log_record_states[stoc_server_id].result = WriteResult::WAIT_FOR_WRITE;
            }
        }
        return true;
    }

    bool RDMALogWriter::CheckCompletion(const std::string &log_file_name,
                                        uint32_t dbid,
                                        WriteState *replicate_log_record_states) {
        nova::CCFragment *frag = nova::NovaConfig::config->db_fragment[dbid];
        // Pull all pending writes.
        int acks = 0;
        int total_states = 0;
        LogFileMetadata *meta = nullptr;
        char *sendbuf = nullptr;
        for (int i = 0; i < frag->log_replica_stoc_ids.size(); i++) {
            uint32_t stoc_server_id = nova::NovaConfig::config->dc_servers[frag->log_replica_stoc_ids[i]].server_id;
            switch (replicate_log_record_states[stoc_server_id].result) {
                case WriteResult::REPLICATE_LOG_RECORD_NONE:
                    break;
                case WriteResult::WAIT_FOR_ALLOC:
                    total_states += 1;
                    break;
                case WriteResult::WAIT_FOR_WRITE:
                    total_states += 1;
                    break;
                case WriteResult::ALLOC_SUCCESS:
                    total_states += 1;
                    break;
                case WriteResult::WRITE_SUCESS:
                    total_states += 1;
                    acks++;
                    break;
            }
        }
        RDMA_ASSERT(total_states == frag->log_replica_stoc_ids.size());
        return acks == frag->log_replica_stoc_ids.size();
    }

    Status
    RDMALogWriter::CloseLogFiles(const std::vector<std::string> &log_file_name,
                                uint32_t dbid, uint32_t client_req_id) {
        nova::CCFragment *frag = nova::NovaConfig::config->db_fragment[dbid];
        for (const auto &logfile : log_file_name) {
            LogFileMetadata *meta = &logfile_last_buf_[logfile];
            delete meta->stoc_bufs;
            logfile_last_buf_.erase(logfile);
        }
        log_manager_->DeleteLogBuf(log_file_name);
        for (int i = 0; i < frag->log_replica_stoc_ids.size(); i++) {
            uint32_t stoc_server_id = nova::NovaConfig::config->dc_servers[frag->log_replica_stoc_ids[i]].server_id;

            char *send_buf = store_->GetSendBuf(stoc_server_id);
            int size = 0;
            send_buf[size] = CCRequestType::CC_DELETE_LOG_FILE;
            size++;
            size += leveldb::EncodeFixed32(send_buf + size,
                                           log_file_name.size());
            for (auto &name : log_file_name) {
                size += leveldb::EncodeStr(send_buf + size, name);
            }
            store_->PostSend(send_buf, size, stoc_server_id, client_req_id);
        }
        return Status::OK();
    }
}