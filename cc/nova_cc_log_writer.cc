
//
// Created by Haoyu Huang on 12/23/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova/nova_config.h"
#include "nova_cc_log_writer.h"

namespace leveldb {
    namespace log {
        // Create a writer that will append data to "*dest".
// "*dest" must be initially empty.
// "*dest" must remain live while this Writer is in use.
        RDMALogWriter::RDMALogWriter(nova::NovaRDMAStore *store, char *rnic_buf)
                : store_(store), rnic_buf_(rnic_buf) {
            write_result_ = new WriteState[nova::NovaConfig::config->servers.size()];
            for (int i = 0;
                 i < nova::NovaConfig::config->servers.size(); i++) {
                write_result_[i].result = WriteResult::NONE;
                write_result_[i].rdma_wr_id = 0;
            }
        }

        void RDMALogWriter::Init(const std::string &log_file_name) {
            auto it = logfile_last_buf_.find(log_file_name);
            if (it == logfile_last_buf_.end()) {
                LogFileBuf *buf = new LogFileBuf[nova::NovaConfig::config->servers.size()];
                logfile_last_buf_.insert(std::make_pair(log_file_name, buf));
                for (int i = 0;
                     i <
                     nova::NovaConfig::config->servers.size(); i++) {
                    buf[i] = {
                            .base = 0,
                            .offset = 0,
                            .size = 0
                    };
                }
            }
        }

        void RDMALogWriter::AckAllocLogBuf(int remote_sid, uint64_t offset,
                                           uint64_t size) {
            write_result_[remote_sid].result = WriteResult::ALLOC_SUCCESS;
            logfile_last_buf_[current_log_file_][remote_sid].base = offset;
            logfile_last_buf_[current_log_file_][remote_sid].size = size;
            logfile_last_buf_[current_log_file_][remote_sid].offset = 0;
        }

        void RDMALogWriter::AckWriteSuccess(int remote_sid, uint64_t wr_id) {
            if (write_result_[remote_sid].rdma_wr_id == wr_id) {
                write_result_[remote_sid].result = WriteResult::WRITE_SUCESS;
            }
        }

        std::string RDMALogWriter::write_result_str(
                leveldb::log::RDMALogWriter::WriteResult wr) {
            switch (wr) {
                case NONE:
                    return "none";
                case WAIT_FOR_ALLOC:
                    return "wait_for_alloc";
                case ALLOC_SUCCESS:
                    return "alloc_success";
                case WAIT_FOR_WRITE:
                    return "wait_for_write";
                case WRITE_SUCESS:
                    return "write_success";
            }
        }

        Status
        RDMALogWriter::AddRecord(const std::string &log_file_name,
                                 const Slice &slice) {
            int nreplicas = 0;
            uint32_t sid;
            uint32_t db_index;
            nova::ParseDBName(log_file_name, &sid, &db_index);
            nova::CCFragment *frag = nova::NovaCCConfig::cc_config->db_fragment[db_index];

            Init(log_file_name);
            current_log_file_ = log_file_name;
            memcpy(rnic_buf_, slice.data(), slice.size());
            for (int i = 0; i < frag->dc_server_ids.size(); i++) {
                uint32_t remote_server_id = frag->dc_server_ids[i];
                nreplicas++;

                auto &it = logfile_last_buf_[log_file_name];
                if (it[remote_server_id].base == 0 ||
                    (it[remote_server_id].offset + slice.size() >
                     it[remote_server_id].size)) {
                    // Allocate a new buf.
                    char *send_buf = store_->GetSendBuf(remote_server_id);
                    char *buf = send_buf;
                    buf[0] = nova::RequestType::ALLOCATE_LOG_BUFFER;
                    buf++;
                    leveldb::EncodeFixed32(buf, log_file_name.size());
                    buf += 4;
                    memcpy(buf, log_file_name.data(), log_file_name.size());
                    write_result_[remote_server_id].result = WriteResult::WAIT_FOR_ALLOC;
                    store_->PostSend(
                            send_buf, 1 + 4 + log_file_name.size(),
                            remote_server_id, 0);
                } else {
                    // WRITE.
                    write_result_[remote_server_id].rdma_wr_id = store_->PostWrite(
                            rnic_buf_, slice.size(), remote_server_id,
                            it[remote_server_id].base +
                            it[remote_server_id].offset,
                            false, 0);
                    it[remote_server_id].offset += slice.size();
                    write_result_[remote_server_id].result = WriteResult::WAIT_FOR_WRITE;
                }
            }

            store_->FlushPendingSends();

            // Pull all pending writes.
            int n = 0;
            while (true) {
                int acks = 0;
                LogFileBuf *it = nullptr;
                bool post_write = false;

                // We need to poll both queues here since a live lock may occur when a remote thread S issue requests to myself while I have a pending request to S.
                store_->PollSQ();
                store_->PollRQ();

                n++;
                for (int i = 0; i < frag->dc_server_ids.size(); i++) {
                    uint32_t remote_server_id = frag->dc_server_ids[i];

                    switch (write_result_[remote_server_id].result) {
                        case WriteResult::NONE:
                            break;
                        case WriteResult::WAIT_FOR_ALLOC:
                            break;
                        case WriteResult::WAIT_FOR_WRITE:
                            break;
                        case WriteResult::ALLOC_SUCCESS:
                            it = logfile_last_buf_[current_log_file_];
                            write_result_[remote_server_id].rdma_wr_id = store_->PostWrite(
                                    rnic_buf_, slice.size(),
                                    remote_server_id,
                                    it[remote_server_id].base +
                                    it[remote_server_id].offset, /*is_remote_offset=*/
                                    false, 0);
                            it[remote_server_id].offset += slice.size();
                            write_result_[remote_server_id].result = WriteResult::WAIT_FOR_WRITE;
                            post_write = true;
                            break;
                        case WriteResult::WRITE_SUCESS:
                            acks++;
                            break;
                    }
                }

                if (post_write) {
                    store_->FlushPendingSends();
                }
                if (acks == nreplicas) {
                    break;
                }
            }
            return Status::OK();
        }

        Status RDMALogWriter::CloseLogFile(const std::string &log_file_name) {
            uint32_t sid;
            uint32_t db_index;
            nova::ParseDBName(log_file_name, &sid, &db_index);
            nova::CCFragment *frag = nova::NovaCCConfig::cc_config->db_fragment[db_index];

            delete logfile_last_buf_[log_file_name];
            logfile_last_buf_.erase(log_file_name);
            for (int i = 0; i < frag->dc_server_ids.size(); i++) {
                uint32_t remote_server_id = frag->dc_server_ids[i];
                char *send_buf = store_->GetSendBuf(remote_server_id);
                char *buf = send_buf;
                buf[0] = nova::RequestType::DELETE_LOG_FILE;
                buf++;
                leveldb::EncodeStr(buf, log_file_name);
                store_->PostSend(send_buf, 1 + 4 + log_file_name.size(),
                                 remote_server_id, 0);
            }
            return Status::OK();
        }
    }
}