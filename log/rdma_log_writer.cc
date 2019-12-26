
//
// Created by Haoyu Huang on 12/23/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "rdma_log_writer.h"

namespace leveldb {
    namespace log {
        // Create a writer that will append data to "*dest".
// "*dest" must be initially empty.
// "*dest" must remain live while this Writer is in use.
        RDMALogWriter::RDMALogWriter(nova::NovaRDMAStore *store,
                                     nova::NovaMemManager *mem_manager,
                                     nova::LogFileManager *log_manager)
                : store_(store), mem_manager_(mem_manager),
                  log_manager_(log_manager), Writer(nullptr) {
            write_result_ = new WriteResult[nova::NovaConfig::config->servers.size()];
            for (int i = 0; i < nova::NovaConfig::config->servers.size(); i++) {
                write_result_[i] = WriteResult::NONE;
            }
        }

        RDMALogWriter::RDMALogWriter(WritableFile *dest, uint64_t dest_length)
                : Writer(dest, dest_length) {}


        void RDMALogWriter::Init(const std::string &log_file_name) {
            auto it = logfile_last_buf_.find(log_file_name);
            if (it == logfile_last_buf_.end()) {
                LogFileBuf *buf = new LogFileBuf[nova::NovaConfig::config->servers.size()];
                logfile_last_buf_.insert(std::make_pair(log_file_name, buf));
                for (int i = 0;
                     i < nova::NovaConfig::config->servers.size(); i++) {
                    buf[i] = {
                            .base = 0,
                            .offset = 0,
                            .size = 0
                    };
                }
            }
        }

        char *RDMALogWriter::AllocateLogBuf(const std::string &log_file) {
            uint32_t slabclassid = mem_manager_->slabclassid(
                    nova::NovaConfig::config->log_buf_size);
            int server_id = nova::NovaConfig::config->my_server_id;
            char *buf = mem_manager_->ItemAlloc(slabclassid);
            Init(log_file);
            logfile_last_buf_[log_file][server_id] = {
                    .base = (uint64_t) buf,
                    .offset = 0,
                    .size = nova::NovaConfig::config->log_buf_size
            };
            log_manager_->Add(log_file, buf);
            return buf;
        }

        char *RDMALogWriter::AddLocalRecord(const std::string &log_file_name,
                                            const Slice &slice) {
            int server_id = nova::NovaConfig::config->my_server_id;
            char *buf = nullptr;
            Init(log_file_name);
            auto &it = logfile_last_buf_[log_file_name];
            if (it[server_id].offset + slice.size() >=
                it[server_id].size) {
                AllocateLogBuf(log_file_name);
            }
            buf = (char *) it[server_id].base;
            memcpy(buf, slice.data(), slice.size());
            it[server_id].offset += slice.size();
            return buf;
        }

        void RDMALogWriter::AckAllocLogBuf(int remote_sid, uint64_t offset,
                                           uint64_t size) {
            write_result_[remote_sid] = WriteResult::ALLOC_SUCCESS;
            logfile_last_buf_[*current_log_file_][remote_sid].base = offset;
            logfile_last_buf_[*current_log_file_][remote_sid].size = size;
            logfile_last_buf_[*current_log_file_][remote_sid].offset = 0;
        }

        void RDMALogWriter::AckWriteSuccess(int remote_sid) {
            write_result_[remote_sid] = WriteResult::WRITE_SUCESS;
        }

        Status
        RDMALogWriter::AddRecord(const std::string &log_file_name,
                                 const Slice &slice) {
            uint64_t db_index;
            nova::ParseDBName(log_file_name, &db_index);
            nova::Fragment *frag = nova::NovaConfig::config->db_fragment[db_index];

            *current_log_file_ = log_file_name;
            int nreplicas = 0;
            char *buf = AddLocalRecord(log_file_name, slice);
            for (int i = 0; i < frag->server_ids.size(); i++) {
                uint32_t remote_server_id = frag->server_ids[i];
                if (remote_server_id ==
                    nova::NovaConfig::config->my_server_id) {
                    continue;
                }
                nreplicas++;

                auto &it = logfile_last_buf_[log_file_name];
                if (it[remote_server_id].base == 0 ||
                    (it[remote_server_id].offset + slice.size() >
                     it[remote_server_id].size)) {
                    // Allocate a new buf.
                    char *send_buf = store_->GetSendBuf(remote_server_id);
                    send_buf[0] = nova::RequestType::ALLOCATE_LOG_BUFFER;
                    send_buf++;
                    leveldb::EncodeFixed32(send_buf, log_file_name.size());
                    send_buf += 4;
                    memcpy(send_buf, log_file_name.data(),
                           log_file_name.size());
                    store_->PostSend(nullptr, 1 + 4 + log_file_name.size(),
                                     remote_server_id);
                    write_result_[remote_server_id] = WriteResult::WAIT_FOR_ALLOC;
                } else {
                    // WRITE.
                    store_->PostWrite(buf, slice.size(), remote_server_id,
                                      it[remote_server_id].base +
                                      it[remote_server_id].offset,
                                      false);
                    it[remote_server_id].offset += slice.size();
                    write_result_[remote_server_id] = WriteResult::WAIT_FOR_WRITE;
                }
            }

            store_->FlushPendingSends();

            // Pull all pending writes.
            while (true) {
                int acks = 0;
                LogFileBuf *it = nullptr;
                for (int i = 0; i < frag->server_ids.size(); i++) {
                    uint32_t remote_server_id = frag->server_ids[i];
                    switch (write_result_[remote_server_id]) {
                        case WriteResult::NONE:
                            break;
                        case WriteResult::WAIT_FOR_ALLOC:
                            store_->PollRQ(remote_server_id);
                            break;
                        case WriteResult::WAIT_FOR_WRITE:
                            store_->PollSQ(remote_server_id);
                            break;
                        case WriteResult::ALLOC_SUCCESS:
                            it = logfile_last_buf_[*current_log_file_];
                            store_->PostWrite(buf, slice.size(),
                                              remote_server_id,
                                              it[remote_server_id].base +
                                              it[remote_server_id].offset, /*is_remote_offset=*/
                                              false);
                            it[remote_server_id].offset += slice.size();
                            write_result_[remote_server_id] = WriteResult::WAIT_FOR_WRITE;
                            break;
                        case WriteResult::WRITE_SUCESS:
                            acks++;
                            break;
                    }
                }
                if (acks == nreplicas) {
                    break;
                }
            }
            return Status::OK();
        }

        Status RDMALogWriter::CloseLogFile(const std::string &log_file_name) {
            uint64_t db_index;
            nova::ParseDBName(log_file_name, &db_index);
            nova::Fragment *frag = nova::NovaConfig::config->db_fragment[db_index];

            delete logfile_last_buf_[log_file_name];
            logfile_last_buf_.erase(log_file_name);
            log_manager_->DeleteLogBuf(log_file_name);
            for (int i = 0; i < frag->server_ids.size(); i++) {
                uint32_t remote_server_id = frag->server_ids[i];
                if (remote_server_id ==
                    nova::NovaConfig::config->my_server_id) {
                    continue;
                }
                char *send_buf = store_->GetSendBuf(remote_server_id);
                send_buf[0] = nova::RequestType::DELETE_LOG_FILE;
                send_buf++;
                leveldb::EncodeFixed32(send_buf, log_file_name.size());
                send_buf += 4;
                memcpy(send_buf, log_file_name.data(),
                       log_file_name.size());
                store_->PostSend(nullptr, 1 + 4 + log_file_name.size(),
                                 remote_server_id);
            }
            return Status::OK();
        }
    }
}