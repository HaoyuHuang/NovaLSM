
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
                                     nova::NovaMemManager *mem_manager)
                : store_(store), mem_manager_(mem_manager), Writer(nullptr) {
            server_remote_cur_offset_ = new uint64_t[nova::NovaConfig::config->servers.size()];
            server_remote_base_offset_ = new uint64_t[nova::NovaConfig::config->servers.size()];
            server_remote_buf_size_ = new uint64_t[nova::NovaConfig::config->servers.size()];
            write_result_ = new WriteResult[nova::NovaConfig::config->servers.size()];

            for (int i = 0; i < nova::NovaConfig::config->servers.size(); i++) {
                server_remote_buf_size_[i] = 0;
                server_remote_cur_offset_[i] = 0;
                server_remote_base_offset_[i] = 0;
                write_result_[i] = WriteResult::NONE;
            }
        }

        RDMALogWriter::RDMALogWriter(WritableFile *dest, uint64_t dest_length)
                : Writer(dest, dest_length) {}

        char *RDMALogWriter::AllocateLogBuf(const std::string &log_file) {
            uint32_t slabclassid = mem_manager_->slabclassid(
                    nova::NovaConfig::config->log_buf_size);
            char *buf = mem_manager_->ItemAlloc(slabclassid);
            auto it = logfile_bufs.find(log_file);
            if (it != logfile_bufs.end()) {
                it->second.emplace_back(
                        Slice(buf, nova::NovaConfig::config->log_buf_size));
            } else {
                std::vector<Slice> bufs;
                bufs.emplace_back(
                        Slice(buf, nova::NovaConfig::config->log_buf_size));
                logfile_bufs.insert(std::make_pair(log_file, bufs));
            }
            return buf;
        }

        void RDMALogWriter::DeleteLogBuf(const std::string &log_file) {
            uint32_t slabclassid = mem_manager_->slabclassid(
                    nova::NovaConfig::config->log_buf_size);
            auto it = logfile_bufs.find(log_file);
            if (it == logfile_bufs.end()) {
                return;
            }
            mem_manager_->FreeItems(it->second, slabclassid);
        }

        char *RDMALogWriter::AddLocalRecord(const std::string &log_file_name,
                                            const Slice &slice) {
            int server_id = nova::NovaConfig::config->my_server_id;
            char *buf = nullptr;
            if (server_remote_cur_offset_[server_id] + slice.size() >=
                server_remote_buf_size_[server_id]) {
                buf = AllocateLogBuf(log_file_name);
                server_remote_buf_size_[server_id] = nova::NovaConfig::config->log_buf_size;
            } else {
                auto it = logfile_bufs.find(log_file_name);
                RDMA_ASSERT(it != logfile_bufs.end());
                buf = (char *) it->second[it->second.size() - 1].data();
            }
            memcpy(buf, slice.data(), slice.size());
            server_remote_cur_offset_[server_id] += slice.size();
            return buf;
        }

        void RDMALogWriter::AckAllocLogBuf(int remote_sid, uint64_t offset,
                                           uint64_t size) {
            write_result_[remote_sid] = WriteResult::ALLOC_SUCCESS;
            server_remote_base_offset_[remote_sid] = offset;
            server_remote_buf_size_[remote_sid] = size;
            server_remote_cur_offset_[remote_sid] = 0;
        }

        void RDMALogWriter::AckWriteSuccess(int remote_sid) {
            write_result_[remote_sid] = WriteResult::WRITE_SUCESS;
        }

        Status
        RDMALogWriter::AddRecord(const std::string &log_file_name,
                                 const Slice &slice) {
            int nreplicas = 0;
            char *buf = AddLocalRecord(log_file_name, slice);
            for (int remote_server_id = 0; remote_server_id <
                                           nova::NovaConfig::config->servers.size(); remote_server_id++) {
                if (remote_server_id ==
                    nova::NovaConfig::config->my_server_id) {
                    continue;
                }
                nreplicas++;
                if (server_remote_cur_offset_[remote_server_id] + slice.size() <
                    server_remote_buf_size_[remote_server_id]) {
                    // WRITE.
                    store_->PostWrite(buf, slice.size(), remote_server_id,
                                      server_remote_cur_offset_[remote_server_id] +
                                      server_remote_base_offset_[remote_server_id],
                                      false);
                    server_remote_cur_offset_[remote_server_id] += slice.size();
                    write_result_[remote_server_id] = WriteResult::WAIT_FOR_WRITE;
                } else {
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
                }
            }

            // Pull all pending writes.
            while (true) {
                int acks = 0;
                for (int remote_server_id = 0; remote_server_id <
                                               nova::NovaConfig::config->servers.size(); remote_server_id++) {
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
                            store_->PostWrite(buf, slice.size(),
                                              remote_server_id,
                                              server_remote_cur_offset_[remote_server_id], /*is_remote_offset=*/
                                              false);
                            server_remote_cur_offset_[remote_server_id] += slice.size();
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
            DeleteLogBuf(log_file_name);
            for (int remote_server_id = 0; remote_server_id <
                                           nova::NovaConfig::config->servers.size(); remote_server_id++) {
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