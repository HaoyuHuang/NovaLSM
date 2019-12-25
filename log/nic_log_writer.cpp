
//
// Created by Haoyu Huang on 12/23/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nic_log_writer.h"

namespace leveldb {
    namespace log {
        // Create a writer that will append data to "*dest".
// "*dest" must be initially empty.
// "*dest" must remain live while this Writer is in use.
        NICLogWriter::NICLogWriter(std::vector<nova::NovaClientSock *> &sockets,
                                   nova::NovaMemManager *mem_manager,
                                   nova::LogFileManager *log_manager)
                : sockets_(sockets), mem_manager_(mem_manager),
                  log_manager_(log_manager), Writer(nullptr) {
        }

        NICLogWriter::NICLogWriter(WritableFile *dest, uint64_t dest_length)
                : Writer(dest, dest_length) {}

        char *NICLogWriter::AllocateLogBuf(const std::string &log_file) {
            uint32_t slabclassid = mem_manager_->slabclassid(
                    nova::NovaConfig::config->log_buf_size);
            char *buf = mem_manager_->ItemAlloc(slabclassid);
            logfile_last_buf_[log_file] = {
                    .base = buf,
                    .size = nova::NovaConfig::config->log_buf_size,
                    .offset = 0
            };
            log_manager_->Add(log_file, buf);
            return buf;
        }

        char *NICLogWriter::AddLocalRecord(const std::string &log_file_name,
                                           const Slice &slice) {
            auto it = logfile_last_buf_.find(log_file_name);
            if (it == logfile_last_buf_.end()) {
                AllocateLogBuf(log_file_name);
                it = logfile_last_buf_.find(log_file_name);
            }
            RDMA_ASSERT(it != logfile_last_buf_.end());
            LogFileBuf &buf = it->second;

            if (buf.offset + 1 + 4 + log_file_name.size() + 4 + slice.size() +
                1 > buf.size) {
                AllocateLogBuf(log_file_name);
                buf = logfile_last_buf_[log_file_name];
            }
            char *base = buf.base + buf.offset;
            base[0] = nova::RequestType::REPLICATE_LOG_RECORD;
            base++;
            leveldb::EncodeFixed32(base, log_file_name.size());
            base += 4;
            memcpy(base, log_file_name.data(), log_file_name.size());
            base += log_file_name.size();
            leveldb::EncodeFixed32(base, slice.size());
            base += 4;
            memcpy(base, slice.data(), slice.size());
            base += slice.size();
            base[0] = MSG_TERMINATER_CHAR;
            buf.offset += slice.size();
            return base;
        }

        Status
        NICLogWriter::AddRecord(const std::string &log_file_name,
                                const Slice &slice) {
            char *buf = AddLocalRecord(log_file_name, slice);
            for (int remote_server_id = 0; remote_server_id <
                                           nova::NovaConfig::config->servers.size(); remote_server_id++) {
                if (remote_server_id ==
                    nova::NovaConfig::config->my_server_id) {
                    continue;
                }
                sockets_[remote_server_id]->Send(buf,
                                                 1 + 4 + log_file_name.size() +
                                                 4 + slice.size() + 1);
            }

            // Pull all pending writes.
            for (int remote_server_id = 0; remote_server_id <
                                           nova::NovaConfig::config->servers.size(); remote_server_id++) {
                if (remote_server_id ==
                    nova::NovaConfig::config->my_server_id) {
                    continue;
                }
                sockets_[remote_server_id]->Receive();
            }
            return Status::OK();
        }

        Status NICLogWriter::CloseLogFile(const std::string &log_file_name) {
            logfile_last_buf_.erase(log_file_name);
            log_manager_->DeleteLogBuf(log_file_name);
            for (int remote_server_id = 0; remote_server_id <
                                           nova::NovaConfig::config->servers.size(); remote_server_id++) {
                if (remote_server_id ==
                    nova::NovaConfig::config->my_server_id) {
                    continue;
                }

                char *send_buf = sockets_[remote_server_id]->send_buf();
                send_buf[0] = nova::RequestType::DELETE_LOG_FILE;
                send_buf++;
                leveldb::EncodeFixed32(send_buf, log_file_name.size());
                send_buf += 4;
                memcpy(send_buf, log_file_name.data(), log_file_name.size());
                send_buf += log_file_name.size();
                send_buf[0] = MSG_TERMINATER_CHAR;
                sockets_[remote_server_id]->Send(nullptr,
                                                 1 + 4 + log_file_name.size() +
                                                 1);
            }
            return Status::OK();
        }
    }
}