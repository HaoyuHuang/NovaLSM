
//
// Created by Haoyu Huang on 12/23/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nic_log_writer.h"
#include "nova/nova_common.h"

namespace leveldb {
    namespace log {
        namespace {
            uint32_t LogRecordSize(const std::string &log_file_name,
                                   const Slice &slice) {
                return 1 + 4 + log_file_name.size() + 4 + slice.size() + 1;
            }

            void PrepareLogRecord(char *start, const std::string &log_file_name,
                                  const Slice &slice) {
                char *base = start;
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
            }
        }

        // Create a writer that will append data to "*dest".
// "*dest" must be initially empty.
// "*dest" must remain live while this Writer is in use.
        NICLogWriter::NICLogWriter(std::vector<nova::NovaClientSock *> *sockets,
                                   nova::LogFileManager *log_manager)
                : sockets_(sockets), log_manager_(log_manager),
                  Writer(nullptr) {
        }

        NICLogWriter::NICLogWriter(WritableFile *dest, uint64_t dest_length)
                : Writer(dest, dest_length) {}

        char *NICLogWriter::AddLocalRecord(const std::string &log_file_name,
                                           const Slice &slice) {
            log_manager_->AddLogRecord(log_file_name, slice);
            return nullptr;
        }

        Status
        NICLogWriter::AddRecord(const std::string &log_file_name,
                                const Slice &slice) {
            uint32_t db_sid;
            uint32_t db_index;
            nova::ParseDBName(log_file_name, &db_sid, &db_index);
            nova::Fragment *frag = nova::NovaConfig::config->db_fragment[db_index];

            RDMA_LOG(rdmaio::DEBUG) << "NIC Replicate Log record of "
                                    << log_file_name << " size " << slice.size()
                                    << " to "
                                    << nova::ToString(frag->server_ids);

            AddLocalRecord(log_file_name, slice);
            uint32_t log_record_size = LogRecordSize(log_file_name, slice);
            for (int i = 0; i < frag->server_ids.size(); i++) {
                uint32_t remote_server_id = frag->server_ids[i];
                if (remote_server_id ==
                    nova::NovaConfig::config->my_server_id) {
                    continue;
                }
                nova::NovaClientSock *sock = (*sockets_)[remote_server_id];
                PrepareLogRecord(sock->send_buf(), log_file_name, slice);
                sock->Send(nullptr, log_record_size);
            }

            // Pull all pending writes.
            for (int i = 0; i < frag->server_ids.size(); i++) {
                uint32_t remote_server_id = frag->server_ids[i];
                if (remote_server_id ==
                    nova::NovaConfig::config->my_server_id) {
                    continue;
                }
                RDMA_LOG(rdmaio::DEBUG)
                    << "NIC log record wait for response from "
                    << remote_server_id;
                nova::NovaClientSock *sock = (*sockets_)[remote_server_id];
                RDMA_ASSERT(sock->Receive() == 1);
                RDMA_ASSERT(sock->recv_buf()[4] ==
                            nova::RequestType::REPLICATE_LOG_RECORD_SUCC);
            }
            RDMA_LOG(rdmaio::DEBUG) << "NIC log record replicated.";
            return Status::OK();
        }

        Status NICLogWriter::CloseLogFile(const std::string &log_file_name) {
            uint32_t db_sid;
            uint32_t db_index;
            nova::ParseDBName(log_file_name, &db_sid, &db_index);
            nova::Fragment *frag = nova::NovaConfig::config->db_fragment[db_index];
            log_manager_->DeleteLogBuf(log_file_name);

            RDMA_LOG(rdmaio::INFO) << "NIC close log file "
                                   << log_file_name << " to "
                                   << nova::ToString(frag->server_ids);

            for (int i = 0; i < frag->server_ids.size(); i++) {
                uint32_t remote_server_id = frag->server_ids[i];
                if (remote_server_id ==
                    nova::NovaConfig::config->my_server_id) {
                    continue;
                }

                char *send_buf = (*sockets_)[remote_server_id]->send_buf();
                send_buf[0] = nova::RequestType::DELETE_LOG_FILE;
                send_buf++;
                leveldb::EncodeFixed32(send_buf, log_file_name.size());
                send_buf += 4;
                memcpy(send_buf, log_file_name.data(), log_file_name.size());
                send_buf += log_file_name.size();
                send_buf[0] = MSG_TERMINATER_CHAR;
                (*sockets_)[remote_server_id]->Send(nullptr,
                                                    1 + 4 +
                                                    log_file_name.size() +
                                                    1);
            }

            // Pull all pending writes.
            for (int i = 0; i < frag->server_ids.size(); i++) {
                uint32_t remote_server_id = frag->server_ids[i];
                if (remote_server_id ==
                    nova::NovaConfig::config->my_server_id) {
                    continue;
                }
                RDMA_LOG(rdmaio::DEBUG)
                    << "NIC log record wait for response from "
                    << remote_server_id;
                nova::NovaClientSock *sock = (*sockets_)[remote_server_id];
                RDMA_ASSERT(sock->Receive() == 1);
                RDMA_ASSERT(sock->recv_buf()[4] ==
                            nova::RequestType::DELETE_LOG_FILE_SUCC);
            }
            return Status::OK();
        }
    }
}