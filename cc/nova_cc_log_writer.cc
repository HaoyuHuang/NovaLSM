
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
        RDMALogWriter::RDMALogWriter(nova::NovaRDMAStore *store, char *rnic_buf,
                                     MemManager *mem_manager,
                                     nova::InMemoryLogFileManager *log_manager)
                : store_(store), rnic_buf_(rnic_buf), mem_manager_(mem_manager),
                  log_manager_(log_manager) {
            write_result_ = new WriteState[nova::NovaConfig::config->servers.size()];
            for (int i = 0;
                 i < nova::NovaConfig::config->servers.size(); i++) {
                write_result_[i].result = WriteResult::NONE;
                write_result_[i].rdma_wr_id = 0;
            }
        }

        char *RDMALogWriter::Init(MemTableIdentifier memtable_id,
                                  uint64_t thread_id,
                                  const std::vector<LevelDBLogRecord> &log_records,
                                  uint32_t size) {
            auto it = logfile_last_buf_.find(memtable_id);
            if (it == logfile_last_buf_.end()) {
                LogFileBuf *buf = new LogFileBuf[nova::NovaConfig::config->servers.size()];
                logfile_last_buf_.insert(std::make_pair(memtable_id, buf));
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

            // Add to local memory.
            int myid = nova::NovaConfig::config->my_server_id;
            LogFileBuf &buf = logfile_last_buf_[memtable_id][myid];
            char *b = nullptr;
            if (buf.base == 0) {
                uint32_t scid = mem_manager_->slabclassid(thread_id,
                                                          nova::NovaConfig::config->log_buf_size);
                b = mem_manager_->ItemAlloc(thread_id, scid);
                RDMA_ASSERT(b);
                log_manager_->Add(thread_id, memtable_id, b);
                buf.base = (uint64_t) (b);
                buf.offset = 0;
                buf.size = nova::NovaConfig::config->log_buf_size;
            } else {
                b = (char *) buf.base + buf.offset;
            }
            RDMA_ASSERT(buf.offset + size < buf.size)
                << fmt::format("{}:{}:{}", buf.offset, buf.size, size);
            uint32_t encoded_size = nova::EncodeLogRecords(b, log_records);
            RDMA_ASSERT(encoded_size == size);
            buf.offset += size;
            return b;
        }

        void RDMALogWriter::AckAllocLogBuf(int remote_sid, uint64_t offset,
                                           uint64_t size) {
            write_result_[remote_sid].result = WriteResult::ALLOC_SUCCESS;
            logfile_last_buf_[current_memtable_id_][remote_sid].base = offset;
            logfile_last_buf_[current_memtable_id_][remote_sid].size = size;
            logfile_last_buf_[current_memtable_id_][remote_sid].offset = 0;
            // Remember the offset and size in log manager.
            log_manager_->AddReplica(current_memtable_id_, remote_sid, offset,
                                     size);
        }

        bool RDMALogWriter::AckWriteSuccess(int remote_sid, uint64_t wr_id) {
            WriteState &state = write_result_[remote_sid];
            if (state.rdma_wr_id == wr_id &&
                state.result == WriteResult::WAIT_FOR_WRITE) {
                state.result = WriteResult::WRITE_SUCESS;
                return true;
            }
            return false;
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
        RDMALogWriter::AddRecord(MemTableIdentifier memtable_id,
                                 uint64_t thread_id,
                                 const std::vector<LevelDBLogRecord> &log_records) {
            current_memtable_id_ = memtable_id;
            int nreplicas = 0;
            nova::CCFragment *frag = nova::NovaCCConfig::cc_config->db_fragment[memtable_id.db_id];
            uint32_t log_record_size = nova::LogRecordsSize(log_records);
            char *rnic_buf = Init(memtable_id, thread_id, log_records,
                                  log_record_size);
            if (frag->cc_server_ids.size() == 1) {
                return Status::OK();
            }

            for (int i = 0; i < frag->cc_server_ids.size(); i++) {
                uint32_t remote_server_id = frag->cc_server_ids[i];
                if (remote_server_id ==
                    nova::NovaConfig::config->my_server_id) {
                    continue;
                }

                nreplicas++;

                auto &it = logfile_last_buf_[memtable_id];
                if (it[remote_server_id].base == 0 ||
                    (it[remote_server_id].offset + log_record_size >
                     it[remote_server_id].size)) {
                    // Allocate a new buf.
                    char *send_buf = store_->GetSendBuf(remote_server_id);
                    char *buf = send_buf;
                    uint32_t msg_size = 1;
                    buf[0] = CCRequestType::CC_ALLOCATE_LOG_BUFFER;
                    msg_size += nova::EncodeMemTableId(buf + msg_size,
                                                       memtable_id);
                    write_result_[remote_server_id].result = WriteResult::WAIT_FOR_ALLOC;
                    store_->PostSend(send_buf, msg_size, remote_server_id, 0);
                } else {
                    // WRITE.
                    write_result_[remote_server_id].rdma_wr_id = store_->PostWrite(
                            rnic_buf, log_record_size, remote_server_id,
                            it[remote_server_id].base +
                            it[remote_server_id].offset,
                            false, 0);
                    it[remote_server_id].offset += log_record_size;
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
                for (int i = 0; i < frag->cc_server_ids.size(); i++) {
                    uint32_t remote_server_id = frag->cc_server_ids[i];
                    if (remote_server_id ==
                        nova::NovaConfig::config->my_server_id) {
                        continue;
                    }

                    switch (write_result_[remote_server_id].result) {
                        case WriteResult::NONE:
                            break;
                        case WriteResult::WAIT_FOR_ALLOC:
                            break;
                        case WriteResult::WAIT_FOR_WRITE:
                            break;
                        case WriteResult::ALLOC_SUCCESS:
                            it = logfile_last_buf_[memtable_id];
                            write_result_[remote_server_id].rdma_wr_id = store_->PostWrite(
                                    rnic_buf, log_record_size,
                                    remote_server_id,
                                    it[remote_server_id].base +
                                    it[remote_server_id].offset, /*is_remote_offset=*/
                                    false, 0);
                            it[remote_server_id].offset += log_record_size;
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

        Status RDMALogWriter::CloseLogFile(MemTableIdentifier memtable_id) {
            nova::CCFragment *frag = nova::NovaCCConfig::cc_config->db_fragment[memtable_id.db_id];

            delete logfile_last_buf_[memtable_id];
            logfile_last_buf_.erase(memtable_id);
            log_manager_->DeleteLogBuf(memtable_id);
            for (int i = 0; i < frag->cc_server_ids.size(); i++) {
                uint32_t remote_server_id = frag->cc_server_ids[i];
                if (remote_server_id ==
                    nova::NovaConfig::config->my_server_id) {
                    continue;
                }

                char *send_buf = store_->GetSendBuf(remote_server_id);
                char *buf = send_buf;
                uint32_t msg_size = 1;
                buf[0] = CCRequestType::CC_DELETE_LOG_FILE;
                msg_size += nova::EncodeMemTableId(buf + msg_size, memtable_id);
                store_->PostSend(send_buf, msg_size, remote_server_id, 0);
            }
            log_manager_->RemoveMemTable(memtable_id);
            return Status::OK();
        }
    }
}