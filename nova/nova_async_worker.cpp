
//
// Created by Haoyu Huang on 12/25/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <leveldb/write_batch.h>
#include "nova_async_worker.h"
#include "nova_mem_config.h"
#include "nova_common.h"

namespace nova {
    void NovaAsyncWorker::AddTask(const NovaAsyncTask &task) {
        mutex_.Lock();
        queue_.push_back(task);
        mutex_.Unlock();
        if (NovaConfig::config->log_record_mode !=
            NovaLogRecordMode::LOG_RDMA) {
            sem_post(&sem_);
        }
    }

    int NovaAsyncWorker::size() {
        mutex_.Lock();
        int size = queue_.size();
        mutex_.Unlock();
        return size;
    }

    int NovaAsyncWorker::ProcessQueue() {
        mutex_.Lock();
        if (queue_.empty()) {
            mutex_.Unlock();
            return 0;
        }
        std::list<NovaAsyncTask> queue(queue_.begin(), queue_.end());
        mutex_.Unlock();

        for (const NovaAsyncTask &task : queue) {
            uint64_t hv = NovaConfig::keyhash(task.key.data(),
                                              task.key.size());
            leveldb::WriteOptions option;
            option.sync = NovaConfig::config->fsync;
            switch (NovaConfig::config->log_record_mode) {
                case LOG_LOCAL:
                    option.local_write = true;
                    break;
                case LOG_NIC:
                    option.local_write = false;
                    option.writer = nic_log_writer_;
                    break;
                case LOG_RDMA:
                    option.local_write = false;
                    option.writer = rdma_log_writer_;
                    break;
            }

            Fragment *frag = NovaConfig::home_fragment(hv);
            leveldb::DB *db = dbs_[frag->db_ids[0]];
            if (!option.local_write) {
                leveldb::WriteBatch batch;
                batch.Put(task.key, task.value);
                db->GenerateLogRecords(option, &batch);
            }
            leveldb::Status status = db->Put(option, task.key, task.value);
            RDMA_LOG(DEBUG) << "############### Async worker processed task "
                            << task.sock_fd
                            << ":" << task.key;
            RDMA_ASSERT(status.ok()) << status.ToString();
        }

        mutex_.Lock();
        auto begin = queue_.begin();
        auto end = queue_.begin();
        std::advance(end, queue.size());
        queue_.erase(begin, end);
        mutex_.Unlock();

        cq_->mutex.Lock();
        for (const NovaAsyncTask &task : queue) {
            NovaAsyncCompleteTask t;
            t.sock_fd = task.sock_fd;
            t.conn = task.conn;
            cq_->queue.push_back(t);
        }
        cq_->mutex.Unlock();

        char buf[1];
        buf[0] = 'a';
        RDMA_ASSERT(write(cq_->write_fd, buf, 1) == 1);
        return queue.size();
    }

    void NovaAsyncWorker::Start() {
        RDMA_LOG(INFO) << "Async worker started";

        if (NovaConfig::config->enable_rdma) {
            rdma_store_->Init();
        }

        bool should_sleep = true;
        uint32_t timeout = RDMA_POLL_MIN_TIMEOUT_US;
        while (is_running_) {
            if (NovaConfig::config->log_record_mode ==
                NovaLogRecordMode::LOG_RDMA) {
                if (should_sleep) {
                    usleep(timeout);
                }

                rdma_store_->PollSQ();
                rdma_store_->PollRQ();

                int n = ProcessQueue();
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
            } else {
                sem_wait(&sem_);
                ProcessQueue();
            }
        }
    }

    //    void
//    NovaAsyncWorker::ProcessRDMAREAD(int remote_server_id,
//                                     char *buf) {
//
//        uint64_t to_server_id = 0;
//        uint64_t to_sock_fd = 0;
//        char *key = nullptr;
//        uint64_t nkey = 0;
//        RequestType type;
//        uint32_t req_len = ParseRDMARequest(buf, &type, &to_server_id,
//                                            &to_sock_fd,
//                                            &key, &nkey);
//        RDMA_ASSERT(to_sock_fd < NOVA_MAX_CONN) << to_sock_fd;
//        uint64_t hash = NovaConfig::keyhash(key, nkey);
//        Connection *conn = nova_conns[to_sock_fd];
//        NovaMemWorker *worker = (NovaMemWorker *) conn->worker;
//
//        char *databuf = buf + req_len;
//        bool invalid = false;
//
//        RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: "
//                        << "SQ: READ completes sid:" << to_server_id << " fd:"
//                        << to_sock_fd << " key:" << key << " nkey:" << nkey
//                        << " hash:" << hash
//                        << " type:" << type << " buf:" << buf;
//
//        if (type == RequestType::GET) {
//            DataEntry entry = DataEntry::chars_to_dataitem(databuf);
//            uint64_t fetched_key = 0;
//            str_to_int(entry.user_key(), &fetched_key, entry.nkey);
//            if (entry.stale) {
//                invalid = true;
//                stats.nget_rdma_stale++;
//            } else if (entry.nkey + entry.nval >
//                       NovaConfig::config->max_msg_size ||
//                       entry.nkey != nkey ||
//                       memcmp(entry.user_key(), key, nkey) != 0) {
//                invalid = true;
//            } else {
//                // Verify checksum.
//                uint64_t checksum = entry.compute_checksum(databuf);
//                if (checksum != entry.checksum) {
//                    invalid = true;
//                }
//            }
//            if (invalid) {
//                if (conn->number_get_retries >
//                    NovaConfig::config->rdma_number_of_get_retries) {
//                    // Exceeds max number of retries.
//                    conn->number_get_retries = 0;
//                    ProcessRDMAGETResponse(to_sock_fd,
//                                           nullptr, /*fetch_from_origin=*/true);
//                    return;
//                }
//                stats.nget_rdma_invalid++;
//                conn->number_get_retries++;
//            }
//            if (invalid) {
//                PostRDMAGETIndexRequest(to_sock_fd, key, nkey,
//                                        to_server_id, /*remote_offset=*/0);
//            } else {
//                ProcessRDMAGETResponse(to_sock_fd,
//                                       &entry, /*fetch_from_origin=*/
//                                       false);
//            }
//            return;
//        }
//
//        if (type == RequestType::GET_INDEX) {
//            // get index completes.
//            char *bucket = databuf;
//            IndexEntry index_entry{};
//            for (uint32_t i = 0;
//                 i < NovaConfig::config->nindex_entry_per_bucket; i++) {
//                char *index_entry_buf = bucket + i * IndexEntry::size();
//                index_entry = IndexEntry::chars_to_indexitem(index_entry_buf);
//                if (index_entry.type == IndexEntryType::EMPTY) {
//                    // empty.
//                    continue;
//                }
//                if (index_entry.type == IndexEntryType::INDRECT_HEADER) {
//                    break;
//                }
//                uint64_t computed_checksum = index_entry.compute_checksum(
//                        index_entry_buf);
//                RDMA_ASSERT(index_entry.type == IndexEntryType::DATA);
//                if (index_entry.checksum != computed_checksum) {
//                    stats.ngetindex_rdma_invalid++;
//                    invalid = true;
//                    break;
//                }
//                if (index_entry.hash == hash) {
//                    // Store it in the location cache.
//                    worker->stats.nput_lc++;
//                    mem_manager_->RemotePut(index_entry);
//                    // hash matches, fetch the data.
//                    PostRDMAGETRequest(to_sock_fd, key, nkey, to_server_id,
//                                       index_entry.data_ptr,
//                                       index_entry.data_size);
//                    return;
//                }
//            }
//            if (invalid) {
//                if (conn->number_get_retries >
//                    NovaConfig::config->rdma_number_of_get_retries) {
//                    // Exceeds max number of retries.
//                    conn->number_get_retries = 0;
//                    ProcessRDMAGETResponse(to_sock_fd,
//                                           nullptr, /*fetch_from_origin=*/true);
//                    return;
//                }
//                stats.nget_rdma_invalid++;
//                conn->number_get_retries++;
//            }
//            if (invalid) {
//                // Fetch the index bucket again.
//                PostRDMAGETIndexRequest(to_sock_fd, key, nkey,
//                                        to_server_id, /*remote_offset=*/0);
//            } else {
//                // Valid index.
//                if (index_entry.type == IndexEntryType::INDRECT_HEADER) {
//                    stats.ngetindex_rdma_indirect++;
//                    PostRDMAGETIndexRequest(to_sock_fd, key, nkey, to_server_id,
//                                            index_entry.data_ptr);
//                } else {
//                    // the key does not exist.
//                    ProcessRDMAGETResponse(to_sock_fd,
//                                           nullptr, /*fetch_from_origin=*/
//                                           false);
//                }
//            }
//        }
//    }

    void
    NovaAsyncWorker::ProcessRDMAWC(ibv_wc_opcode opcode,
                                   int remote_server_id,
                                   char *buf) {
        if (opcode == IBV_WC_RDMA_READ) {
//            ProcessRDMAREAD(remote_server_id, buf);
        } else if (opcode == IBV_WC_RDMA_WRITE) {
            rdma_log_writer_->AckWriteSuccess(remote_server_id);
        } else if (opcode == IBV_WC_RECV) {
            if (buf[0] == RequestType::ALLOCATE_LOG_BUFFER) {
                uint32_t size = leveldb::DecodeFixed32(buf + 1);
                std::string log_file(buf + 5, size);
                char *buf = rdma_log_writer_->AllocateLogBuf(log_file);
                char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
                send_buf[0] = RequestType::ALLOCATE_LOG_BUFFER_SUCC;
                leveldb::EncodeFixed64(send_buf + 1, (uint64_t) buf);
                leveldb::EncodeFixed64(send_buf + 9,
                                       NovaConfig::config->log_buf_size);
                rdma_store_->PostSend(nullptr, 1 + 8 + 8, remote_server_id);
                rdma_store_->FlushPendingSends(remote_server_id);
            } else if (buf[0] == RequestType::ALLOCATE_LOG_BUFFER_SUCC) {
                uint64_t base = leveldb::DecodeFixed64(buf + 1);
                uint64_t size = leveldb::DecodeFixed64(buf + 9);
                rdma_log_writer_->AckAllocLogBuf(remote_server_id, base, size);
            } else if (buf[0] == RequestType::DELETE_LOG_FILE) {
                uint32_t size = leveldb::DecodeFixed32(buf + 1);
                std::string log_file(buf + 5, size);
                log_manager_->DeleteLogBuf(log_file);
            } else {
                RDMA_ASSERT(false) << "memstore[" << rdma_store_->store_id()
                                   << "]: unknown recv from "
                                   << remote_server_id << " buf:"
                                   << buf;
            }
        } else if (opcode == IBV_WC_SEND) {

        } else {
            RDMA_ASSERT(false) << "memstore[" << rdma_store_->store_id()
                               << "]: unknown opcode from "
                               << remote_server_id << " buf:"
                               << buf;
        }
    }
//
//    void
//    NovaAsyncWorker::PostRDMAGETRequest(int fd, char *key, uint64_t nkey,
//                                      int home_server,
//                                      uint64_t remote_offset,
//                                      uint64_t remote_size) {
//        RDMA_ASSERT(remote_size <= NovaConfig::config->max_msg_size);
//        stats.nget_rdma++;
//        char *rdma_send_buf = rdma_store_->GetSendBuf(home_server);
//        int local_offset = GenerateRDMARequest(GET, rdma_send_buf, home_server,
//                                               fd,
//                                               key, nkey);
//        RDMA_LOG(DEBUG) << "memstore[" << thread_id_ << "]: " << " RDMA-Get fd:"
//                        << fd << " key:" << key << " home:" << home_server
//                        << " off:" << remote_offset << " size:" << remote_size
//                        << " loff:" << local_offset;
//        rdma_store_->PostRead(nullptr, remote_size,
//                              home_server,
//                              local_offset, remote_offset, /*is_offset=*/false);
//    }
//
//    void
//    NovaAsyncWorker::PostRDMAGETIndexRequest(int fd, char *key, uint64_t nkey,
//                                           int home_server,
//                                           uint64_t remote_addr) {
//        uint64_t raddr = remote_addr;
//        bool is_offset = false;
//        stats.ngetindex_rdma++;
//        uint32_t bucket_size =
//                IndexEntry::size() *
//                NovaConfig::config->nindex_entry_per_bucket;
//        if (remote_addr == 0) {
//            is_offset = true;
//            raddr = NovaConfig::config->index_buf_offset +
//                    (NovaConfig::keyhash(key, nkey) %
//                     NovaConfig::config->nbuckets) * bucket_size;
//        }
//        char *rdma_send_buf = rdma_store_->GetSendBuf(home_server);
//        int local_offset = GenerateRDMARequest(GET_INDEX, rdma_send_buf,
//                                               home_server, fd, key, nkey);
//        rdma_store_->PostRead(nullptr, bucket_size, home_server, local_offset,
//                              raddr, is_offset);
//    }
//
//    void NovaAsyncWorker::ProcessRDMAGETResponse(uint64_t to_sock_fd,
//                                               DataEntry *entry,
//                                               bool fetch_from_origin) {
//        // return the value.
//        Connection *conn = nova_conns[to_sock_fd];
//        if (fetch_from_origin) {
//            char *response_buf = conn->buf;
//            int nlen = 1;
//            int len = int_to_str(response_buf, nlen);
//            response_buf += len;
//            response_buf[0] = RequestType::FORCE_GET;
//            conn->response_buf = conn->buf;
//            conn->response_size = len + nlen;
//        } else {
//            if (entry == nullptr) {
//                // A miss.
//                char *response_buf = conn->buf;
//                int nlen = 1;
//                int len = int_to_str(response_buf, nlen);
//                response_buf += len;
//                response_buf[0] = RequestType::MISS;
//                conn->response_buf = conn->buf;
//                conn->response_size = len + nlen;
//            } else {
//                conn->response_buf = entry->value();
//                conn->response_size =
//                        nint_to_str(entry->nval) + 1 + 1 + entry->nval;
//            }
//        }
//        RDMA_ASSERT(socket_write_handler(to_sock_fd, conn) == COMPLETE);
//        write_socket_complete(to_sock_fd, conn);
//    }
}