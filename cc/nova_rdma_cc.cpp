
//
// Created by Haoyu Huang on 12/25/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <leveldb/write_batch.h>
#include "nova_rdma_cc.h"
#include "nova/nova_config.h"
#include "nova/nova_common.h"

namespace nova {
    void NovaRDMAComputeComponent::AddTask(const NovaAsyncTask &task) {
        mutex_.Lock();
        queue_.push_back(task);
        mutex_.Unlock();

        if (is_worker_thread_) {
            sem_post(&sem_);
        }
    }

    int NovaRDMAComputeComponent::size() {
        mutex_.Lock();
        int size = queue_.size();
        mutex_.Unlock();
        return size;
    }

    void NovaRDMAComputeComponent::ProcessVerify(
            const nova::NovaAsyncTask &task) {
        // Verify loaded data are correct.
        // Assert the loaded data is valid.
        leveldb::ReadOptions read_options = {};
        read_options.mem_manager = mem_manager_;
        read_options.dc_client = cc_client_;
        read_options.thread_id = thread_id_;
        read_options.verify_checksums = false;
        std::vector<CCFragment *> &frags = NovaCCConfig::cc_config->fragments;
        for (int i = 0; i < frags.size(); i++) {
            if (frags[i]->cc_server_id !=
                NovaConfig::config->my_server_id) {
                continue;
            }
            leveldb::DB *db = dbs_[frags[i]->dbid];

            RDMA_LOG(INFO) << "Verify range "
                           << frags[i]->range.key_start
                           << " to "
                           << frags[i]->range.key_end;

            for (uint64_t j = frags[i]->range.key_end;
                 j >= frags[i]->range.key_start; j--) {
                auto v = static_cast<char>((j % 10) + 'a');
                std::string key = std::to_string(j);
                std::string expected_val(
                        NovaConfig::config->load_default_value_size, v
                );
                std::string val;
                leveldb::Status status = db->Get(read_options, key, &val);
                RDMA_ASSERT(status.ok())
                    << fmt::format("key:{} status:{}", key, status.ToString());
                RDMA_ASSERT(expected_val.compare(val) == 0) << val;

                if (j == frags[i]->range.key_start) {
                    break;
                }
            }

            RDMA_LOG(INFO) << "Success: Verified range "
                           << frags[i]->range.key_start
                           << " to "
                           << frags[i]->range.key_end;
        }
    }

    void NovaRDMAComputeComponent::ProcessPut(const nova::NovaAsyncTask &task) {
        uint64_t hv = keyhash(task.key.data(), task.key.size());
        leveldb::WriteOptions option;
        option.dc_client = cc_client_;
        option.sync = true;
        option.local_write = false;
        option.thread_id = thread_id_;
        CCFragment *frag = NovaCCConfig::home_fragment(hv);
        leveldb::DB *db = dbs_[frag->dbid];
        if (!option.local_write) {
            leveldb::WriteBatch batch;
            batch.Put(task.key, task.value);
            db->GenerateLogRecords(option, &batch);
        }
        leveldb::Status status = db->Put(option, task.key, task.value);
        RDMA_LOG(DEBUG) << "############### CC worker processed task "
                        << task.sock_fd << ":" << task.key;
        RDMA_ASSERT(status.ok()) << status.ToString();

        char *response_buf = task.conn->buf;
        int nlen = 1;
        int len = int_to_str(response_buf, nlen);
        task.conn->response_buf = task.conn->buf;
        task.conn->response_size = len + nlen;
    }

    void NovaRDMAComputeComponent::ProcessGet(const nova::NovaAsyncTask &task) {
        uint64_t hv = keyhash(task.key.data(), task.key.size());
        CCFragment *frag = NovaCCConfig::home_fragment(hv);
        leveldb::DB *db = dbs_[frag->dbid];
        std::string value;
        leveldb::ReadOptions read_options;
        read_options.dc_client = cc_client_;
        read_options.mem_manager = mem_manager_;
        read_options.thread_id = thread_id_;

        leveldb::Status s = db->Get(read_options, task.key, &value);
        RDMA_ASSERT(s.ok()) << s.ToString();
        task.conn->response_buf = task.conn->buf;
        char *response_buf = task.conn->response_buf;
        task.conn->response_size =
                nint_to_str(value.size()) + 1 + 1 + value.size();

        response_buf += int_to_str(response_buf, value.size() + 1);
        response_buf[0] = 'h';
        response_buf += 1;
        memcpy(response_buf, value.data(), value.size());
        RDMA_ASSERT(
                task.conn->response_size <
                NovaConfig::config->max_msg_size);
    }

    int NovaRDMAComputeComponent::ProcessQueue() {
        mutex_.Lock();
        if (queue_.empty()) {
            mutex_.Unlock();
            return 0;
        }
        std::list<NovaAsyncTask> queue(queue_.begin(), queue_.end());
        mutex_.Unlock();

        for (const NovaAsyncTask &task : queue) {
            switch (task.type) {
                case RequestType::PUT:
                    ProcessPut(task);
                    conn_workers_[task.conn_worker_id] = true;
                    break;
                case RequestType::GET:
                    ProcessGet(task);
                    conn_workers_[task.conn_worker_id] = true;
                    break;
                case RequestType::VERIFY_LOAD:
                    ProcessVerify(task);
                    break;
            }
        }

        mutex_.Lock();
        auto begin = queue_.begin();
        auto end = queue_.begin();
        std::advance(end, queue.size());
        queue_.erase(begin, end);
        mutex_.Unlock();

        for (int i = 0; i < NovaCCConfig::cc_config->num_conn_workers; i++) {
            if (!conn_workers_[i]) {
                continue;
            }
            conn_workers_[i] = false;
            cqs_[i]->mutex.Lock();
            for (const NovaAsyncTask &task : queue) {
                if (task.conn_worker_id != i) {
                    continue;
                }
                NovaAsyncCompleteTask t = {
                        .sock_fd = task.sock_fd,
                        .conn = task.conn
                };
                cqs_[i]->queue.push_back(t);
            }
            cqs_[i]->mutex.Unlock();

            char buf[1];
            buf[0] = 'a';
            RDMA_ASSERT(write(cqs_[i]->write_fd, buf, 1) == 1);
        }
        return queue.size();
    }

    bool NovaRDMAComputeComponent::IsInitialized() {
        mutex_.Lock();
        bool t = is_running_;
        mutex_.Unlock();
        return t;
    }

    void NovaRDMAComputeComponent::Start() {
        RDMA_LOG(INFO) << "Async worker started";

        if (NovaConfig::config->enable_rdma) {
            rdma_store_->Init(rdma_ctrl_);
        }

        mutex_.Lock();
        is_running_ = true;
        mutex_.Unlock();

        if (is_worker_thread_) {
            sem_wait(&sem_);
        }

        bool should_sleep = true;
        uint32_t timeout = RDMA_POLL_MIN_TIMEOUT_US;
        while (is_running_) {
//            if (should_sleep) {
//                usleep(timeout);
//            }
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
        }
    }

    bool
    NovaRDMAComputeComponent::ProcessRDMAWC(ibv_wc_opcode opcode,
                                            uint64_t wr_id,
                                            int remote_server_id,
                                            char *buf, uint32_t imm_data) {
        if (opcode == IBV_WC_SEND || opcode == IBV_WC_RDMA_READ) {
            return true;
        }
        bool processed_by_client = cc_client_->OnRecv(opcode, wr_id,
                                                      remote_server_id, buf,
                                                      imm_data);
        bool processed_by_server = cc_server_->ProcessRDMAWC(opcode, wr_id,
                                                             remote_server_id,
                                                             buf,
                                                             imm_data);
        if (processed_by_client && processed_by_server) {
            RDMA_ASSERT(false) << fmt::format("Processed by both client and server");
        }
    }
}