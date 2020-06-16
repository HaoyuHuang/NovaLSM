
//
// Created by Haoyu Huang on 2/12/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <fmt/core.h>

#include "rdma_write_server_worker.h"

namespace nova {
    void RDMAWRITEServerWorker::AddCompleteTasks(
            const std::vector<nova::ServerWorkerCompleteTask> &tasks) {
        mutex_.lock();
        for (auto &task : tasks) {
            async_cq_.push_back(task);
        }
        mutex_.unlock();
    }

    void RDMAWRITEServerWorker::AddCompleteTask(
            const nova::ServerWorkerCompleteTask &task) {
        mutex_.lock();
        async_cq_.push_back(task);
        mutex_.unlock();
    }

    void RDMAWRITEServerWorker::AddAsyncTask(
            const nova::ServerWorkerAsyncTask &task) {
        async_workers_[task.dc_req_id % async_workers_.size()]->AddTask(task);
    }

    int RDMAWRITEServerWorker::PullAsyncCQ() {
        int nworks = 0;
        mutex_.lock();
        nworks = async_cq_.size();
        while (!async_cq_.empty()) {
            auto &task = async_cq_.front();
            if (task.request_type ==
                BenchRequestType::BENCH_PERSIST) {
                if (!is_local_disk_bench_) {
                    char *sendbuf = rdma_broker_->GetSendBuf(
                            task.remote_server_id);
                    sendbuf[0] = BenchRequestType::BENCH_PERSIST_RESPONSE;
                    uint32_t msg_size = 1;
                    rdma_broker_->PostSend(sendbuf, msg_size,
                                           task.remote_server_id,
                                           task.dc_req_id);
                }
                req_context_.erase(task.dc_req_id);
            } else {
                NOVA_ASSERT(false);
            }

            NOVA_LOG(DEBUG)
                << fmt::format(
                        "server[{}]: persist complete req:{} for server {}",
                        thread_id_, task.dc_req_id, task.remote_server_id);
            async_cq_.pop_front();
        }
        mutex_.unlock();
        if (nworks > 0 && !is_local_disk_bench_) {
            rdma_broker_->FlushPendingSends();
        }
        return nworks;
    }

    RDMAWRITEServerWorker::RDMAWRITEServerWorker(uint32_t max_run_time,
                                                 uint32_t write_size_kb,
                                                 bool is_local_disk_bench,
                                                 bool eval_disk_horizontal_scalability,
                                                 uint32_t server_id)
            : max_run_time_(max_run_time), write_size_kb_(write_size_kb),
              is_local_disk_bench_(is_local_disk_bench),
              eval_disk_horizontal_scalability_(
                      eval_disk_horizontal_scalability), server_id_(server_id) {
    }


    void RDMAWRITEServerWorker::Start() {
        struct ::timeval start_timeval;
        ::gettimeofday(&start_timeval, nullptr);
        int64_t start_unix_time = start_timeval.tv_sec;

        NOVA_LOG(INFO) << fmt::format("worker[{}]: Started", thread_id_);

        if (is_local_disk_bench_) {
//            uint32_t scid = mem_manager_->slabclassid(thread_id_,
//                                                      write_size_kb_ *
//                                                      1024);
//            char *buf = mem_manager_->ItemAlloc(thread_id_, scid);
//            RDMA_ASSERT(buf);
//
//            std::string path = fmt::format("{}/{}", table_path_, thread_id_);
//            mkdirs(path.c_str());
//            MockRTable *rtable = new MockRTable(env_, path, rtable_size_,
//                                                max_num_rtables_);



            while (true) {
                rtable_->Read(4096);
//                int read = rand() % 100;
//                if (read <= 1) {
//                    rtable->Persist(buf, write_size_kb_ * 1024);
//                } else {
//                    if () {
//
//                    } else {
//                        rtable->Persist(buf, write_size_kb_ * 1024);
//                    }
//                }

//                ServerWorkerAsyncTask task = {};
//                task.local_buf = buf;
//                task.dc_req_id = processed_number_of_req_;
//                task.remote_server_id = 0;
//                task.request_type = BenchRequestType::BENCH_PERSIST;
//                task.cc_server_thread_id = thread_id_;
//                AddAsyncTask(task);
//
//                while (PullAsyncCQ() != 1);

                processed_number_of_req_ += 1;

                if (processed_number_of_req_ % 10 == 0) {
                    struct ::timeval timeval;
                    ::gettimeofday(&timeval, nullptr);
                    int64_t unix_time = timeval.tv_sec;

                    if (unix_time - start_unix_time > max_run_time_) {
                        break;
                    }
                }
            }
            return;
        }

        rdma_broker_->Init(rdma_ctrl_);


        while (true) {
            if (eval_disk_horizontal_scalability_ && server_id_ != 0) {
//                rdma_store_->PollSQ();
//                rdma_store_->PollRQ();
                PullAsyncCQ();
                processed_number_of_req_ += 1;
                if (processed_number_of_req_ % 10000 == 0) {
                    struct ::timeval timeval;
                    ::gettimeofday(&timeval, nullptr);
                    int64_t unix_time = timeval.tv_sec;

                    if (unix_time - start_unix_time > max_run_time_) {
                        break;
                    }
                }
                continue;
            }

            uint32_t req_id = client_->Initiate();
            processed_number_of_req_ += 1;

            while (!client_->IsDone(req_id)) {
//                rdma_store_->PollSQ();
//                rdma_store_->PollRQ();
                PullAsyncCQ();
            }

            if (processed_number_of_req_ % 1000 == 0) {
                struct ::timeval timeval;
                ::gettimeofday(&timeval, nullptr);
                int64_t unix_time = timeval.tv_sec;

                if (unix_time - start_unix_time > max_run_time_) {
                    break;
                }
            }
        }

        NOVA_LOG(INFO)
            << fmt::format("worker[{}]: Prepare to terminate", thread_id_);

        usleep(100000);
        for (int i = 0; i < 1000000; i++) {
//            rdma_store_->PollSQ();
//            rdma_store_->PollRQ();
            PullAsyncCQ();
        }
    }

    bool
    RDMAWRITEServerWorker::ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id,
                                         int remote_server_id, char *buf,
                                         uint32_t req_id, bool *new_request) {
        bool processed_by_client = client_->ProcessRDMAWC(type, wr_id,
                                                          remote_server_id, buf,
                                                          req_id, new_request);
        bool processed_by_server = false;

        switch (type) {
            case IBV_WC_SEND:
                processed_by_server = true;
                break;
            case IBV_WC_RDMA_WRITE:
                break;
            case IBV_WC_RDMA_READ:
                processed_by_server = true;
                break;
            case IBV_WC_RECV:
            case IBV_WC_RECV_RDMA_WITH_IMM:
                auto it = req_context_.find(req_id);
                if (it != req_context_.end()) {
                    // Mark as written.
                    processed_by_server = true;
                    NOVA_ASSERT(it->second.local_buf[0] == '1');

                    NOVA_LOG(DEBUG)
                        << fmt::format(
                                "server[{}]: written req:{} for server {}",
                                thread_id_, req_id, remote_server_id);

                    if (buf[0] == BenchRequestType::BENCH_PERSIST) {
                        ServerWorkerAsyncTask task = {};
                        task.request_type = BenchRequestType::BENCH_PERSIST;
                        task.remote_server_id = remote_server_id;
                        task.local_buf = it->second.local_buf;
                        task.dc_req_id = req_id;
                        task.cc_server_thread_id = thread_id_;
                        AddAsyncTask(task);

                        NOVA_LOG(DEBUG)
                            << fmt::format(
                                    "server[{}]: persist req:{} for server {}",
                                    thread_id_, req_id, remote_server_id);
                    }
                    break;
                }

                if (buf[0] == BenchRequestType::BENCH_ALLOCATE) {
                    uint64_t asize = leveldb::DecodeFixed64(buf + 1);
                    uint64_t size = write_size_kb_ * 1024;
                    NOVA_ASSERT(asize == size);

                    uint32_t scid = mem_manager_->slabclassid(thread_id_, size);
                    char *local_buf = mem_manager_->ItemAlloc(thread_id_, scid);
                    NOVA_ASSERT(local_buf);
                    local_buf[0] = '2';

                    RequestContext ctx = {};
                    ctx.local_buf = local_buf;
                    req_context_[req_id] = ctx;

                    char *sendbuf = rdma_broker_->GetSendBuf(remote_server_id);
                    sendbuf[0] = BenchRequestType::BENCH_ALLOCATE_RESPONSE;
                    leveldb::EncodeFixed64(sendbuf + 1, (uint64_t) (local_buf));
                    rdma_broker_->PostSend(sendbuf, 9, remote_server_id, req_id);
                    processed_by_server = true;

                    NOVA_LOG(DEBUG)
                        << fmt::format(
                                "server[{}]: allocate req:{} for server {}",
                                thread_id_, req_id, remote_server_id);

                }
                break;
        }

        NOVA_ASSERT((processed_by_client || processed_by_server));
        NOVA_ASSERT(!(processed_by_client && processed_by_server));
    }

    RDMAWRITEDiskWorker::RDMAWRITEDiskWorker(const std::string &table_path,
                                             uint32_t write_size_kb,
                                             uint32_t rtable_size,
                                             uint32_t max_num_rtables)
            : table_path_(table_path), write_size_kb_(write_size_kb),
              rtable_size_(rtable_size),
              max_num_rtables_(max_num_rtables) {

    }

    void RDMAWRITEDiskWorker::Init() {
        std::string path = fmt::format("{}/{}", table_path_, worker_id_);
        mkdirs(path.c_str());
        rtable_ = new MockRTable(env_, path, rtable_size_, max_num_rtables_);
        sem_init(&sem_, 0, 0);
    }

    void RDMAWRITEDiskWorker::AddTask(
            const nova::ServerWorkerAsyncTask &task) {
        mutex_.lock();
        queue_.push_back(task);
        mutex_.unlock();

        sem_post(&sem_);
    }

    void RDMAWRITEDiskWorker::Start() {
        NOVA_LOG(DEBUG) << "CC server worker started";

        while (is_running_) {
            sem_wait(&sem_);

            std::vector<ServerWorkerAsyncTask> tasks;
            mutex_.lock();

            while (!queue_.empty()) {
                auto task = queue_.front();
                tasks.push_back(task);
                queue_.pop_front();
            }
            mutex_.unlock();

            if (tasks.empty()) {
                continue;
            }

            std::map<uint32_t, std::vector<ServerWorkerCompleteTask>> t_tasks;
            for (auto &task : tasks) {
                ServerWorkerCompleteTask ct = {};
                ct.remote_server_id = task.remote_server_id;
                ct.dc_req_id = task.dc_req_id;
                ct.request_type = task.request_type;

                if (task.request_type ==
                    BenchRequestType::BENCH_PERSIST) {
                    rtable_->Persist(task.local_buf,
                                     write_size_kb_ * 1024);

                    uint32_t scid = mem_manager_->slabclassid(
                            task.cc_server_thread_id,
                            write_size_kb_ * 1024);
                    mem_manager_->FreeItem(task.cc_server_thread_id,
                                           task.local_buf, scid);
                } else {
                    NOVA_ASSERT(false);
                }

                NOVA_LOG(DEBUG)
                    << fmt::format(
                            "CCWorker: Working on t:{} ss:{} req:{} type:{}",
                            task.cc_server_thread_id, ct.remote_server_id,
                            ct.dc_req_id,
                            ct.request_type);
                t_tasks[task.cc_server_thread_id].push_back(ct);
            }

            for (auto &it : t_tasks) {
                cc_servers_[it.first]->AddCompleteTasks(it.second);
            }
        }

    }
}