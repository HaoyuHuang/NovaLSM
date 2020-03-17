
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <fmt/core.h>
#include <semaphore.h>

#include "db/filename.h"
#include "nova_cc_server.h"
#include "cc/nova_cc_client.h"
#include "nova/nova_config.h"

namespace nova {
    namespace {
        uint64_t to_req_id(uint32_t remote_sid, uint32_t dc_req_id) {
            uint64_t req_id = 0;
            req_id = ((uint64_t) remote_sid) << 32;
            return req_id + dc_req_id;
        }

        uint64_t to_dc_req_id(uint64_t req_id) {
            return (uint32_t) (req_id);
        }
    }

    NovaCCServer::NovaCCServer(rdmaio::RdmaCtrl *rdma_ctrl,
                               NovaMemManager *mem_manager,
                               leveldb::NovaRTableManager *rtable_manager,
                               LogFileManager *log_manager, uint32_t thread_id,
                               bool is_compaction_thread)
            : rdma_ctrl_(rdma_ctrl),
              mem_manager_(mem_manager),
              rtable_manager_(rtable_manager),
              log_manager_(log_manager), thread_id_(thread_id),
              is_compaction_thread_(is_compaction_thread) {
        if (is_compaction_thread) {
            current_rtable_ = rtable_manager_->CreateNewRTable(thread_id_);
        }
        current_worker_id_ = thread_id;
    }

    void NovaCCServer::AddCompleteTasks(
            const std::vector<nova::NovaServerCompleteTask> &tasks) {
        mutex_.lock();
        for (auto &task : tasks) {
            async_cq_.push_back(task);
        }
        mutex_.unlock();
    }

    void NovaCCServer::AddCompleteTask(
            const nova::NovaServerCompleteTask &task) {
        mutex_.lock();
        async_cq_.push_back(task);
        mutex_.unlock();
    }

    void NovaCCServer::AddAsyncTask(const nova::NovaServerAsyncTask &task) {
        current_worker_id_ = current_worker_id_ % async_workers_.size();
        async_workers_[current_worker_id_]->AddTask(task);
        current_worker_id_ += 1;

//        async_workers_[task.rtable_id % async_workers_.size()]->AddTask(task);
//        task.rtable_id % async_workers_.size();
    }

    int NovaCCServer::PullAsyncCQ() {
        int nworks = 0;
        mutex_.lock();
        nworks = async_cq_.size();
        while (!async_cq_.empty()) {
            auto &task = async_cq_.front();
            if (task.request_type ==
                leveldb::CCRequestType::CC_RTABLE_READ_BLOCKS) {
                char *sendbuf = rdma_store_->GetSendBuf(task.remote_server_id);
                uint64_t wr_id = rdma_store_->PostWrite(task.rdma_buf,
                                                        task.rtable_handle.size,
                                                        task.remote_server_id,
                                                        task.cc_mr_offset,
                                                        false,
                                                        task.dc_req_id);
                sendbuf[0] = leveldb::CCRequestType::CC_RTABLE_READ_BLOCKS;
                leveldb::EncodeFixed64(sendbuf + 1, wr_id);
                leveldb::EncodeFixed32(sendbuf + 9, task.rtable_handle.size);
                leveldb::EncodeFixed64(sendbuf + 13,
                                       (uint64_t) (task.rdma_buf));
            } else if (task.request_type ==
                       leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE) {
                char *sendbuf = rdma_store_->GetSendBuf(task.remote_server_id);
                sendbuf[0] = leveldb::CCRequestType::CC_RTABLE_PERSIST_RESPONSE;
                uint32_t msg_size = 1;
                leveldb::EncodeFixed32(sendbuf + msg_size,
                                       task.rtable_handles.size());
                msg_size += 4;
                for (int i = 0; i < task.rtable_handles.size(); i++) {
                    task.rtable_handles[i].EncodeHandle(sendbuf + msg_size);
                    msg_size += leveldb::RTableHandle::HandleSize();
                }
                rdma_store_->PostSend(sendbuf, msg_size, task.remote_server_id,
                                      task.dc_req_id);
            } else {
                RDMA_ASSERT(false);
            }
            async_cq_.pop_front();
            RDMA_LOG(DEBUG) << fmt::format(
                        "CCServer[{}]: Completed Request ss:{} req:{} type:{}",
                        thread_id_, task.remote_server_id, task.dc_req_id,
                        task.request_type);
        }
        mutex_.unlock();
        if (nworks > 0) {
            rdma_store_->FlushPendingSends();
        }
        return nworks;
    }

    // No need to flush RDMA requests since Flush will be done after all requests are processed in a receive queue.
    bool
    NovaCCServer::ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id,
                                int remote_server_id, char *buf,
                                uint32_t imm_data) {
        bool processed = false;
        switch (type) {
            case IBV_WC_SEND:
                break;
            case IBV_WC_RDMA_WRITE:
                if (buf[0] == leveldb::CCRequestType::CC_RTABLE_READ_BLOCKS) {
                    uint64_t written_wr_id = leveldb::DecodeFixed64(buf + 1);
                    if (written_wr_id == wr_id) {
                        uint32_t size = leveldb::DecodeFixed32(buf + 9);
                        uint64_t allocated_buf_int = leveldb::DecodeFixed64(
                                buf + 13);
                        char *allocated_buf = (char *) (allocated_buf_int);
                        uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                                  size);
                        mem_manager_->FreeItem(thread_id_, allocated_buf, scid);
                        processed = true;

                        RDMA_LOG(DEBUG) << fmt::format(
                                    "dc[{}]: imm:{} type:{} allocated buf:{} size:{} wr:{}.",
                                    thread_id_,
                                    imm_data,
                                    buf[0], allocated_buf_int, size, wr_id);
                    }
                }
                break;
            case IBV_WC_RDMA_READ:
                break;
            case IBV_WC_RECV:
            case IBV_WC_RECV_RDMA_WITH_IMM:
                uint32_t dc_req_id = imm_data;
                uint64_t req_id = to_req_id(remote_server_id, dc_req_id);

                auto context_it = request_context_map_.find(req_id);
                if (context_it != request_context_map_.end()) {
                    auto &context = context_it->second;
                    // Waiting for writes.
                    if (context.request_type ==
                        leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE) {
                        if (IsRDMAWRITEComplete((char *) context.rtable_offset,
                                                context.size)) {
                            RDMA_ASSERT(buf[0] == '~');

                            rtable_manager_->rtable(
                                    context.rtable_id)->MarkOffsetAsWritten(
                                    context.rtable_offset);
                            processed = true;

                            RDMA_LOG(DEBUG) << fmt::format(
                                        "dc[{}]: Write RTable complete id:{} offset:{} creq_id:{} req_id:{}",
                                        thread_id_, context.rtable_id,
                                        context.rtable_offset, dc_req_id,
                                        req_id);

                            NovaServerAsyncTask task = {};
                            task.request_type = leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE;
                            task.remote_server_id = remote_server_id;
                            task.cc_server_thread_id = thread_id_;
                            task.dc_req_id = dc_req_id;
                            task.is_meta_blocks = context.is_meta_blocks;
                            leveldb::SSTableRTablePair pair = {};
                            pair.rtable_id = context.rtable_id;
                            pair.sstable_id = context.sstable_id;
                            task.persist_pairs.push_back(pair);
                            AddAsyncTask(task);

                            request_context_map_.erase(req_id);
                        }
                    }
                }

                if (buf[0] == leveldb::CCRequestType::CC_DC_READ_STATS) {
                    char *sendbuf = rdma_store_->GetSendBuf(remote_server_id);
                    sendbuf[0] =
                            leveldb::CCRequestType::CC_DC_READ_STATS_RESPONSE;
                    leveldb::EncodeFixed64(sendbuf + 1,
                                           dc_stats.dc_queue_depth);
                    leveldb::EncodeFixed64(sendbuf + 9,
                                           dc_stats.dc_pending_disk_reads);
                    leveldb::EncodeFixed64(sendbuf + 17,
                                           dc_stats.dc_pending_disk_writes);
                    rdma_store_->PostSend(sendbuf, 13, remote_server_id,
                                          dc_req_id);
                    processed = true;
                } else if (buf[0] == leveldb::CCRequestType::CC_DELETE_TABLES) {
                    uint32_t msg_size = 1;
                    uint32_t nrtables = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;

                    for (int i = 0; i < nrtables; i++) {
                        std::string sstable_id;
                        msg_size += leveldb::DecodeStr(buf + msg_size,
                                                       &sstable_id);
                        uint32_t rtableid = leveldb::DecodeFixed32(
                                buf + msg_size);
                        msg_size += 4;
                        rtable_manager_->rtable(rtableid)->DeleteSSTable(
                                sstable_id);
                    }

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: Delete SSTables. nsstables:{}",
                                thread_id_, nrtables);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_RTABLE_READ_BLOCKS) {
                    uint32_t msg_size = 1;
                    uint32_t rtable_id = 0;
                    uint64_t offset = 0;
                    uint32_t size = 0;
                    uint64_t cc_mr_offset = 0;

                    rtable_id = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    offset = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;
                    size = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    cc_mr_offset = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;

                    uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                              size);
                    char *rdma_buf = mem_manager_->ItemAlloc(thread_id_, scid);
                    RDMA_ASSERT(rdma_buf);

                    NovaServerAsyncTask task = {};
                    task.dc_req_id = dc_req_id;
                    task.cc_server_thread_id = thread_id_;
                    task.remote_server_id = remote_server_id;
                    task.request_type = leveldb::CCRequestType::CC_RTABLE_READ_BLOCKS;
                    task.rtable_id = rtable_id;

                    task.cc_mr_offset = cc_mr_offset;
                    task.rdma_buf = rdma_buf;
                    task.rtable_handle.server_id = nova::NovaConfig::config->my_server_id;
                    task.rtable_handle.rtable_id = rtable_id;
                    task.rtable_handle.offset = offset;
                    task.rtable_handle.size = size;

                    AddAsyncTask(task);

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc{}: Read blocks of RTable {} offset:{} size:{} cc_mr_offset:{} last:{}",
                                thread_id_, rtable_id, offset, size,
                                cc_mr_offset,
                                rdma_buf[task.rtable_handle.size - 1]);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE) {
                    uint32_t msg_size = 2;
                    std::string dbname;
                    uint64_t file_number;
                    uint32_t size;
                    bool is_meta_blocks = false;

                    if (buf[1] == 'm') {
                        is_meta_blocks = true;
                    } else {
                        RDMA_ASSERT(buf[1] == 'd');
                        is_meta_blocks = false;
                    }

                    msg_size += leveldb::DecodeStr(buf + msg_size, &dbname);
                    file_number = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;
                    size = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    std::string table_name = leveldb::TableFileName(dbname,
                                                                    file_number);
                    uint64_t rtable_off = current_rtable_->AllocateBuf(
                            table_name, size, is_meta_blocks);
                    if (rtable_off == UINT64_MAX) {
                        // overflow.
                        // close.
                        current_rtable_ = rtable_manager_->CreateNewRTable(
                                thread_id_);
                        rtable_off = current_rtable_->AllocateBuf(table_name,
                                                                  size,
                                                                  is_meta_blocks);
                    }

                    char *sendbuf = rdma_store_->GetSendBuf(remote_server_id);
                    sendbuf[0] =
                            leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE_RESPONSE;
                    leveldb::EncodeFixed32(sendbuf + 1,
                                           current_rtable_->rtable_id());
                    leveldb::EncodeFixed64(sendbuf + 5, rtable_off);
                    rdma_store_->PostSend(sendbuf, 13, remote_server_id,
                                          dc_req_id);

                    MarkCharAsWaitingForRDMAWRITE((char *) rtable_off, size);

                    RequestContext context = {};
                    context.request_type = leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE;
                    context.rtable_id = current_rtable_->rtable_id();
                    context.rtable_offset = rtable_off;
                    context.sstable_id = leveldb::TableFileName(dbname,
                                                                file_number);
                    context.is_meta_blocks = is_meta_blocks;
                    context.size = size;
                    request_context_map_[req_id] = context;

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc{}: Allocate buf for RTable Write db:{} fn:{} size:{} rtable_id:{} rtable_off:{}",
                                thread_id_, dbname, file_number, size,
                                current_rtable_->rtable_id(), rtable_off);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_ALLOCATE_LOG_BUFFER) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    std::string log_file(buf + 5, size);
                    uint32_t slabclassid = mem_manager_->slabclassid(thread_id_,
                                                                     nova::NovaConfig::config->log_buf_size);
                    char *buf = mem_manager_->ItemAlloc(thread_id_,
                                                        slabclassid);
                    RDMA_ASSERT(buf) << "Running out of memory";
                    log_manager_->Add(thread_id_, log_file, buf);
                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
                    send_buf[0] = leveldb::CCRequestType::CC_ALLOCATE_LOG_BUFFER_SUCC;
                    leveldb::EncodeFixed64(send_buf + 1, (uint64_t) buf);
                    leveldb::EncodeFixed64(send_buf + 9,
                                           NovaConfig::config->log_buf_size);
                    rdma_store_->PostSend(send_buf, 1 + 8 + 8, remote_server_id,
                                          0);
                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: Allocate log buffer for file {}.",
                                thread_id_, log_file);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_DELETE_LOG_FILE) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    std::string log_file(buf + 5, size);
                    log_manager_->DeleteLogBuf(log_file);
                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: Delete log buffer for file {}.",
                                thread_id_, log_file);
                    processed = true;
                }
                break;
        }
        return processed;
    }


    NovaCCServerAsyncWorker::NovaCCServerAsyncWorker(
            leveldb::NovaRTableManager *rtable_manager,
            std::vector<NovaCCServer *> cc_servers) : rtable_manager_(
            rtable_manager), cc_servers_(cc_servers) {
        sem_init(&sem_, 0, 0);
    }

    void NovaCCServerAsyncWorker::AddTask(
            const nova::NovaServerAsyncTask &task) {
        mutex_.lock();
        queue_.push_back(task);
        mutex_.unlock();

        sem_post(&sem_);
    }

    void NovaCCServerAsyncWorker::Start() {
        RDMA_LOG(DEBUG) << "CC server worker started";

        nova::NovaConfig::config->add_tid_mapping();

        while (is_running_) {
            sem_wait(&sem_);

            std::vector<NovaServerAsyncTask> tasks;
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

            std::map<uint32_t, std::vector<NovaServerCompleteTask>> t_tasks;
            for (auto &task : tasks) {
                NovaServerCompleteTask ct = {};
                ct.remote_server_id = task.remote_server_id;
                ct.dc_req_id = task.dc_req_id;
                ct.request_type = task.request_type;

                ct.rdma_buf = task.rdma_buf;
                ct.cc_mr_offset = task.cc_mr_offset;
                ct.rtable_handle = task.rtable_handle;

                if (task.request_type ==
                    leveldb::CCRequestType::CC_RTABLE_READ_BLOCKS) {
                    rtable_manager_->ReadDataBlock(task.rtable_handle,
                                                   task.rtable_handle.offset,
                                                   task.rtable_handle.size,
                                                   task.rdma_buf);
                } else if (task.request_type ==
                           leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE) {
                    RDMA_ASSERT(task.persist_pairs.size() == 1);
                    for (auto &pair : task.persist_pairs) {
                        leveldb::NovaRTable *rtable = rtable_manager_->rtable(
                                pair.rtable_id);
                        rtable->Persist();

                        RDMA_LOG(DEBUG) << fmt::format(
                                    "Persisting rtable {} for sstable {}",
                                    pair.rtable_id, pair.sstable_id);

                        leveldb::BlockHandle h = rtable->Handle(
                                pair.sstable_id, task.is_meta_blocks);
                        leveldb::RTableHandle rh = {};
                        rh.server_id = NovaConfig::config->my_server_id;
                        rh.rtable_id = pair.rtable_id;
                        rh.offset = h.offset();
                        rh.size = h.size();
                        ct.rtable_handles.push_back(rh);
                    }
                } else {
                    RDMA_ASSERT(false);
                }

                RDMA_LOG(DEBUG)
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