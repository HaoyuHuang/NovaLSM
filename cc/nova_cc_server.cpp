
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <fmt/core.h>
#include <semaphore.h>
#include <db/compaction.h>
#include <db/table_cache.h>

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
                               InMemoryLogFileManager *log_manager,
                               uint32_t thread_id,
                               bool is_compaction_thread,
                               RDMAAdmissionCtrl *admission_control)
            : rdma_ctrl_(rdma_ctrl),
              mem_manager_(mem_manager),
              rtable_manager_(rtable_manager),
              log_manager_(log_manager), thread_id_(thread_id),
              is_compaction_thread_(is_compaction_thread),
              admission_control_(admission_control) {
        current_worker_id_ = thread_id;
    }

    void NovaCCServer::AddCompleteTasks(
            const std::vector<nova::NovaServerCompleteTask> &tasks) {
        mutex_.lock();
        for (auto &task : tasks) {
            public_cq_.push_back(task);
        }
        mutex_.unlock();
    }

    void NovaCCServer::AddCompleteTask(
            const nova::NovaServerCompleteTask &task) {
        mutex_.lock();
        public_cq_.push_back(task);
        mutex_.unlock();
    }

    void NovaCCServer::AddFGStorageTask(const nova::NovaStorageTask &task) {
        uint32_t id =
                fg_storage_worker_seq_id_.fetch_add(1,
                                                    std::memory_order_relaxed) %
                fg_storage_workers_.size();
        fg_storage_workers_[id]->AddTask(task);
    }

    void NovaCCServer::AddBGStorageTask(const nova::NovaStorageTask &task) {
        uint32_t id =
                bg_storage_worker_seq_id_.fetch_add(1,
                                                    std::memory_order_relaxed) %
                bg_storage_workers_.size();
        bg_storage_workers_[id]->AddTask(task);
    }

    void
    NovaCCServer::AddCompactionStorageTask(const nova::NovaStorageTask &task) {
        uint32_t id =
                compaction_storage_worker_seq_id_.fetch_add(1,
                                                            std::memory_order_relaxed) %
                compaction_storage_workers_.size();
        compaction_storage_workers_[id]->AddTask(task);
    }

    int NovaCCServer::ProcessCompletionQueue() {
        int nworks = 0;
        mutex_.lock();
        while (!public_cq_.empty()) {
            auto &task = public_cq_.front();
            private_cq_.push_back(task);
            public_cq_.pop_front();
        }
        mutex_.unlock();
        nworks = private_cq_.size();

        auto it = private_cq_.begin();
        while (it != private_cq_.end()) {
            const auto &task = *it;
            RDMA_ASSERT(task.remote_server_id != -1);
            if (!admission_control_->CanIssueRequest(task.remote_server_id)) {
                it++;
                continue;
            }

            if (task.request_type == leveldb::CC_FILENAME_RTABLEID) {
                char *send_buf = rdma_store_->GetSendBuf(task.remote_server_id);
                send_buf[0] = leveldb::CCRequestType::CC_FILENAME_RTABLEID_RESPONSE;
                rdma_store_->PostSend(send_buf, 1, task.remote_server_id,
                                      task.dc_req_id);
            } else if (task.request_type == leveldb::CC_ALLOCATE_LOG_BUFFER) {
                char *send_buf = rdma_store_->GetSendBuf(task.remote_server_id);
                send_buf[0] = leveldb::CCRequestType::CC_ALLOCATE_LOG_BUFFER_SUCC;
                leveldb::EncodeFixed64(send_buf + 1, (uint64_t) task.rdma_buf);
                leveldb::EncodeFixed64(send_buf + 9,
                                       NovaConfig::config->log_buf_size);
                rdma_store_->PostSend(send_buf, 1 + 8 + 8,
                                      task.remote_server_id,
                                      task.dc_req_id);
            } else if (task.request_type == leveldb::CC_RTABLE_WRITE_SSTABLE) {
                char *sendbuf = rdma_store_->GetSendBuf(task.remote_server_id);
                sendbuf[0] =
                        leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE_RESPONSE;
                leveldb::EncodeFixed32(sendbuf + 1,
                                       task.rtable_id);
                leveldb::EncodeFixed64(sendbuf + 5, task.rtable_offset);
                rdma_store_->PostSend(sendbuf, 13, task.remote_server_id,
                                      task.dc_req_id);
            } else if (task.request_type == leveldb::CC_DC_READ_STATS) {
                char *sendbuf = rdma_store_->GetSendBuf(task.remote_server_id);
                sendbuf[0] =
                        leveldb::CCRequestType::CC_DC_READ_STATS_RESPONSE;
                leveldb::EncodeFixed64(sendbuf + 1,
                                       NovaGlobalVariables::global.dc_queue_depth);
                leveldb::EncodeFixed64(sendbuf + 9,
                                       NovaGlobalVariables::global.dc_pending_disk_reads);
                leveldb::EncodeFixed64(sendbuf + 17,
                                       NovaGlobalVariables::global.dc_pending_disk_writes);
                rdma_store_->PostSend(sendbuf, 13, task.remote_server_id,
                                      task.dc_req_id);
            } else if (task.request_type ==
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
                       leveldb::CCRequestType::CC_RTABLE_PERSIST) {
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
            } else if (task.request_type ==
                       leveldb::CCRequestType::CC_COMPACTION) {
                char *sendbuf = rdma_store_->GetSendBuf(task.remote_server_id);
                sendbuf[0] = leveldb::CCRequestType::CC_COMPACTION_RESPONSE;
                uint32_t msg_size = 1;
                msg_size += leveldb::EncodeFixed32(sendbuf + msg_size,
                                                   task.compaction_state->outputs.size());
                for (auto &out : task.compaction_state->outputs) {
                    msg_size += out.Encode(sendbuf + msg_size);
                }
                rdma_store_->PostSend(sendbuf, msg_size, task.remote_server_id,
                                      task.dc_req_id);
                task.compaction_request->FreeMemoryStoC();
                delete task.compaction_request;
                delete task.compaction_state->compaction;
                delete task.compaction_state;
            } else if (task.request_type ==
                       leveldb::CCRequestType::CC_IS_READY_FOR_REQUESTS) {
                char *sendbuf = rdma_store_->GetSendBuf(task.remote_server_id);
                uint32_t msg_size = 1;
                sendbuf[0] =
                        leveldb::CCRequestType::CC_IS_READY_FOR_REQUESTS_RESPONSE;
                msg_size += leveldb::EncodeBool(sendbuf + 1,
                                                NovaGlobalVariables::global.is_ready_to_process_requests);
                rdma_store_->PostSend(sendbuf, msg_size, task.remote_server_id,
                                      task.dc_req_id);
            } else {
                RDMA_ASSERT(false) << task.request_type;
            }
            RDMA_LOG(DEBUG) << fmt::format(
                        "CCServer[{}]: Completed Request ss:{} req:{} type:{}",
                        thread_id_, task.remote_server_id, task.dc_req_id,
                        task.request_type);
            it = private_cq_.erase(it);
        }
        return nworks;
    }

    // No need to flush RDMA requests since Flush will be done after all requests are processed in a receive queue.
    bool
    NovaCCServer::ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id,
                                int remote_server_id, char *buf,
                                uint32_t imm_data,
                                bool *) {
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
                            RDMA_ASSERT(rtable_manager_->FindRTable(
                                    context.rtable_id)->MarkOffsetAsWritten(
                                    context.rtable_id,
                                    context.rtable_offset)) << fmt::format(
                                        "dc[{}]: Write RTable failed id:{} offset:{} creq_id:{} req_id:{}",
                                        thread_id_, context.rtable_id,
                                        context.rtable_offset, dc_req_id,
                                        req_id);
                            processed = true;

                            RDMA_LOG(DEBUG) << fmt::format(
                                        "dc[{}]: Write RTable complete id:{} offset:{} creq_id:{} req_id:{}",
                                        thread_id_, context.rtable_id,
                                        context.rtable_offset, dc_req_id,
                                        req_id);

                            NovaStorageTask task = {};
                            task.request_type = leveldb::CCRequestType::CC_RTABLE_PERSIST;
                            task.remote_server_id = remote_server_id;
                            task.cc_server_thread_id = thread_id_;
                            task.dc_req_id = dc_req_id;
                            task.is_meta_blocks = context.is_meta_blocks;
                            leveldb::SSTableRTablePair pair = {};
                            pair.rtable_id = context.rtable_id;
                            pair.sstable_id = context.sstable_id;
                            task.persist_pairs.push_back(pair);
                            AddBGStorageTask(task);
                            request_context_map_.erase(req_id);
                        }
                    }
                }

                if (buf[0] == leveldb::CCRequestType::CC_DC_READ_STATS) {
                    processed = true;
                    NovaServerCompleteTask ct = {};
                    ct.remote_server_id = remote_server_id;
                    ct.request_type = leveldb::CCRequestType::CC_DC_READ_STATS;
                    ct.dc_req_id = dc_req_id;
                    private_cq_.push_back(ct);
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
                        rtable_manager_->DeleteSSTable(sstable_id);
                        leveldb::FileType type;
                        RDMA_ASSERT(leveldb::ParseFileName(sstable_id, &type));
                        if (type == leveldb::FileType::kTableFile) {
                            rtable_manager_->DeleteSSTable(
                                    sstable_id + "-meta");
                        }
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
                    bool is_foreground_read = leveldb::DecodeBool(
                            buf + msg_size);
                    msg_size += 1;
                    rtable_id = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    offset = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;
                    size = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    cc_mr_offset = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;
                    std::string filename;
                    leveldb::DecodeStr(buf + msg_size, &filename);
                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc{}: Read blocks of RTable {} offset:{} size:{} cc_mr_offset:{} file:{}",
                                thread_id_, rtable_id, offset, size,
                                cc_mr_offset,
                                filename);

                    if (!filename.empty()) {
                        rtable_id = rtable_manager_->OpenRTable(thread_id_,
                                                                filename)->rtable_id();
                    }

                    uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                              size);
                    char *rdma_buf = mem_manager_->ItemAlloc(thread_id_, scid);
                    RDMA_ASSERT(rdma_buf);

                    NovaStorageTask task = {};
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

                    if (is_foreground_read) {
                        AddFGStorageTask(task);
                    } else {
                        AddBGStorageTask(task);
                    }
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

                    std::string filename;
                    if (file_number == 0) {
                        filename = leveldb::DescriptorFileName(dbname, 0);
                    } else {
                        filename = leveldb::TableFileName(dbname,
                                                          file_number,
                                                          is_meta_blocks);
                    }

                    leveldb::NovaRTable *rtable = rtable_manager_->OpenRTable(
                            thread_id_, filename);
                    uint64_t rtable_off = rtable->AllocateBuf(
                            filename, size, is_meta_blocks);
                    RDMA_ASSERT(rtable_off != UINT64_MAX)
                        << fmt::format("dc{}: {} {}", thread_id_, filename,
                                       size);
                    RDMA_ASSERT(rtable->rtable_name_ == filename)
                        << fmt::format("dc{}: {} {}", thread_id_,
                                       rtable->rtable_name_, filename);


                    NovaServerCompleteTask task = {};
                    task.request_type = leveldb::CC_RTABLE_WRITE_SSTABLE;
                    task.remote_server_id = remote_server_id;
                    task.dc_req_id = dc_req_id;
                    task.rtable_id = rtable->rtable_id();
                    task.rtable_offset = rtable_off;
                    private_cq_.push_back(task);

                    RequestContext context = {};
                    context.request_type = leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE;
                    context.rtable_id = rtable->rtable_id();
                    context.rtable_offset = rtable_off;
                    context.sstable_id = filename;
                    context.is_meta_blocks = is_meta_blocks;
                    context.size = size;
                    request_context_map_[req_id] = context;

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc{}: Allocate buf for RTable Write db:{} fn:{} size:{} rtable_id:{} rtable_off:{} fname:{}",
                                thread_id_, dbname, file_number, size,
                                rtable->rtable_id(), rtable_off, filename);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_ALLOCATE_LOG_BUFFER) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    std::string log_file(buf + 5, size);
                    uint32_t slabclassid = mem_manager_->slabclassid(thread_id_,
                                                                     nova::NovaConfig::config->log_buf_size);
                    char *rmda_buf = mem_manager_->ItemAlloc(thread_id_,
                                                             slabclassid);
                    RDMA_ASSERT(rmda_buf) << "Running out of memory";
                    log_manager_->Add(thread_id_, log_file, rmda_buf);

                    NovaServerCompleteTask task = {};
                    task.request_type = leveldb::CC_ALLOCATE_LOG_BUFFER;
                    task.rdma_buf = rmda_buf;
                    task.remote_server_id = remote_server_id;
                    task.dc_req_id = dc_req_id;
                    private_cq_.push_back(task);

                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: Allocate log buffer for file {}.",
                                thread_id_, log_file);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_QUERY_LOG_FILES) {
                    uint32_t server_id = leveldb::DecodeFixed32(buf + 1);
                    uint32_t dbid = leveldb::DecodeFixed32(buf + 5);
                    std::unordered_map<std::string, uint64_t> logfile_offset;
                    log_manager_->QueryLogFiles(server_id, dbid,
                                                &logfile_offset);
                    // TODO.
                    uint32_t msg_size = 0;
                    char *send_buf = rdma_store_->GetSendBuf(remote_server_id);
                    send_buf[msg_size] = leveldb::CCRequestType::CC_QUERY_LOG_FILES_RESPONSE;
                    msg_size += 1;
                    msg_size += leveldb::EncodeFixed32(send_buf + msg_size,
                                                       logfile_offset.size());
                    for (const auto &it : logfile_offset) {
                        msg_size += leveldb::EncodeStr(send_buf + msg_size,
                                                       it.first);
                        msg_size += leveldb::EncodeFixed64(send_buf + msg_size,
                                                           it.second);
                    }
                    rdma_store_->PostSend(send_buf, msg_size, remote_server_id,
                                          dc_req_id);
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_DELETE_LOG_FILE) {
                    int size = 1;
                    uint32_t nlogs = leveldb::DecodeFixed32(buf + 1);
                    size += 4;
                    std::vector<std::string> logfiles;
                    for (int i = 0; i < nlogs; i++) {
                        std::string log;
                        size += leveldb::DecodeStr(buf + size, &log);
                        logfiles.push_back(log);
                    }
                    log_manager_->DeleteLogBuf(logfiles);
                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: Delete log buffer for file {}.",
                                thread_id_, logfiles.size());
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_FILENAME_RTABLEID) {
                    uint32 read_size = 1;
                    uint32_t nfiles = leveldb::DecodeFixed32(buf + read_size);
                    read_size += 4;
                    std::unordered_map<std::string, uint32_t> fn_rtable;

                    for (int i = 0; i < nfiles; i++) {
                        std::string fn;
                        read_size += leveldb::DecodeStr(buf + read_size, &fn);
                        uint32_t rtableid = leveldb::DecodeFixed32(
                                buf + read_size);
                        read_size += 4;
                        fn_rtable[fn] = rtableid;
                    }
                    rtable_manager_->OpenRTables(fn_rtable);
                    NovaServerCompleteTask task = {};
                    task.remote_server_id = remote_server_id;
                    task.request_type = leveldb::CC_FILENAME_RTABLEID;
                    task.dc_req_id = dc_req_id;
                    private_cq_.push_back(task);
                    RDMA_LOG(DEBUG) << fmt::format(
                                "dc[{}]: Filename rtable mapping {}.",
                                thread_id_, fn_rtable.size());
                    processed = true;
                } else if (buf[0] == leveldb::CCRequestType::CC_COMPACTION) {
                    auto req = new leveldb::CompactionRequest;
                    req->DecodeRequest(buf + 1,
                                       nova::NovaConfig::config->max_msg_size);
                    NovaStorageTask task = {};
                    task.dc_req_id = dc_req_id;
                    task.cc_server_thread_id = thread_id_;
                    task.remote_server_id = remote_server_id;
                    task.request_type = leveldb::CCRequestType::CC_COMPACTION;
                    task.compaction_request = req;
                    AddCompactionStorageTask(task);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::CCRequestType::CC_IS_READY_FOR_REQUESTS) {
                    processed = true;
                    NovaServerCompleteTask ct = {};
                    ct.remote_server_id = remote_server_id;
                    ct.request_type = leveldb::CCRequestType::CC_IS_READY_FOR_REQUESTS;
                    ct.dc_req_id = dc_req_id;
                    private_cq_.push_back(ct);
                }
                break;
        }
        return processed;
    }
}