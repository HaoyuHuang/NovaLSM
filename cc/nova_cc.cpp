
//
// Created by Haoyu Huang on 1/11/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <semaphore.h>

#include "nova_cc.h"

#include "db/filename.h"
#include "nova/nova_config.h"

#define MAX_BLOCK_SIZE 102400

namespace leveldb {
    NovaCCMemFile::NovaCCMemFile(Env *env, uint64_t file_number,
                                 MemManager *mem_manager,
                                 SSTableManager *sstable_manager,
                                 CCClient *cc_client,
                                 const std::string &dbname,
                                 uint64_t thread_id,
                                 uint64_t file_size)
            : env_(env), file_number_(file_number),
              fname_(TableFileName(dbname, file_number)),
              mem_manager_(mem_manager),
              sstable_manager_(sstable_manager),
              cc_client_(cc_client),
              dbname_(dbname), thread_id_(thread_id),
              allocated_size_(file_size),
              MemFile(nullptr, "", false) {

        RDMA_ASSERT(mem_manager);
        RDMA_ASSERT(sstable_manager);
        RDMA_ASSERT(cc_client);

        // Only used for flushing SSTables.
        // Policy.

        int server_id = 0;
        switch (nova::NovaCCConfig::cc_config->scatter_sstable_policy) {
            case nova::ScatterSSTablePolicy::SCATTER_SSTABLE_RANDOM:
                server_id = rand() %
                            nova::NovaCCConfig::cc_config->cc_servers.size();
                break;
        }


        if (server_id != nova::NovaConfig::config->my_server_id) {
            // local
            uint32_t scid = mem_manager->slabclassid(thread_id, file_size);
            backing_mem_ = mem_manager->ItemAlloc(thread_id, scid);
            RDMA_ASSERT(backing_mem_) << "Running out of memory";

            // remote
            uint32_t req_id = cc_client->InitiateAllocateSSTableBuffer(
                    server_id, dbname, file_number,
                    file_size);
            CCResponse response;
            while (!cc_client->IsDone(req_id, &response)) {
                remote_sstable_bufs_[server_id] = response.remote_sstable_buf;
            }
        }

        //  Allocate memory from other CCs in parallel. If fail, create a local linux file.
        //  A hash map in cc_client to remember the remote memory buffer for this file as well.
        //  Fsync: WRITE to the remote buffer. Record the request id.
        //  When compaction completes, wait for all WRITEs to complete.
//        if (backing_mem_) {
//            uint32_t sid;
//            uint32_t db_index;
//            nova::ParseDBIndexFromDBName(dbname, &sid, &db_index);
//            nova::CCFragment *frag = nova::NovaCCConfig::cc_config->db_fragment[db_index];
//            int requests[frag->cc_server_ids.size()];
//            for (int i = 0; i < frag->cc_server_ids.size(); i++) {
//                int replica_id = frag->cc_server_ids[i];
//                if (replica_id == nova::NovaConfig::config->my_server_id) {
//                    continue;
//                }
//
//                uint32_t req_id = cc_client->InitiateAllocateSSTableBuffer(
//                        replica_id, dbname, file_number,
//                        file_size);
//                requests[i] = req_id;
//            }
//
//            // wait for them to complete.
//            for (int i = 0; i < frag->cc_server_ids.size(); i++) {
//                int replica_id = frag->cc_server_ids[i];
//                if (replica_id == nova::NovaConfig::config->my_server_id) {
//                    continue;
//                }
//
//                CCResponse response;
//                while (!cc_client->IsDone(requests[i], &response)) {
//                    if (response.remote_sstable_buf != 0) {
//                        remote_sstable_bufs_[replica_id] = response.remote_sstable_buf;
//                    }
//                }
//            }
//
//            // Check if I got all bufs.
//            if (remote_sstable_bufs_.size() == frag->cc_server_ids.size() - 1) {
//                // Nice.
//
//            } else {
//                // Release all bufs since one of them does not have memory.
//                for (auto &it : remote_sstable_bufs_) {
//                    cc_client->InitiateReleaseSSTableBuffer(it.first, dbname,
//                                                            file_number,
//                                                            file_size);
//                }
//                mem_manager->FreeItem(thread_id, backing_mem_, scid);
//                backing_mem_ = nullptr;
//                remote_sstable_bufs_.clear();
//            }
//        }

        RDMA_LOG(rdmaio::DEBUG) << fmt::format(
                    "Create remote memory file tid:{} fname:{} size:{}",
                    thread_id, fname_, file_size);

        if (!backing_mem_) {
            EnvFileMetadata env_meta;
            env_meta.level = 0;
            RDMA_ASSERT(
                    env_->NewWritableFile(fname_, env_meta,
                                          &local_writable_file_).ok());
        }
    }

    NovaCCMemFile::~NovaCCMemFile() {
        if (backing_mem_) {
            uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                      allocated_size_);
            mem_manager_->FreeItem(thread_id_, backing_mem_, scid);

            RDMA_LOG(rdmaio::DEBUG) << fmt::format(
                        "Free remote memory file tid:{} fn:{} size:{}",
                        thread_id_, fname_, allocated_size_);
        }
        if (local_writable_file_) {
            delete local_writable_file_;
        }
    }

    Status
    NovaCCMemFile::Read(uint64_t offset, size_t n, leveldb::Slice *result,
                        char *scratch) {
        const uint64_t available = Size() - std::min(Size(), offset);
        size_t offset_ = static_cast<size_t>(offset);
        if (n > available) {
            n = static_cast<size_t>(available);
        }
        if (n == 0) {
            *result = Slice();
            return Status::OK();
        }
        if (scratch) {
            memcpy(scratch, &(backing_mem_[offset_]), n);
            *result = Slice(scratch, n);
        } else {
            *result = Slice(&(backing_mem_[offset_]), n);
        }
        return Status::OK();
    }

    Status NovaCCMemFile::Append(const leveldb::Slice &data) {
        if (!backing_mem_) {
            RDMA_ASSERT(local_writable_file_);
            return local_writable_file_->Append(data);
        }

        char *buf = backing_mem_ + used_size_;
        RDMA_ASSERT(used_size_ + data.size() < allocated_size_)
            << fmt::format(
                    "ccremotememfile[{}]: fn:{} db:{} alloc_size:{} used_size:{} data size:{}",
                    thread_id_, fname_, dbname_, allocated_size_, used_size_,
                    data.size());
        memcpy(buf, data.data(), data.size());
        used_size_ += data.size();
        return Status::OK();
    }

    Status
    NovaCCMemFile::Write(uint64_t offset, const leveldb::Slice &data) {
        assert(offset + data.size() < allocated_size_);
        memcpy(backing_mem_ + offset, data.data(), data.size());
        if (offset + data.size() > used_size_) {
            used_size_ = offset + data.size();
        }
        return Status::OK();
    }

    Status NovaCCMemFile::Fsync() {
        RDMA_ASSERT(used_size_ == meta_.file_size) << fmt::format(
                    "ccremotememfile[{}]: fn:{} db:{} alloc_size:{} used_size:{}",
                    thread_id_, fname_, dbname_, allocated_size_, used_size_);
        for (auto &it : remote_sstable_bufs_) {
            uint32_t reqid = cc_client_->InitiateWRITESSTableBuffer(
                    it.first, backing_mem_,
                    it.second, used_size_);
            WRITE_requests_.push_back(reqid);
        }

        if (!backing_mem_) {
            local_writable_file_->Sync();
        }
        return Status::OK();
    }

    void NovaCCMemFile::WaitForWRITEs() {
        if (WRITE_requests_.empty()) {
            return;
        }

        for (auto req : WRITE_requests_) {
            while (!cc_client_->IsDone(req, nullptr));
        }
    }


    NovaCCRandomAccessFile::NovaCCRandomAccessFile(
            Env *env, const std::string &dbname, uint64_t file_number,
            const leveldb::FileMetaData &meta, leveldb::CCClient *dc_client,
            leveldb::MemManager *mem_manager,
            SSTableManager *sstable_manager,
            uint64_t thread_id,
            bool prefetch_all) : env_(env), dbname_(dbname),
                                 file_number_(file_number),
                                 meta_(meta), dc_client_(dc_client),
                                 mem_manager_(mem_manager),
                                 sstable_manager_(sstable_manager),
                                 thread_id_(thread_id),
                                 prefetch_all_(prefetch_all) {
        RDMA_ASSERT(mem_manager_);
        RDMA_ASSERT(dc_client_);
        RDMA_ASSERT(sstable_manager);

        // Look up SSTable Manager. If file is present, read it from the buffer.
        //  Otherwise, read the file from linux. When it's done, decrement the ref count of the buffered SSTable.
        sstable_manager_->GetSSTable(dbname, file_number, &wb_table_);
        if (!wb_table_) {
            Status s = env_->NewRandomAccessFile(
                    TableFileName(dbname, file_number), &local_ra_file_);
            RDMA_ASSERT(s.ok())
                << fmt::format("Failed to read file {} db:{} fn:{}",
                               s.ToString(), dbname, file_number);
        }
    }

    bool NovaCCRandomAccessFile::IsWBTableDeleted() {
        if (!wb_table_) {
            return false;
        }
        return wb_table_->is_deleted();
    }

    NovaCCRandomAccessFile::~NovaCCRandomAccessFile() {
        if (wb_table_) {
            wb_table_->Unref();
        }
        if (local_ra_file_) {
            delete local_ra_file_;
        }

        if (backing_mem_table_) {
            uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                      meta_.file_size);
            mem_manager_->FreeItem(thread_id_, backing_mem_table_, scid);
            backing_mem_table_ = nullptr;
        }
        if (backing_mem_block_) {
            uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                      MAX_BLOCK_SIZE);
            mem_manager_->FreeItem(thread_id_, backing_mem_block_, scid);
            backing_mem_block_ = nullptr;
        }
    }

    Status NovaCCRandomAccessFile::Read(uint64_t offset, size_t n,
                                        leveldb::Slice *result,
                                        char *scratch) {
        RDMA_ASSERT(scratch);

        const uint64_t available =
                meta_.file_size - std::min(meta_.file_size, offset);
        if (n > available) {
            n = static_cast<size_t>(available);
        }
        if (n == 0) {
            *result = Slice();
            return Status::OK();
        }

        char *ptr = nullptr;
        if (wb_table_) {
            ptr = &wb_table_->backing_mem[offset];
            memcpy(scratch, ptr, n);
            *result = Slice(scratch, n);
            return Status::OK();
        }

        if (local_ra_file_) {
            return local_ra_file_->Read(offset, n, result, scratch);
        }

        RDMA_ASSERT(false) << "Should never happen";

        if (!prefetch_all_ && backing_mem_block_ == nullptr) {
            uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                      MAX_BLOCK_SIZE);
            backing_mem_block_ = mem_manager_->ItemAlloc(thread_id_, scid);
            RDMA_ASSERT(backing_mem_block_) << "Running out of memory";
        }


        if (prefetch_all_) {
            if (backing_mem_table_ == nullptr) {
                RDMA_ASSERT(ReadAll().ok());
            }
            ptr = &backing_mem_table_[offset];
        } else {
            CCBlockHandle handle = {
                    .offset = offset,
                    .size = n
            };
            ptr = backing_mem_block_;
            uint32_t req_id = dc_client_->InitiateReadBlock(dbname_,
                                                            file_number_, meta_,
                                                            handle,
                                                            ptr);
            while (!dc_client_->IsDone(req_id, nullptr)) {
                // Wait until the request is complete.
            }
        }
        memcpy(scratch, ptr, n);
        *result = Slice(scratch, n);
        return Status::OK();
    }

    Status NovaCCRandomAccessFile::ReadAll() {
        uint32_t scid = mem_manager_->slabclassid(thread_id_, meta_.file_size);
        backing_mem_table_ = mem_manager_->ItemAlloc(thread_id_, scid);
        RDMA_ASSERT(backing_mem_table_) << "Running out of memory";
        uint32_t req_id = dc_client_->InitiateReadSSTable(dbname_, file_number_,
                                                          meta_,
                                                          backing_mem_table_);
        while (!dc_client_->IsDone(req_id, nullptr)) {

        }
        return Status::OK();
    }

    NovaCCCompactionThread::NovaCCCompactionThread(rdmaio::RdmaCtrl *rdma_ctrl)
            : rdma_ctrl_(rdma_ctrl) {
        sem_init(&signal, 0, 0);
    }

    void NovaCCCompactionThread::Schedule(
            void (*background_work_function)(void *background_work_arg),
            void *background_work_arg) {
        background_work_mutex_.Lock();

        // If the queue is empty, the background thread may be waiting for work.
        background_work_queue_.emplace(background_work_function,
                                       background_work_arg);
        background_work_mutex_.Unlock();

        sem_post(&signal);
    }

    bool NovaCCCompactionThread::IsInitialized() {
        mutex_.Lock();
        bool is_running = is_running_;
        mutex_.Unlock();
        return is_running;
    }

    void NovaCCCompactionThread::AddDeleteSSTables(
            const std::vector<leveldb::DeleteTableRequest> &requests) {
        mutex_.Lock();
        for (auto &req : requests) {
            delete_table_requests_.push_back(req);
        }
        mutex_.Unlock();
        sem_post(&signal);
    }

    void NovaCCCompactionThread::DeleteMCSSTables() {
        mutex_.Lock();
        if (delete_table_requests_.empty()) {
            mutex_.Unlock();
            return;
        }

        std::map<std::string, std::vector<uint64_t >> dbfiles;
        for (auto &req : delete_table_requests_) {
            dbfiles[req.dbname].push_back(req.file_number);
        }
        delete_table_requests_.clear();
        mutex_.Unlock();

        for (auto &it : dbfiles) {
            uint32_t sid = 0;
            uint32_t index = 0;
            nova::ParseDBIndexFromDBName(it.first, &sid, &index);
            dc_client_->InitiateDeleteTables(it.first, it.second);
        }
    }

    void NovaCCCompactionThread::Start() {
        rdma_store_->Init(rdma_ctrl_);

        mutex_.Lock();
        is_running_ = true;
        mutex_.Unlock();

        std::cout << "BG thread started" << std::endl;

        bool should_sleep = true;
        uint32_t timeout = RDMA_POLL_MIN_TIMEOUT_US;
        while (is_running_) {
            if (should_sleep) {
                usleep(timeout);
            }
            int n = 0;
            n += rdma_store_->PollSQ();
            n += rdma_store_->PollRQ();

            DeleteMCSSTables();

            background_work_mutex_.Lock();
            n += background_work_queue_.size();
            if (background_work_queue_.empty()) {
                background_work_mutex_.Unlock();
            } else {
                auto background_work_function = background_work_queue_.front().function;
                void *background_work_arg = background_work_queue_.front().arg;
                background_work_queue_.pop();
                background_work_mutex_.Unlock();
                background_work_function(background_work_arg);
            }

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

}