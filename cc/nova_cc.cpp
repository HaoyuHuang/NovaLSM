
//
// Created by Haoyu Huang on 1/11/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_cc.h"

namespace leveldb {
    NovaCCRemoteMemFile::NovaCCRemoteMemFile(MemManager *mem_manager,
                                             DCClient *dc_client,
                                             const std::string &dbname,
                                             char *backing_mem,
                                             uint64_t allocated_size)
            : mem_manager_(mem_manager), dc_client_(dc_client),
              dbname_(dbname),
              backing_mem_(backing_mem), allocated_size_(allocated_size),
              MemFile(nullptr, "", false) {
    }

    NovaCCRemoteMemFile::~NovaCCRemoteMemFile() {
        uint32_t scid = mem_manager_->slabclassid(allocated_size_);
        mem_manager_->FreeItem(backing_mem_, scid);
    }

    Status
    NovaCCRemoteMemFile::Read(uint64_t offset, size_t n, leveldb::Slice *result,
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

    Status NovaCCRemoteMemFile::Append(const leveldb::Slice &data) {
        assert(used_size_ + data.size() < allocated_size_);
        memcpy(backing_mem_ + used_size_, data.data(), data.size());
        used_size_ += data.size();
        return Status::OK();
    }

    Status
    NovaCCRemoteMemFile::Write(uint64_t offset, const leveldb::Slice &data) {
        assert(offset + data.size() < allocated_size_);
        memcpy(backing_mem_ + offset, data.data(), data.size());
        if (offset + data.size() > used_size_) {
            used_size_ = offset + data.size();
        }
        return Status::OK();
    }

    Status NovaCCRemoteMemFile::Fsync() {
        // NOOP.
        uint32_t req_id = dc_client_->InitiateFlushSSTable(dbname_,
                                                           meta_.number, meta_,
                                                           backing_mem_);
        while (!dc_client_->IsDone(req_id)) {
            //
        }
    }


    NovaCCRemoteRandomAccessFile::NovaCCRemoteRandomAccessFile(
            const std::string &dbname, uint64_t file_number,
            const leveldb::FileMetaData &meta, leveldb::DCClient *dc_client,
            leveldb::MemManager *mem_manager, bool cache_all) : dbname_(
            dbname), file_number_(file_number), meta_(meta), dc_client_(
            dc_client), mem_manager_(mem_manager), cache_all_(cache_all) {

    }

    NovaCCRemoteRandomAccessFile::~NovaCCRemoteRandomAccessFile() {
        if (backing_mem_table_) {
            uint32_t scid = mem_manager_->slabclassid(meta_.file_size);
            mem_manager_->FreeItem(backing_mem_table_, scid);
        }
        if (backing_mem_block_) {
            uint32_t scid = mem_manager_->slabclassid(10240);
            mem_manager_->FreeItem(backing_mem_block_, scid);
        }
    }

    Status NovaCCRemoteRandomAccessFile::Read(uint64_t offset, size_t n,
                                              leveldb::Slice *result,
                                              char *scratch) {
        RDMA_ASSERT(scratch);
        if (!cache_all_ && backing_mem_block_ == nullptr) {
            uint32_t scid = mem_manager_->slabclassid(10240);
            backing_mem_block_ = mem_manager_->ItemAlloc(scid);
        }

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
        if (cache_all_) {
            if (!done_read_all_) {
                RDMA_ASSERT(ReadAll().ok());
            }
            ptr = &backing_mem_table_[offset];
        } else {
            DCBlockHandle handle = {
                    .offset = offset,
                    .size = n
            };
            ptr = backing_mem_block_;
            uint32_t req_id = dc_client_->InitiateReadBlock(dbname_,
                                                            file_number_, meta_,
                                                            handle,
                                                            ptr);
            while (!dc_client_->IsDone(req_id)) {
                // Wait until the request is complete.
            }
        }


        if (scratch) {
            memcpy(scratch, ptr, n);
            *result = Slice(scratch, n);
        } else {
            *result = Slice(ptr, n);
        }
    }

    Status NovaCCRemoteRandomAccessFile::ReadAll() {
        uint32_t scid = mem_manager_->slabclassid(meta_.file_size);
        backing_mem_table_ = mem_manager_->ItemAlloc(scid);
        uint32_t req_id = dc_client_->InitiateReadSSTable(dbname_, file_number_,
                                                          meta_,
                                                          backing_mem_table_);
        while (!dc_client_->IsDone(req_id)) {

        }

    }

    PosixEnvBGThread::PosixEnvBGThread() : background_work_cv_(
            &background_work_mutex_) {
    }

    void PosixEnvBGThread::Schedule(
            void (*background_work_function)(void *background_work_arg),
            void *background_work_arg) {
        background_work_mutex_.Lock();

        // If the queue is empty, the background thread may be waiting for work.
        if (background_work_queue_.empty()) {
            background_work_cv_.Signal();
        }

        background_work_queue_.emplace(background_work_function,
                                       background_work_arg);
        background_work_mutex_.Unlock();
    }

    void PosixEnvBGThread::Start() {
        std::cout << "BG thread started" << std::endl;
        while (true) {
            background_work_mutex_.Lock();

            // Wait until there is work to be done.
            while (background_work_queue_.empty()) {
                background_work_cv_.Wait();
            }

            assert(!background_work_queue_.empty());
            auto background_work_function = background_work_queue_.front().function;
            void *background_work_arg = background_work_queue_.front().arg;
            background_work_queue_.pop();

            background_work_mutex_.Unlock();
            background_work_function(background_work_arg);
        }
    }

}