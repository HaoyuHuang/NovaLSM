
//
// Created by Haoyu Huang on 1/29/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//
#include <fmt/core.h>

#include "nova_rtable.h"

#include "db/filename.h"
#include "nova/logging.hpp"
#include "nova/nova_common.h"

namespace leveldb {

    void RTableHandle::EncodeHandle(char *buf) {
        EncodeFixed32(buf, server_id);
        EncodeFixed32(buf + 4, rtable_id);
        EncodeFixed64(buf + 8, offset);
        EncodeFixed32(buf + 16, size);
    }

    void RTableHandle::DecodeHandle(const char *buf) {
        server_id = DecodeFixed32(buf);
        rtable_id = DecodeFixed32(buf + 4);
        offset = DecodeFixed64(buf + 8);
        size = DecodeFixed32(buf + 16);
    }

    NovaRTable::NovaRTable(uint32_t rtable_id, leveldb::Env *env,
                           std::string rtable_name, MemManager *mem_manager,
                           uint32_t thread_id, uint32_t rtable_size) :
            rtable_id_(rtable_id), env_(env), rtable_name_(rtable_name),
            mem_manager_(mem_manager), thread_id_(thread_id) {
        EnvFileMetadata meta;
        meta.level = 0;
        Status s = env_->NewReadWriteFile(rtable_name, meta, &file_);
        RDMA_ASSERT(s.ok()) << s.ToString();

        uint32_t scid = mem_manager_->slabclassid(thread_id,
                                                  rtable_size);
        backing_mem_ = mem_manager->ItemAlloc(thread_id, scid);
        file_size_ = 0;
        allocated_mem_size_ = rtable_size;

        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format(
                    "Rtable {} created with t:{} file size {} allocated size {}",
                    rtable_id_, thread_id,
                    file_size_, allocated_mem_size_);

        RDMA_ASSERT(backing_mem_) << "Running out of memory";
    }

    Status NovaRTable::Read(uint64_t offset, uint32_t size, char *scratch) {
        Slice s;
        RTableHandle h = {};
        return file_->Read(h, offset, size, &s, scratch);
    }

    void NovaRTable::MarkOffsetAsWritten(uint64_t offset) {
        bool found = false;
        mutex_.lock();
        uint64_t relative_off = offset - (uint64_t) (backing_mem_);
        for (auto it = allocated_bufs_.rbegin();
             it != allocated_bufs_.rend(); it++) {
            if (it->offset == relative_off) {
                it->persisted = true;
                found = true;
                break;
            }
        }
        mutex_.unlock();
        RDMA_ASSERT(found);
    }

    uint64_t NovaRTable::AllocateBuf(const std::string &sstable,
                                     uint32_t size) {
        mutex_.lock();
        if (is_full_ || current_mem_offset_ + size > allocated_mem_size_) {
            is_full_ = true;
            mutex_.unlock();
            Seal();
            return UINT64_MAX;
        }
        RDMA_ASSERT(!sealed_);
        uint32_t off = current_mem_offset_;
        BlockHandle handle = {};
        handle.set_offset(off);
        handle.set_size(size);

        RDMA_ASSERT(sstable_offset_.find(sstable) == sstable_offset_.end());

        sstable_offset_[sstable] = handle;
        current_mem_offset_ += size;
        AllocatedBuf allocated_buf;
        allocated_buf.sstable_id = sstable;
        allocated_buf.offset = off;
        allocated_buf.size = size;
        allocated_buf.persisted = false;
        allocated_bufs_.push_back(allocated_buf);
        file_size_ += size;
        mutex_.unlock();

        return (uint64_t) (backing_mem_) + off;
    }

    void NovaRTable::Persist() {
        mutex_.lock();
        if (allocated_bufs_.empty()) {
            mutex_.unlock();
            Seal();
            return;
        }

        // sequential IOs to disk.
        uint64_t disk_offset = current_disk_offset_;
        std::vector<BlockHandle> written_mem_blocks;
        auto buf = allocated_bufs_.begin();
        while (buf != allocated_bufs_.end()) {
            if (!buf->persisted) {
                buf++;
                continue;
            }
            sstable_offset_[buf->sstable_id].set_offset(disk_offset);
            sstable_offset_[buf->sstable_id].set_size(buf->size);
            disk_offset += buf->size;
            if (!written_mem_blocks.empty()) {
                BlockHandle &prev_handle = written_mem_blocks[
                        written_mem_blocks.size() - 1];
                if (prev_handle.offset() + prev_handle.size() == buf->offset) {
                    prev_handle.set_size(prev_handle.size() + buf->size);
                } else {
                    BlockHandle bh;
                    bh.set_offset(buf->offset);
                    bh.set_size(buf->size);
                    written_mem_blocks.push_back(bh);
                }
            } else {
                BlockHandle bh;
                bh.set_offset(buf->offset);
                bh.set_size(buf->size);
                written_mem_blocks.push_back(bh);
            }
            buf = allocated_bufs_.erase(buf);
        }
        uint64_t base_disk_offset = current_disk_offset_;
        for (BlockHandle &mem_block_handle : written_mem_blocks) {
            current_disk_offset_ += mem_block_handle.size();
        }
        mutex_.unlock();

        for (BlockHandle &mem_block_handle : written_mem_blocks) {
            Status s = file_->Append(
                    Slice(backing_mem_ + mem_block_handle.offset(),
                          mem_block_handle.size()));
            RDMA_ASSERT(s.ok());
            s = file_->Sync();
            RDMA_ASSERT(s.ok());
            base_disk_offset += mem_block_handle.size();
        }

        // Append 16 KB at a time.
        // Sync at every 1 MB.
//        uint64_t append_size = 1024 * 64;
//        uint64_t sync_size = 2 * 1024 * 1024;
//        for (BlockHandle &mem_block_handle : written_mem_blocks) {
//            uint64_t off = mem_block_handle.offset();
//            uint64_t written_size = 0;
//            uint64_t synced_size = 0;
//
//            do {
//                uint64_t size = std::min(append_size, mem_block_handle.size() -
//                                                      written_size);
//                Status s = file_->Append(
//                        Slice(backing_mem_ + off,
//                              size));
//                RDMA_ASSERT(s.ok());
//                off += size;
//                written_size += size;
//
//                if (written_size - synced_size >= sync_size) {
//                    s = file_->Sync();
//                    RDMA_ASSERT(s.ok());
//                    synced_size = written_size;
//                }
//            } while (written_size < mem_block_handle.size());
//
//            if (written_size - synced_size > 0) {
//                Status s = file_->Sync();
//                RDMA_ASSERT(s.ok());
//            }
//
//            current_disk_offset_ += mem_block_handle.size();
//        }

        Seal();
    }

    void NovaRTable::DeleteSSTable(const std::string &sstable_id) {
        bool delete_rtable = false;

        mutex_.lock();
        sstable_offset_.erase(sstable_id);
        if (sstable_offset_.empty()) {
            if (!deleted_) {
                deleted_ = true;
                delete_rtable = true;
            }
        }

        RDMA_LOG(rdmaio::DEBUG) << fmt::format(
                    "Delete SSTable {} from RTable {}. Delete RTable: {}",
                    sstable_id, rtable_name_, delete_rtable);
        mutex_.unlock();

        if (!delete_rtable) {
            return;
        }

        is_full_ = true;
        allocated_bufs_.clear();
        Seal();

        if (file_) {
            file_->Close();
            delete file_;
            file_ = nullptr;
        }
        Status s = env_->DeleteFile(rtable_name_);
        RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
    }

    void NovaRTable::Seal() {
        bool seal = false;
        mutex_.lock();
        if (allocated_bufs_.empty() && is_full_) {
            if (!sealed_) {
                seal = true;
            }
        }

        if (!seal) {
            mutex_.unlock();
            return;
        }
        mutex_.unlock();

        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format(
                    "Rtable {} closed with t:{} file size {} allocated size {}",
                    rtable_id_, thread_id_,
                    file_size_, allocated_mem_size_);

        uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                  allocated_mem_size_);
        mem_manager_->FreeItem(thread_id_, backing_mem_, scid);
        sealed_ = true;
        backing_mem_ = nullptr;
    }

    BlockHandle &NovaRTable::Handle(const std::string &sstable_id) {
        mutex_.lock();
        auto it = sstable_offset_.find(sstable_id);
        RDMA_ASSERT(it != sstable_offset_.end());
        BlockHandle &handle = it->second;
        mutex_.unlock();
        return handle;
    }

    void NovaRTableManager::ReadDataBlock(
            const leveldb::RTableHandle &rtable_handle, uint64_t offset,
            uint32_t size, char *scratch) {
        NovaRTable *rtable = rtables_[rtable_handle.rtable_id];
        rtable->Read(offset, size, scratch);
    }

    NovaRTable *NovaRTableManager::active_rtable(uint32_t thread_id) {
        NovaRTable *rtable = active_rtables_[thread_id];
        RDMA_ASSERT(rtable)
            << fmt::format("Active RTable of thread {} is null.", thread_id);
        return rtable;
    }

    NovaRTable *NovaRTableManager::CreateNewRTable(uint32_t thread_id) {
        mutex_.lock();
        uint32_t id = current_rtable_id_;
        current_rtable_id_ += 1;
        mutex_.unlock();

        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format("Create a new RTable {} for thread {}", id,
                           thread_id);
        RDMA_ASSERT(id < MAX_NUM_RTABLES) << fmt::format("Too many RTables");
        NovaRTable *rtable = new NovaRTable(id, env_,
                                            fmt::format("{}/rtable-{}",
                                                        rtable_path_, id),
                                            mem_manager_,
                                            thread_id, rtable_size_);
        rtables_[id] = rtable;
        active_rtables_[thread_id] = rtable;
        return rtable;
    }

    NovaRTable *NovaRTableManager::rtable(int rtable_id) {
        NovaRTable *rtable = rtables_[rtable_id];
        RDMA_ASSERT(rtable) << fmt::format("RTable {} is null.", rtable_id);
        return rtable;
    }

    NovaRTableManager::NovaRTableManager(leveldb::Env *env,
                                         leveldb::MemManager *mem_manager,
                                         const std::string &rtable_path,
                                         uint32_t rtable_size,
                                         uint32_t nservers, uint32_t nranges) :
            env_(env), mem_manager_(mem_manager), rtable_path_(rtable_path),
            rtable_size_(rtable_size) {
    }
}