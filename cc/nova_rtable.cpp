
//
// Created by Haoyu Huang on 1/29/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//
#include <fmt/core.h>
#include "leveldb/cache.h"
#include "db/filename.h"

#include "nova_rtable.h"
#include "nova/logging.hpp"
#include "nova/nova_common.h"
#include "nova/nova_config.h"

namespace leveldb {

    std::string RTableHandle::DebugString() const {
        return fmt::format("[{} {} {} {}]", server_id, rtable_id, offset, size);
    }

    void RTableHandle::EncodeHandle(char *buf) const {
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

    bool RTableHandle::DecodeHandle(leveldb::Slice *data,
                                    leveldb::RTableHandle *handle) {
        if (data->size() < HandleSize()) {
            return false;
        }

        handle->DecodeHandle(data->data());
        *data = Slice(data->data() + HandleSize(), data->size() - HandleSize());
        return true;
    }

    bool RTableHandle::DecodeHandles(leveldb::Slice *data,
                                     std::vector<leveldb::RTableHandle> *handles) {
        uint32_t size = 0;
        if (!DecodeFixed32(data, &size)) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            RTableHandle handle = {};
            if (!DecodeHandle(data, &handle)) {
                return false;
            }
            handles->push_back(handle);
        }
        return true;
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

    Status NovaRTable::Read(uint64_t offset, uint32_t size, char *scratch,
                            Slice *result) {
        RTableHandle h = {};
        nova::dc_stats.dc_queue_depth += 1;
        nova::dc_stats.dc_pending_disk_reads += size;
        Status status = file_->Read(h, offset, size, result, scratch);
        nova::dc_stats.dc_queue_depth -= 1;
        nova::dc_stats.dc_pending_disk_reads -= size;
        return status;
    }

    bool NovaRTable::MarkOffsetAsWritten(uint32_t given_rtable_id_for_assertion,
                                         uint64_t offset) {
        RDMA_ASSERT(given_rtable_id_for_assertion == rtable_id_)
            << fmt::format("{} {}", given_rtable_id_for_assertion, rtable_id_);
        bool found = false;
        mutex_.lock();
        uint64_t relative_off = offset - (uint64_t)(backing_mem_);
        for (auto it = allocated_bufs_.rbegin();
             it != allocated_bufs_.rend(); it++) {
            if (it->offset == relative_off) {
                it->written_to_mem = true;
                found = true;
                break;
            }
        }
        mutex_.unlock();
        if (!found) {
            RDMA_LOG(rdmaio::INFO)
                << fmt::format("rtablename:{} id:{}", rtable_name_, rtable_id_);
        }
        return found;
    }

    uint64_t NovaRTable::AllocateBuf(const std::string &sstable,
                                     uint32_t size, bool is_meta_blocks) {
        RDMA_ASSERT(current_mem_offset_ + size <= allocated_mem_size_)
            << "exceed maximum rtable size "
            << size << ","
            << allocated_mem_size_;
        leveldb::FileType type = leveldb::FileType::kCurrentFile;
        RDMA_ASSERT(ParseFileName(sstable, &type));
        mutex_.lock();
        if (is_full_ || current_mem_offset_ + size > allocated_mem_size_) {
            Seal();
            mutex_.unlock();
            return UINT64_MAX;
        }
        RDMA_ASSERT(!sealed_);
        uint32_t off = current_mem_offset_;
        BlockHandle handle = {};
        handle.set_offset(off);
        handle.set_size(size);

        if (is_meta_blocks) {
            RDMA_ASSERT(sstable_meta_block_offset_.find(sstable) ==
                        sstable_meta_block_offset_.end());
        } else if (type == leveldb::FileType::kTableFile) {
            RDMA_ASSERT(sstable_data_block_offset_.find(sstable) ==
                        sstable_data_block_offset_.end());
        }

        current_mem_offset_ += size;
        AllocatedBuf allocated_buf = {};
        allocated_buf.sstable_id = sstable;
        allocated_buf.offset = off;
        allocated_buf.size = size;
        allocated_buf.written_to_mem = false;
        allocated_buf.is_meta_blocks = is_meta_blocks;
        allocated_bufs_.push_back(allocated_buf);
        file_size_ += size;
        mutex_.unlock();
        return (uint64_t)(backing_mem_) + off;
    }

    uint64_t NovaRTable::Persist(uint32_t given_rtable_id_for_assertion) {
        RDMA_ASSERT(given_rtable_id_for_assertion == rtable_id_)
            << fmt::format("{} {}", given_rtable_id_for_assertion, rtable_id_);

        uint64_t persisted_bytes = 0;
        mutex_.lock();
        if (allocated_bufs_.empty()) {
            Seal();
            mutex_.unlock();
            return persisted_bytes;
        }

        // sequential IOs to disk.
        auto buf = allocated_bufs_.begin();
        while (buf != allocated_bufs_.end()) {
            if (!buf->written_to_mem) {
                buf++;
                continue;
            }

            leveldb::FileType type = leveldb::FileType::kCurrentFile;
            RDMA_ASSERT(leveldb::ParseFileName(buf->sstable_id, &type));

            if (buf->is_meta_blocks) {
                RDMA_ASSERT(
                        sstable_meta_block_offset_.find(buf->sstable_id) ==
                        sstable_meta_block_offset_.end());
                SSTablePersistStatus &s = sstable_meta_block_offset_[buf->sstable_id];
                s.disk_handle.set_offset(current_disk_offset_);
                s.disk_handle.set_size(buf->size);
                s.persisted = false;
            } else {
                if (type == leveldb::FileType::kTableFile) {
                    RDMA_ASSERT(
                            sstable_data_block_offset_.find(buf->sstable_id) ==
                            sstable_data_block_offset_.end());
                }
                SSTablePersistStatus &s = sstable_data_block_offset_[buf->sstable_id];
                s.disk_handle.set_offset(current_disk_offset_);
                s.disk_handle.set_size(buf->size);
                s.persisted = false;
            }

            BlockHandle mem_handle = {};
            mem_handle.set_offset(buf->offset);
            mem_handle.set_size(buf->size);
//            diskoff_memoff_[current_disk_offset_] = mem_handle;

            persisting_cnt += 1;
            current_disk_offset_ += buf->size;

            BatchWrite bw = {};
            bw.mem_handle.set_offset(buf->offset);
            bw.mem_handle.set_size(buf->size);
            bw.sstable = buf->sstable_id;
            bw.is_meta_blocks = buf->is_meta_blocks;
            written_mem_blocks_.push_back(bw);
            buf = allocated_bufs_.erase(buf);
        }

        RDMA_ASSERT(current_disk_offset_ <= file_size_);
        mutex_.unlock();

        persist_mutex_.lock();
        // Make a copy of written_mem_blocks.
        mutex_.lock();
        std::vector<BatchWrite> writes;
        for (int i = 0; i < written_mem_blocks_.size(); i++) {
            writes.push_back(written_mem_blocks_[i]);
        }
        if (writes.empty()) {
            Seal();
        }
        mutex_.unlock();

        if (writes.empty()) {
            persist_mutex_.unlock();
            return persisted_bytes;
        }


        int i = 1;
        int persisted_i = 0;
        uint64_t offset = writes[0].mem_handle.offset();
        uint64_t size = writes[0].mem_handle.size();
        while (i < writes.size()) {
            if (offset + size == writes[i].mem_handle.offset()) {
                size += writes[i].mem_handle.size();
                i++;
                continue;
            }

            // persist offset -> size.
            nova::dc_stats.dc_queue_depth += 1;
            nova::dc_stats.dc_pending_disk_writes += size;
            persisted_bytes += size;

            Status s = file_->Append(Slice(backing_mem_ + offset, size));
            RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
            s = file_->Sync();
            RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());

            nova::dc_stats.dc_queue_depth -= 1;
            nova::dc_stats.dc_pending_disk_writes -= size;

            mutex_.lock();
            for (int j = persisted_i; j < i; j++) {
                if (writes[j].is_meta_blocks) {
                    RDMA_ASSERT(
                            sstable_meta_block_offset_.find(
                                    writes[j].sstable) !=
                            sstable_meta_block_offset_.end());
                    sstable_meta_block_offset_[writes[j].sstable].persisted = true;
                } else {
                    RDMA_ASSERT(
                            sstable_data_block_offset_.find(
                                    writes[j].sstable) !=
                            sstable_data_block_offset_.end());
                    sstable_data_block_offset_[writes[j].sstable].persisted = true;
                }
                persisting_cnt -= 1;
            }
            mutex_.unlock();
            persisted_i = i;
            offset = writes[i].mem_handle.offset();
            size = writes[i].mem_handle.size();
            i += 1;
        }
        // Persist the last range.
        nova::dc_stats.dc_queue_depth += 1;
        nova::dc_stats.dc_pending_disk_writes += size;
        persisted_bytes += size;

        Status s = file_->Append(Slice(backing_mem_ + offset, size));
        RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        s = file_->Sync();
        RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());

        nova::dc_stats.dc_queue_depth -= 1;
        nova::dc_stats.dc_pending_disk_writes -= size;

        mutex_.lock();
        for (int j = persisted_i; j < writes.size(); j++) {
            if (writes[j].is_meta_blocks) {
                RDMA_ASSERT(
                        sstable_meta_block_offset_.find(writes[j].sstable) !=
                        sstable_meta_block_offset_.end());
                sstable_meta_block_offset_[writes[j].sstable].persisted = true;
            } else {
                RDMA_ASSERT(
                        sstable_data_block_offset_.find(writes[j].sstable) !=
                        sstable_data_block_offset_.end());
                sstable_data_block_offset_[writes[j].sstable].persisted = true;
            }
            persisting_cnt -= 1;
        }
        mutex_.unlock();

        mutex_.lock();
        written_mem_blocks_.erase(written_mem_blocks_.begin(),
                                  written_mem_blocks_.begin() + writes.size());
        Seal();
        mutex_.unlock();
        persist_mutex_.unlock();
        return persisted_bytes;
    }

    bool NovaRTable::DeleteSSTable(uint32_t given_rtable_id_for_assertion,
                                   const std::string &sstable_id) {
        RDMA_ASSERT(given_rtable_id_for_assertion == rtable_id_)
            << fmt::format("{} {}", given_rtable_id_for_assertion, rtable_id_);
        bool delete_rtable = false;

        mutex_.lock();
        Seal();
        {
            auto it = sstable_data_block_offset_.find(sstable_id);
            if (it != sstable_data_block_offset_.end()) {
                RDMA_ASSERT(it->second.persisted);
                int n = sstable_data_block_offset_.erase(sstable_id);
                RDMA_ASSERT(n == 1);
            }
        }
        {
            auto it = sstable_meta_block_offset_.find(sstable_id);
            if (it != sstable_meta_block_offset_.end()) {
                RDMA_ASSERT(it->second.persisted);
                int n = sstable_meta_block_offset_.erase(sstable_id);
                RDMA_ASSERT(n == 1);
            }
        }
        if (sstable_data_block_offset_.empty() &&
            sstable_meta_block_offset_.empty() && is_full_ &&
            allocated_bufs_.empty() &&
            persisting_cnt == 0 && sealed_) {
            if (!deleted_) {
                deleted_ = true;
                delete_rtable = true;
            }
        }

        RDMA_LOG(rdmaio::DEBUG) << fmt::format(
                    "Delete SSTable {} from RTable {}. Delete RTable: {}",
                    sstable_id, rtable_name_, delete_rtable);
        if (delete_rtable) {
            RDMA_ASSERT(current_disk_offset_ == file_size_);
        }
        mutex_.unlock();

        if (!delete_rtable) {
            return false;
        }

        RDMA_ASSERT(file_);

        Status s = file_->Close();
        RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        delete file_;
        file_ = nullptr;
        s = env_->DeleteFile(rtable_name_);
        RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        return true;
    }

    void NovaRTable::Close() {
        mutex_.lock();
        RDMA_ASSERT(allocated_bufs_.empty());
        RDMA_ASSERT(persisting_cnt == 0);
        is_full_ = true;
        Seal();

        RDMA_ASSERT(file_);
        Status s = file_->Close();
        RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        delete file_;
        file_ = nullptr;
        mutex_.unlock();
    }

    void NovaRTable::ForceSeal() {
        mutex_.lock();
        RDMA_ASSERT(allocated_bufs_.empty());
        RDMA_ASSERT(persisting_cnt == 0);
        is_full_ = true;
        Seal();
        mutex_.unlock();
    }

    void NovaRTable::Seal() {
        bool seal = false;
        if (allocated_bufs_.empty() && is_full_ && persisting_cnt == 0) {
            if (!sealed_) {
                seal = true;
                sealed_ = true;
            }
        }

        if (seal) {
            RDMA_ASSERT(current_disk_offset_ == file_size_);
        }

        if (!seal) {
            return;
        }

        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format(
                    "Rtable {} closed with t:{} file size {} allocated size {}",
                    rtable_id_, thread_id_,
                    file_size_, allocated_mem_size_);
        RDMA_ASSERT(backing_mem_);
        uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                  allocated_mem_size_);
        mem_manager_->FreeItem(thread_id_, backing_mem_, scid);
        backing_mem_ = nullptr;
    }

    BlockHandle
    NovaRTable::Handle(const std::string &sstable_id, bool is_meta_blocks) {
        BlockHandle handle = {};
        while (true) {
            mutex_.lock();
            if (is_meta_blocks) {
                auto it = sstable_meta_block_offset_.find(sstable_id);
                RDMA_ASSERT(it != sstable_meta_block_offset_.end());
                SSTablePersistStatus &s = it->second;
                if (s.persisted) {
                    handle = s.disk_handle;
                    mutex_.unlock();
                    break;
                }
            } else {
                auto it = sstable_data_block_offset_.find(sstable_id);
                RDMA_ASSERT(it != sstable_data_block_offset_.end());
                SSTablePersistStatus &s = it->second;
                if (s.persisted) {
                    handle = s.disk_handle;
                    mutex_.unlock();
                    break;
                }
            }
            mutex_.unlock();
        }

        return handle;
    }

    static void DeleteCachedBlock(const Slice &key, void *value) {
        char *block = reinterpret_cast<char *>(value);
        delete block;
    }

    void NovaRTableManager::ReadDataBlock(
            const leveldb::RTableHandle &rtable_handle, uint64_t offset,
            uint32_t size, char *scratch, Slice *result) {
        NovaRTable *rtable = FindRTable(rtable_handle.rtable_id);
        if (!block_cache_) {
            leveldb::FileType type;
            RDMA_ASSERT(ParseFileName(rtable->rtable_name_, &type));
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("Read {} from rtable {} offset:{} size:{}",
                               rtable_handle.DebugString(),
                               rtable->rtable_id(), offset, size);
            RDMA_ASSERT(rtable->Read(offset, size, scratch, result).ok());
            if (type == leveldb::FileType::kTableFile) {
                RDMA_ASSERT(result->size() == size)
                    << fmt::format("fn:{} given size:{} read size:{}",
                                   rtable->rtable_name_,
                                   size,
                                   result->size());
                RDMA_ASSERT(scratch[size - 1] != 0)
                    << fmt::format("Read {} from rtable {} offset:{} size:{}",
                                   rtable_handle.DebugString(),
                                   rtable->rtable_id(), offset, size);
            }
            return;
        }

        char cache_key_buffer[RTableHandle::HandleSize()];
        rtable_handle.EncodeHandle(cache_key_buffer);
        Slice key(cache_key_buffer, sizeof(cache_key_buffer));
        auto cache_handle = block_cache_->Lookup(key);
        if (cache_handle != nullptr) {
            auto block = reinterpret_cast<char *>(block_cache_->Value(
                    cache_handle));
            memcpy(scratch, block, rtable_handle.size);
        } else {
            rtable->Read(offset, size, scratch, result);
            char *block = new char[size];
            memcpy(block, scratch, size);
            cache_handle = block_cache_->Insert(key, block,
                                                size,
                                                &DeleteCachedBlock);
        }
        block_cache_->Release(cache_handle);
    }

//    NovaRTable *NovaRTableManager::active_rtable(uint32_t thread_id) {
//        mutex_.lock();
//        NovaRTable *rtable = active_rtables_[thread_id];
//        RDMA_ASSERT(rtable)
//            << fmt::format("Active RTable of thread {} is null.",
//                           thread_id);
//        mutex_.unlock();
//        return rtable;
//    }
//
//    NovaRTable *NovaRTableManager::CreateNewRTable(uint32_t thread_id) {
//        mutex_.lock();
//        uint32_t id = current_rtable_id_;
//        current_rtable_id_ += 1;
//        mutex_.unlock();
//
//        RDMA_LOG(rdmaio::DEBUG)
//            << fmt::format("Create a new RTable {} for thread {}", id,
//                           thread_id);
//        RDMA_ASSERT(id < MAX_NUM_RTABLES)
//            << fmt::format("Too many RTables");
//        NovaRTable *rtable = new NovaRTable(id, env_,
//                                            fmt::format("{}/rtable-{}",
//                                                        rtable_path_, id),
//                                            mem_manager_,
//                                            thread_id, rtable_size_);
//        mutex_.lock();
//        RDMA_ASSERT(rtables_[id] == nullptr);
//        rtables_[id] = rtable;
////        active_rtables_[thread_id] = rtable;
//        mutex_.unlock();
//        return rtable;
//    }

    void NovaRTableManager::OpenRTables(
            std::unordered_map<std::string, uint32_t> &fn_rtables) {
        mutex_.lock();
        for (auto &it : fn_rtables) {
            auto &fn = it.first;
            auto &rtableid = it.second;
            RDMA_LOG(rdmaio::INFO)
                << fmt::format("Open RTable {} for file {}", rtableid, fn);

            NovaRTable *rtable = new NovaRTable(rtableid, env_,
                                                fn,
                                                mem_manager_,
                                                0, rtable_size_);
            rtable->ForceSeal();
            RDMA_ASSERT(rtables_[rtableid] == nullptr);
            rtables_[rtableid] = rtable;
            fn_rtable_map_[fn] = rtable;
            current_rtable_id_ = std::max(current_rtable_id_, rtableid);
        }

        current_rtable_id_ += 1;
        mutex_.unlock();
    }

    NovaRTable *NovaRTableManager::OpenRTable(uint32_t thread_id,
                                              std::string &filename) {
        mutex_.lock();
        auto rtable_ptr = fn_rtable_map_.find(filename);
        if (rtable_ptr != fn_rtable_map_.end()) {
            auto rtable = rtable_ptr->second;
            mutex_.unlock();
            return rtable;
        }
        // not found.
        uint32_t id = current_rtable_id_;
        current_rtable_id_ += 1;
        mutex_.unlock();

        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format("Create a new RTable {} for thread {} fn:{}", id,
                           thread_id, filename);
        RDMA_ASSERT(id < MAX_NUM_RTABLES)
            << fmt::format("Too many RTables");
        NovaRTable *rtable = new NovaRTable(id, env_,
                                            filename,
                                            mem_manager_,
                                            thread_id, rtable_size_);
        mutex_.lock();
        RDMA_ASSERT(rtables_[id] == nullptr);
        rtables_[id] = rtable;
        fn_rtable_map_[filename] = rtable;
        mutex_.unlock();
        return rtable;
    }

    void NovaRTableManager::DeleteSSTable(const std::string &sstable_id) {
        mutex_.lock();
        auto it = fn_rtable_map_.find(sstable_id);
        NovaRTable *rtable = nullptr;
        if (it != fn_rtable_map_.end()) {
            rtable = it->second;
            fn_rtable_map_.erase(sstable_id);
        }
        mutex_.unlock();
        if (rtable) {
            rtable->DeleteSSTable(rtable->rtable_id(), sstable_id);
        }
    }

    NovaRTable *NovaRTableManager::FindRTable(uint32_t rtable_id) {
//        mutex_.lock();
        RDMA_ASSERT(rtable_id < MAX_NUM_RTABLES);
        NovaRTable *rtable = rtables_[rtable_id];
        RDMA_ASSERT(rtable) << fmt::format("RTable {} is null.", rtable_id);
        RDMA_ASSERT(rtable->rtable_id() == rtable_id)
            << fmt::format("RTable {} {}.", rtable->rtable_id(), rtable_id);
//        mutex_.unlock();
        return rtable;
    }

    NovaRTableManager::NovaRTableManager(leveldb::Env *env,
                                         leveldb::MemManager *mem_manager,
                                         const std::string &rtable_path,
                                         uint32_t rtable_size,
                                         uint32_t nservers,
                                         uint32_t server_id,
                                         uint32_t nranges) :
            env_(env), mem_manager_(mem_manager), rtable_path_(rtable_path),
            rtable_size_(rtable_size) {
        for (int i = 0; i < MAX_NUM_RTABLES; i++) {
            rtables_[i] = nullptr;
        }
//        for (int i = 0; i < 64; i++) {
//            active_rtables_[i] = nullptr;
//        }
    }
}