
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
            mem_manager_(mem_manager),
            thread_id_(thread_id) {
        EnvFileMetadata meta;
        meta.level = 0;
        Status s = env_->NewWritableFile(rtable_name, meta, &file_);
        RDMA_ASSERT(s.ok()) << s.ToString();
        s = env_->NewRandomAccessFile(rtable_name_, &readable_file_);
        RDMA_ASSERT(s.ok());

        uint32_t scid = mem_manager_->slabclassid(thread_id,
                                                  rtable_size);
        backing_mem_ = mem_manager->ItemAlloc(thread_id, scid);
        file_size_ = 0;
        allocated_mem_size_ = rtable_size;
        RDMA_ASSERT(backing_mem_) << "Running out of memory";
    }

    Status NovaRTable::Read(uint64_t offset, uint32_t size, char *scratch) {
        Slice s;
        RTableHandle h;
        return readable_file_->Read(h, offset, size, &s, scratch);
    }

    void NovaRTable::MarkOffsetAsWritten(uint64_t offset) {
        mutex_.lock();
        uint64_t relative_off = offset - (uint64_t) (backing_mem_);
        for (auto it = allocated_bufs_.end();
             it != allocated_bufs_.begin(); it--) {
            if (it->offset == relative_off) {
                it->persisted = true;
                break;
            }
        }
        mutex_.unlock();
    }

    uint64_t NovaRTable::AllocateBuf(const std::string &sstable,
                                     uint32_t size) {
        mutex_.lock();
        if (current_mem_offset_ + size > allocated_mem_size_) {
            sealed_ = true;
            mutex_.unlock();
            return UINT64_MAX;
        }
        uint32_t off = current_mem_offset_;
        BlockHandle handle = {};
        handle.set_offset(off);
        handle.set_size(size);
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
            return;
        }

        // sequential IOs to disk.
        uint64_t disk_offset = current_disk_offset_;
        std::vector<BlockHandle> written_mem_blocks;
        for (auto buf = allocated_bufs_.begin();
             buf != allocated_bufs_.end(); buf++) {
            if (!buf->persisted) {
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
            allocated_bufs_.erase(buf);
        }
        mutex_.unlock();

        for (BlockHandle &mem_block_handle : written_mem_blocks) {
            Status s = file_->Append(
                    Slice(backing_mem_ + mem_block_handle.offset(),
                          mem_block_handle.size()));
            RDMA_ASSERT(s.ok());
        }
        file_->Flush();
        Seal();
    }

    void NovaRTable::DeleteSSTable(const std::string &dbname,
                                   uint64_t file_number) {
        std::string file_name = TableFileName(dbname, file_number);
        mutex_.lock();
        sstable_offset_.erase(file_name);
        if (sstable_offset_.empty()) {
            if (!deleted_) {
                deleted_ = true;
            } else {
                mutex_.unlock();
                return;
            }
        }
        mutex_.unlock();

        if (file_) {
            file_->Close();
            delete file_;
            file_ = nullptr;
        }

        if (readable_file_) {
            delete readable_file_;
            readable_file_ = nullptr;
        }
        Status s = env_->DeleteFile(file_name);
        RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
    }

    void NovaRTable::Seal() {
        mutex_.lock();
        if (!allocated_bufs_.empty() || !sealed_) {
            mutex_.unlock();
            return;
        }
        mutex_.unlock();

        if (file_) {
            file_->Close();
            delete file_;
            file_ = nullptr;
        }

        uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                  allocated_mem_size_);
        mem_manager_->FreeItem(thread_id_, backing_mem_, scid);
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
            const leveldb::RTableHandle &rtable_handle, char *scratch) {
        NovaRTable *rtable = rtables_[rtable_handle.rtable_id];
        rtable->Read(rtable_handle.offset, rtable_handle.size, scratch);
    }

    void NovaRTableManager::DeleteSSTable(const std::string &dbname,
                                          uint64_t file_number) {
        uint32_t server_id;
        uint32_t dbidx;
        nova::ParseDBIndexFromDBName(dbname, &server_id, &dbidx);

        DBSSTableRTableMapping *rtable_mapping = server_db_sstable_rtable_mapping_[server_id][dbidx];
        rtable_mapping->mutex_.lock();
        NovaRTable *rtable = rtable_mapping->fn_rtable[file_number];
        rtable_mapping->mutex_.unlock();
        rtable->DeleteSSTable(dbname, file_number);
    }

    void NovaRTableManager::DeleteSSTable(const std::string &dbname,
                                          const std::vector<uint64_t> &file_numbers) {
        uint32_t server_id;
        uint32_t dbidx;
        nova::ParseDBIndexFromDBName(dbname, &server_id, &dbidx);

        DBSSTableRTableMapping *rtable_mapping = server_db_sstable_rtable_mapping_[server_id][dbidx];
        std::vector<NovaRTable *> rtables(file_numbers.size());
        rtable_mapping->mutex_.lock();
        for (int i = 0; i < file_numbers.size(); i++) {
            rtables[i] = rtable_mapping->fn_rtable[file_numbers[i]];
        }
        rtable_mapping->mutex_.unlock();

        for (int i = 0; i < file_numbers.size(); i++) {
            rtables[i]->DeleteSSTable(dbname, file_numbers[i]);
        }
    }

    void NovaRTableManager::ReadDataBlocksOfSSTable(const std::string &dbname,
                                                    uint64_t file_number,
                                                    char *scratch) {
        uint32_t server_id;
        uint32_t dbidx;
        nova::ParseDBIndexFromDBName(dbname, &server_id, &dbidx);

        DBSSTableRTableMapping *rtable_mapping = server_db_sstable_rtable_mapping_[server_id][dbidx];
        rtable_mapping->mutex_.lock();
        NovaRTable *rtable = rtable_mapping->fn_rtable[file_number];
        rtable_mapping->mutex_.unlock();

        BlockHandle &handle = rtable->Handle(TableFileName(dbname,
                                                           file_number));
        rtable->Read(handle.offset(), handle.size(), scratch);
    }

    NovaRTable *NovaRTableManager::active_rtable(uint32_t thread_id) {
        return active_rtables_[thread_id];
    }

    NovaRTable *NovaRTableManager::CreateNewRTable(uint32_t thread_id) {
        mutex_.lock();
        uint32_t id = current_rtable_id_;
        current_rtable_id_ += 1;
        mutex_.unlock();
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
        return rtables_[rtable_id];
    }

    NovaRTableManager::NovaRTableManager(leveldb::Env *env,
                                         leveldb::MemManager *mem_manager,
                                         const std::string &rtable_path,
                                         uint32_t rtable_size,
                                         uint32_t nservers, uint32_t nranges) :
            env_(env), mem_manager_(mem_manager), rtable_path_(rtable_path),
            rtable_size_(rtable_size) {
        server_db_sstable_rtable_mapping_ = new DBSSTableRTableMapping **[nservers];
        for (int i = 0; i < nservers; i++) {
            server_db_sstable_rtable_mapping_[i] = new DBSSTableRTableMapping *[nranges];
            for (int j = 0; j < nranges; j++) {
                server_db_sstable_rtable_mapping_[i][j] = new DBSSTableRTableMapping;
            }
        }
    }
}