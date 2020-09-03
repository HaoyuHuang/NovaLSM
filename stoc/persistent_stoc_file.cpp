
//
// Created by Haoyu Huang on 1/29/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//
#include <fmt/core.h>
#include "leveldb/cache.h"
#include "db/filename.h"

#include "persistent_stoc_file.h"
#include "common/nova_console_logging.h"
#include "common/nova_common.h"
#include "common/nova_config.h"

namespace leveldb {

    std::string StoCBlockHandle::DebugString() const {
        return fmt::format("[{} {} {} {}]", server_id, stoc_file_id, offset,
                           size);
    }

    void StoCBlockHandle::EncodeHandle(char *buf) const {
        EncodeFixed32(buf, server_id);
        EncodeFixed32(buf + 4, stoc_file_id);
        EncodeFixed64(buf + 8, offset);
        EncodeFixed32(buf + 16, size);
    }

    void StoCBlockHandle::DecodeHandle(const char *buf) {
        server_id = DecodeFixed32(buf);
        stoc_file_id = DecodeFixed32(buf + 4);
        offset = DecodeFixed64(buf + 8);
        size = DecodeFixed32(buf + 16);
    }

    bool StoCBlockHandle::DecodeHandle(leveldb::Slice *data,
                                       leveldb::StoCBlockHandle *handle) {
        if (data->size() < HandleSize()) {
            return false;
        }

        handle->DecodeHandle(data->data());
        *data = Slice(data->data() + HandleSize(), data->size() - HandleSize());
        return true;
    }

    bool StoCBlockHandle::DecodeHandles(leveldb::Slice *data,
                                        std::vector<leveldb::StoCBlockHandle> *handles) {
        uint32_t size = 0;
        if (!DecodeFixed32(data, &size)) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            StoCBlockHandle handle = {};
            if (!DecodeHandle(data, &handle)) {
                return false;
            }
            handles->push_back(handle);
        }
        return true;
    }

    StoCPersistentFile::StoCPersistentFile(uint32_t file_id,
                                           leveldb::Env *env,
                                           std::string filename,
                                           MemManager *mem_manager,
                                           uint32_t thread_id,
                                           uint32_t file_size) :
            file_id_(file_id), env_(env), stoc_file_name_(filename),
            mem_manager_(mem_manager), thread_id_(thread_id) {
        EnvFileMetadata meta;
        meta.level = 0;
        Status s = env_->NewReadWriteFile(filename, meta, &file_);
        NOVA_ASSERT(s.ok()) << s.ToString();

        uint32_t scid = mem_manager_->slabclassid(thread_id,
                                                  file_size);
        backing_mem_ = mem_manager->ItemAlloc(thread_id, scid);
        file_size_ = 0;
        allocated_mem_size_ = file_size;

        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format(
                    "StoC file {} created with t:{} file size {} allocated size {}",
                    file_id_, thread_id,
                    file_size_, allocated_mem_size_);

        NOVA_ASSERT(backing_mem_) << "Running out of memory";
    }

    Status
    StoCPersistentFile::ReadForReplication(uint64_t offset, uint32_t size,
                                           char *scratch, Slice *result) {
        mutex_.lock();
        if (deleted_) {
            mutex_.unlock();
            return Status::NotFound("");
        }
        reading_cnt++;
        mutex_.unlock();

        StoCBlockHandle h = {};
        nova::NovaGlobalVariables::global.stoc_queue_depth += 1;
        nova::NovaGlobalVariables::global.stoc_pending_disk_reads += size;
        nova::NovaGlobalVariables::global.total_disk_reads += size;
        Status status = file_->Read(h, offset, size, result, scratch);
        nova::NovaGlobalVariables::global.stoc_queue_depth -= 1;
        nova::NovaGlobalVariables::global.stoc_pending_disk_reads -= size;

        mutex_.lock();
        reading_cnt--;
        if (reading_cnt == 0 && waiting_to_be_deleted && !deleted_) {
            waiting_to_be_deleted = false;
            deleted_ = true;
            NOVA_LOG(rdmaio::DEBUG) << fmt::format(
                        "Delete  Stoc File {}.", stoc_file_name_);
            NOVA_ASSERT(file_);
            Status s = file_->Close();
            NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
            delete file_;
            file_ = nullptr;
            s = env_->DeleteFile(stoc_file_name_);
            NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        }
        if (deleted_) {
            mutex_.unlock();
            return Status::NotFound("");
        }
        mutex_.unlock();
        return status;
    }

    Status
    StoCPersistentFile::Read(uint64_t offset, uint32_t size, char *scratch,
                             Slice *result) {
        StoCBlockHandle h = {};
        nova::NovaGlobalVariables::global.stoc_queue_depth += 1;
        nova::NovaGlobalVariables::global.stoc_pending_disk_reads += size;
        nova::NovaGlobalVariables::global.total_disk_reads += size;
        Status status = file_->Read(h, offset, size, result, scratch);
        nova::NovaGlobalVariables::global.stoc_queue_depth -= 1;
        nova::NovaGlobalVariables::global.stoc_pending_disk_reads -= size;
        return status;
    }

    bool StoCPersistentFile::MarkOffsetAsWritten(
            uint32_t given_file_id_for_assertion,
            uint64_t offset) {
        NOVA_ASSERT(given_file_id_for_assertion == file_id_)
            << fmt::format("{} {}", given_file_id_for_assertion, file_id_);
        bool found = false;
        mutex_.lock();
        uint64_t relative_off = offset - (uint64_t) (backing_mem_);
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
            NOVA_LOG(rdmaio::INFO)
                << fmt::format("stocfile:{} id:{}", stoc_file_name_,
                               file_id_);
        }
        return found;
    }

    uint64_t StoCPersistentFile::AllocateBuf(const std::string &filename,
                                             uint32_t size,
                                             FileInternalType internal_type) {
        NOVA_ASSERT(current_mem_offset_ + size <= allocated_mem_size_)
            << "exceed maximum stoc file size "
            << size << ","
            << allocated_mem_size_;
        leveldb::FileType type = leveldb::FileType::kCurrentFile;
        NOVA_ASSERT(ParseFileName(filename, &type));
        mutex_.lock();
        if (is_full_ || current_mem_offset_ + size > allocated_mem_size_) {
            Seal();
            mutex_.unlock();
            return UINT64_MAX;
        }
        NOVA_ASSERT(!sealed_);
        uint32_t off = current_mem_offset_;
        BlockHandle handle = {};
        handle.set_offset(off);
        handle.set_size(size);

        if (internal_type == FileInternalType::kFileMetadata) {
            NOVA_ASSERT(file_meta_block_offset_.find(filename) == file_meta_block_offset_.end());
        } else if (internal_type == FileInternalType::kFileParity) {
            NOVA_ASSERT(file_parity_block_offset_.find(filename) == file_parity_block_offset_.end());
        } else if (type == leveldb::FileType::kTableFile) {
            NOVA_ASSERT(file_block_offset_.find(filename) == file_block_offset_.end());
        }

        current_mem_offset_ += size;
        AllocatedBuf allocated_buf = {};
        allocated_buf.filename = filename;
        allocated_buf.offset = off;
        allocated_buf.size = size;
        allocated_buf.written_to_mem = false;
        allocated_buf.internal_type = internal_type;
        allocated_bufs_.push_back(allocated_buf);
        file_size_ += size;
        mutex_.unlock();
        return (uint64_t) (backing_mem_) + off;
    }

    uint64_t
    StoCPersistentFile::Persist(uint32_t given_file_id_for_assertion) {
        NOVA_ASSERT(given_file_id_for_assertion == file_id_)
            << fmt::format("{} {}", given_file_id_for_assertion, file_id_);

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
            NOVA_ASSERT(leveldb::ParseFileName(buf->filename, &type));

            if (buf->internal_type == FileInternalType::kFileMetadata) {
                NOVA_ASSERT(
                        file_meta_block_offset_.find(buf->filename) ==
                        file_meta_block_offset_.end());
                StoCPersistStatus &s = file_meta_block_offset_[buf->filename];
                s.disk_handle.set_offset(current_disk_offset_);
                s.disk_handle.set_size(buf->size);
                s.persisted = false;
            } else if (buf->internal_type == FileInternalType::kFileParity) {
                NOVA_ASSERT(
                        file_parity_block_offset_.find(buf->filename) ==
                        file_parity_block_offset_.end());
                StoCPersistStatus &s = file_parity_block_offset_[buf->filename];
                s.disk_handle.set_offset(current_disk_offset_);
                s.disk_handle.set_size(buf->size);
                s.persisted = false;
            } else {
                if (type == leveldb::FileType::kTableFile) {
                    NOVA_ASSERT(
                            file_block_offset_.find(buf->filename) ==
                            file_block_offset_.end());
                }
                StoCPersistStatus &s = file_block_offset_[buf->filename];
                s.disk_handle.set_offset(current_disk_offset_);
                s.disk_handle.set_size(buf->size);
                s.persisted = false;
            }

            BlockHandle mem_handle = {};
            mem_handle.set_offset(buf->offset);
            mem_handle.set_size(buf->size);
            persisting_cnt += 1;
            current_disk_offset_ += buf->size;

            BatchWrite bw = {};
            bw.mem_handle.set_offset(buf->offset);
            bw.mem_handle.set_size(buf->size);
            bw.sstable = buf->filename;
            bw.internal_type = buf->internal_type;
            written_mem_blocks_.push_back(bw);
            buf = allocated_bufs_.erase(buf);
        }

        NOVA_ASSERT(current_disk_offset_ <= file_size_);
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
            nova::NovaGlobalVariables::global.stoc_queue_depth += 1;
            nova::NovaGlobalVariables::global.stoc_pending_disk_writes += size;
            nova::NovaGlobalVariables::global.total_disk_writes += size;
            persisted_bytes += size;

            Status s = file_->Append(Slice(backing_mem_ + offset, size));
            NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
            s = file_->Sync();
            NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());

            nova::NovaGlobalVariables::global.stoc_queue_depth -= 1;
            nova::NovaGlobalVariables::global.stoc_pending_disk_writes -= size;

            mutex_.lock();
            for (int j = persisted_i; j < i; j++) {
                if (writes[j].internal_type == FileInternalType::kFileMetadata) {
                    NOVA_ASSERT(file_meta_block_offset_.find(writes[j].sstable) != file_meta_block_offset_.end());
                    file_meta_block_offset_[writes[j].sstable].persisted = true;
                } else if (writes[j].internal_type == FileInternalType::kFileParity) {
                    NOVA_ASSERT(file_parity_block_offset_.find(writes[j].sstable) != file_parity_block_offset_.end());
                    file_parity_block_offset_[writes[j].sstable].persisted = true;
                } else {
                    NOVA_ASSERT(file_block_offset_.find(writes[j].sstable) != file_block_offset_.end());
                    file_block_offset_[writes[j].sstable].persisted = true;
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
        nova::NovaGlobalVariables::global.stoc_queue_depth += 1;
        nova::NovaGlobalVariables::global.stoc_pending_disk_writes += size;
        nova::NovaGlobalVariables::global.total_disk_writes += size;
        persisted_bytes += size;

        Status s = file_->Append(Slice(backing_mem_ + offset, size));
        NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        s = file_->Sync();
        NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());

        nova::NovaGlobalVariables::global.stoc_queue_depth -= 1;
        nova::NovaGlobalVariables::global.stoc_pending_disk_writes -= size;

        mutex_.lock();
        for (int j = persisted_i; j < writes.size(); j++) {
            if (writes[j].internal_type == FileInternalType::kFileMetadata) {
                NOVA_ASSERT(file_meta_block_offset_.find(writes[j].sstable) != file_meta_block_offset_.end());
                file_meta_block_offset_[writes[j].sstable].persisted = true;
            } else if (writes[j].internal_type == FileInternalType::kFileParity) {
                NOVA_ASSERT(file_parity_block_offset_.find(writes[j].sstable) != file_parity_block_offset_.end());
                file_parity_block_offset_[writes[j].sstable].persisted = true;
            } else {
                NOVA_ASSERT(file_block_offset_.find(writes[j].sstable) != file_block_offset_.end());
                file_block_offset_[writes[j].sstable].persisted = true;
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

    bool
    StoCPersistentFile::DeleteSSTable(uint32_t given_fileid_for_assertion,
                                      const std::string &filename) {
        NOVA_ASSERT(given_fileid_for_assertion == file_id_)
            << fmt::format("{} {}", given_fileid_for_assertion, file_id_);
        bool delete_file = false;

        mutex_.lock();
        Seal();
        {
            auto it = file_block_offset_.find(filename);
            if (it != file_block_offset_.end()) {
                NOVA_ASSERT(it->second.persisted);
                int n = file_block_offset_.erase(filename);
                NOVA_ASSERT(n == 1);
            }
        }
        {
            auto it = file_meta_block_offset_.find(filename);
            if (it != file_meta_block_offset_.end()) {
                NOVA_ASSERT(it->second.persisted);
                int n = file_meta_block_offset_.erase(filename);
                NOVA_ASSERT(n == 1);
            }
        }
        {
            auto it = file_parity_block_offset_.find(filename);
            if (it != file_parity_block_offset_.end()) {
                NOVA_ASSERT(it->second.persisted);
                int n = file_parity_block_offset_.erase(filename);
                NOVA_ASSERT(n == 1);
            }
        }
        if (file_block_offset_.empty() && file_meta_block_offset_.empty() && file_parity_block_offset_.empty() &&
            is_full_ &&
            allocated_bufs_.empty() &&
            persisting_cnt == 0 && sealed_) {

            waiting_to_be_deleted = true;
            if (reading_cnt == 0) {
                if (!deleted_) {
                    deleted_ = true;
                    delete_file = true;
                }
            }
        }
        if (delete_file) {
            NOVA_ASSERT(current_disk_offset_ == file_size_);
        }
        mutex_.unlock();

        if (!delete_file) {
            return false;
        }
        NOVA_LOG(rdmaio::DEBUG) << fmt::format("Delete SSTable {} from Stoc File {}.", filename, stoc_file_name_);
        NOVA_ASSERT(file_);
        Status s = file_->Close();
        NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        delete file_;
        file_ = nullptr;
        s = env_->DeleteFile(stoc_file_name_);
        NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        return true;
    }

    void StoCPersistentFile::Close() {
        mutex_.lock();
        NOVA_ASSERT(allocated_bufs_.empty());
        NOVA_ASSERT(persisting_cnt == 0);
        is_full_ = true;
        Seal();

        NOVA_ASSERT(file_);
        Status s = file_->Close();
        NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        delete file_;
        file_ = nullptr;
        mutex_.unlock();
    }

    void StoCPersistentFile::ForceSeal() {
        mutex_.lock();
        NOVA_ASSERT(allocated_bufs_.empty());
        NOVA_ASSERT(persisting_cnt == 0);
        is_full_ = true;
        Seal();
        mutex_.unlock();
    }

    void StoCPersistentFile::Seal() {
        bool seal = false;
        if (allocated_bufs_.empty() && is_full_ && persisting_cnt == 0) {
            if (!sealed_) {
                seal = true;
                sealed_ = true;
            }
        }

        if (seal) {
            NOVA_ASSERT(current_disk_offset_ == file_size_);
        }

        if (!seal) {
            return;
        }

        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format(
                    "StoC file {} closed with t:{} file size {} allocated size {}",
                    file_id_, thread_id_,
                    file_size_, allocated_mem_size_);
        NOVA_ASSERT(backing_mem_);
        uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                  allocated_mem_size_);
        mem_manager_->FreeItem(thread_id_, backing_mem_, scid);
        backing_mem_ = nullptr;
    }

    BlockHandle
    StoCPersistentFile::Handle(const std::string &filename,
                               FileInternalType internal_type) {
        BlockHandle handle = {};
        while (true) {
            mutex_.lock();
            if (internal_type == FileInternalType::kFileMetadata) {
                auto it = file_meta_block_offset_.find(filename);
                NOVA_ASSERT(it != file_meta_block_offset_.end());
                StoCPersistStatus &s = it->second;
                if (s.persisted) {
                    handle = s.disk_handle;
                    mutex_.unlock();
                    break;
                }
            } else if (internal_type == FileInternalType::kFileParity) {
                auto it = file_parity_block_offset_.find(filename);
                NOVA_ASSERT(it != file_parity_block_offset_.end());
                StoCPersistStatus &s = it->second;
                if (s.persisted) {
                    handle = s.disk_handle;
                    mutex_.unlock();
                    break;
                }
            } else {
                auto it = file_block_offset_.find(filename);
                NOVA_ASSERT(it != file_block_offset_.end());
                StoCPersistStatus &s = it->second;
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

    bool StocPersistentFileManager::ReadDataBlockForReplication(
            const StoCBlockHandle &stoc_block_handle, uint64_t offset,
            uint32_t size, char *scratch, Slice *result) {
        StoCPersistentFile *stoc_file = FindStoCFile(stoc_block_handle.stoc_file_id);
        if (!stoc_file) {
            return false;
        }

        if (!block_cache_) {
            leveldb::FileType type;
            NOVA_ASSERT(ParseFileName(stoc_file->stoc_file_name_, &type));
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("Read {} from stoc file {} offset:{} size:{}",
                               stoc_block_handle.DebugString(),
                               stoc_file->file_id(), offset, size);
            auto status = stoc_file->ReadForReplication(offset, size, scratch, result);
            if (status.IsNotFound()) {
                return false;
            }
            NOVA_ASSERT(status.ok()) << status.ToString();
            NOVA_ASSERT(type == leveldb::FileType::kTableFile);
            NOVA_ASSERT(result->size() == size)
                << fmt::format("fn:{} given size:{} read size:{}",
                               stoc_file->stoc_file_name_,
                               size,
                               result->size());
            NOVA_ASSERT(scratch[size - 1] != 0)
                << fmt::format(
                        "Read {} from stoc file {} offset:{} size:{}",
                        stoc_block_handle.DebugString(),
                        stoc_file->file_id(), offset, size);
            return true;
        }

        char cache_key_buffer[StoCBlockHandle::HandleSize()];
        stoc_block_handle.EncodeHandle(cache_key_buffer);
        Slice key(cache_key_buffer, sizeof(cache_key_buffer));
        auto cache_handle = block_cache_->Lookup(key);
        if (cache_handle != nullptr) {
            auto block = reinterpret_cast<char *>(block_cache_->Value(
                    cache_handle));
            memcpy(scratch, block, stoc_block_handle.size);
        } else {
            stoc_file->Read(offset, size, scratch, result);
            char *block = new char[size];
            memcpy(block, scratch, size);
            cache_handle = block_cache_->Insert(key, block,
                                                size,
                                                &DeleteCachedBlock);
        }
        block_cache_->Release(cache_handle);
        return true;
    }

    void StocPersistentFileManager::ReadDataBlock(
            const leveldb::StoCBlockHandle &stoc_block_handle, uint64_t offset, uint32_t size, char *scratch,
            Slice *result) {
        StoCPersistentFile *stoc_file = FindStoCFile(stoc_block_handle.stoc_file_id);
        NOVA_ASSERT(stoc_file) << stoc_block_handle.stoc_file_id;
        if (!block_cache_) {
            leveldb::FileType type;
            NOVA_ASSERT(ParseFileName(stoc_file->stoc_file_name_, &type));
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("Read {} from stoc file {} offset:{} size:{}",
                               stoc_block_handle.DebugString(), stoc_file->file_id(), offset, size);
            NOVA_ASSERT(stoc_file->Read(offset, size, scratch, result).ok());
            if (type == leveldb::FileType::kTableFile) {
                NOVA_ASSERT(result->size() == size)
                    << fmt::format("fn:{} given size:{} read size:{}",
                                   stoc_file->stoc_file_name_, size, result->size());
                NOVA_ASSERT(stoc_file->sealed()) << fmt::format("Read but not sealed {}", stoc_file->stoc_file_name_);
//                NOVA_ASSERT(scratch[size - 1] != 0)
//                    << fmt::format(
//                            "Read {} from stoc file {} offset:{} size:{} result:{}",
//                            stoc_block_handle.DebugString(), stoc_file->file_id(), offset, size, result->size());
            } else {
                NOVA_LOG(rdmaio::DEBUG)
                    << fmt::format("Read file {} read size {}:{}", stoc_file->stoc_file_name_, size,
                                   result->size());
            }
            return;
        }

        char cache_key_buffer[StoCBlockHandle::HandleSize()];
        stoc_block_handle.EncodeHandle(cache_key_buffer);
        Slice key(cache_key_buffer, sizeof(cache_key_buffer));
        auto cache_handle = block_cache_->Lookup(key);
        if (cache_handle != nullptr) {
            auto block = reinterpret_cast<char *>(block_cache_->Value(
                    cache_handle));
            memcpy(scratch, block, stoc_block_handle.size);
        } else {
            stoc_file->Read(offset, size, scratch, result);
            char *block = new char[size];
            memcpy(block, scratch, size);
            cache_handle = block_cache_->Insert(key, block,
                                                size,
                                                &DeleteCachedBlock);
        }
        block_cache_->Release(cache_handle);
    }

    void StocPersistentFileManager::OpenStoCFiles(
            const std::unordered_map<std::string, uint32_t> &fn_files) {
        mutex_.lock();
        for (const auto &it : fn_files) {
            const auto &fn = it.first;
            const auto &fileid = it.second;
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("Open StoC file {} for file {}", fileid, fn);
            StoCPersistentFile *stoc_file = new StoCPersistentFile(
                    fileid, env_,
                    fn,
                    mem_manager_,
                    0, stoc_file_size_);
            stoc_file->ForceSeal();
            NOVA_ASSERT(stoc_files_[fileid] == nullptr)
                << fmt::format("{} {} {}", fileid, it.first,
                               stoc_files_[fileid]->stoc_file_name_);
            stoc_files_[fileid] = stoc_file;
            fn_stoc_file_map_[fn] = stoc_file;
            current_stoc_file_id_ = std::max(current_stoc_file_id_, fileid);
        }
        current_stoc_file_id_ += 1;
        mutex_.unlock();
    }

    StoCPersistentFile *
    StocPersistentFileManager::OpenStoCFile(uint32_t thread_id, std::string &filename) {
        mutex_.lock();
        auto stoc_file_ptr = fn_stoc_file_map_.find(filename);
        if (stoc_file_ptr != fn_stoc_file_map_.end()) {
            auto stoc_file = stoc_file_ptr->second;
            mutex_.unlock();
            return stoc_file;
        }
        // not found.
        FileType type;
        NOVA_ASSERT(leveldb::ParseFileName(filename, &type)) << filename;
        uint32_t id = 0;
        if (type == FileType::kDescriptorFile) {
            id = current_manifest_file_stoc_file_id_;
            current_manifest_file_stoc_file_id_ += 1;
            NOVA_LOG(rdmaio::DEBUG) << fmt::format("Open manifest file {} id:{}", filename, id);
            NOVA_ASSERT(
                    current_manifest_file_stoc_file_id_ <= MAX_MANIFEST_FILE_ID) << filename;
        } else {
            id = current_stoc_file_id_;
            current_stoc_file_id_ += 1;
        }
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("Create a new stoc file {} for thread {} fn:{}", id,
                           thread_id, filename);
        mutex_.unlock();
        uint32_t file_size = stoc_file_size_;
        if (type == FileType::kDescriptorFile) {
            file_size = nova::NovaConfig::config->manifest_file_size;
        }

        StoCPersistentFile *stoc_file = new StoCPersistentFile(id, env_,
                                                               filename,
                                                               mem_manager_,
                                                               thread_id,
                                                               file_size);
        mutex_.lock();
        NOVA_ASSERT(stoc_files_[id] == nullptr);
        stoc_files_[id] = stoc_file;
        fn_stoc_file_map_[filename] = stoc_file;
        mutex_.unlock();
        return stoc_file;
    }

    void
    StocPersistentFileManager::DeleteSSTable(const std::string &filename) {
        mutex_.lock();
        auto it = fn_stoc_file_map_.find(filename);
        StoCPersistentFile *stoc_file = nullptr;
        if (it != fn_stoc_file_map_.end()) {
            stoc_file = it->second;
            fn_stoc_file_map_.erase(filename);
        }
        mutex_.unlock();
        if (stoc_file) {
            stoc_file->DeleteSSTable(stoc_file->file_id(), filename);
        }
    }

    StoCPersistentFile *
    StocPersistentFileManager::FindStoCFile(uint32_t stoc_file_id) {
        mutex_.lock();
        StoCPersistentFile *stoc_file = stoc_files_[stoc_file_id];
        NOVA_ASSERT(stoc_file) << fmt::format("stoc file {} is null.", stoc_file_id);
        NOVA_ASSERT(stoc_file->file_id() == stoc_file_id)
            << fmt::format("stoc file {} {}.", stoc_file->file_id(), stoc_file_id);
        mutex_.unlock();
        return stoc_file;
    }

    StocPersistentFileManager::StocPersistentFileManager(
            leveldb::Env *env,
            leveldb::MemManager *mem_manager,
            const std::string &stoc_file_path,
            uint32_t stoc_file_size) :
            env_(env), mem_manager_(mem_manager),
            stoc_file_path_(stoc_file_path),
            stoc_file_size_(stoc_file_size) {
    }
}