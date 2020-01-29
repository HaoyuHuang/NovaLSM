
//
// Created by Haoyu Huang on 1/11/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <semaphore.h>
#include <leveldb/table.h>
#include <table/block.h>
#include <table/block_builder.h>
#include <util/crc32c.h>

#include "nova_cc.h"

#include "db/filename.h"
#include "nova/nova_config.h"

#define MAX_BLOCK_SIZE 102400

namespace leveldb {
    NovaCCMemFile::NovaCCMemFile(Env *env, const Options &options,
                                 uint64_t file_number,
                                 MemManager *mem_manager,
                                 CCClient *cc_client,
                                 const std::string &dbname,
                                 uint64_t thread_id,
                                 uint64_t file_size)
            : env_(env), options_(options), file_number_(file_number),
              fname_(TableFileName(dbname, file_number)),
              mem_manager_(mem_manager),
              cc_client_(cc_client),
              dbname_(dbname), thread_id_(thread_id),
              allocated_size_(file_size),
              MemFile(nullptr, "", false) {
        RDMA_ASSERT(mem_manager);
        RDMA_ASSERT(cc_client);

        // Only used for flushing SSTables.
        // Policy.
        uint32_t scid = mem_manager->slabclassid(thread_id, file_size);
        backing_mem_ = mem_manager->ItemAlloc(thread_id, scid);
        RDMA_ASSERT(backing_mem_) << "Running out of memory";

        RDMA_LOG(rdmaio::DEBUG) << fmt::format(
                    "Create remote memory file tid:{} fname:{} size:{}",
                    thread_id, fname_, file_size);
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
        Format();
        return Status::OK();
    }

    void NovaCCMemFile::Format() {
        Status s;
        int file_size = used_size_;
        Slice footer_input(backing_mem_ + file_size - Footer::kEncodedLength,
                           Footer::kEncodedLength);

        Footer footer;
        s = footer.DecodeFrom(&footer_input);
        RDMA_ASSERT(s.ok()) << fmt::format("footer", s.ToString());

        // Read the index block
        BlockContents index_block_contents;
        const char *buf = backing_mem_ + footer.index_handle().offset();
        Slice contents(buf, footer.index_handle().size());
        footer.index_handle().offset();
        s = Table::ReadBlock(buf, contents, ReadOptions(),
                             footer.index_handle(), &index_block_contents);
        RDMA_ASSERT(s.ok());

        index_block_ = new Block(index_block_contents,
                                 file_number_,
                                 footer.index_handle().offset());
        int num_data_blocks_in_group = num_data_blocks_ /
                                       nova::NovaCCConfig::cc_config->num_rtable_num_servers_scatter_data_blocks;
        int nblocks_in_group[nova::NovaCCConfig::cc_config->num_rtable_num_servers_scatter_data_blocks];
        for (int i = 0;
             i <
             nova::NovaCCConfig::cc_config->num_rtable_num_servers_scatter_data_blocks; i++) {
            nblocks_in_group[i] = num_data_blocks_in_group;
        }
        nblocks_in_group[
                nova::NovaCCConfig::cc_config->num_rtable_num_servers_scatter_data_blocks -
                1] +=
                num_data_blocks_ %
                nova::NovaCCConfig::cc_config->num_rtable_num_servers_scatter_data_blocks;

        Iterator *it = index_block_->NewIterator(options_.comparator);
        it->SeekToFirst();
        int n = 0;
        int offset = 0;
        int size = 0;
        int group_id = 0;
        uint32_t server_id = rand() % nova::NovaConfig::config->my_server_id;
        while (it->Valid()) {
            if (n == nblocks_in_group[group_id]) {
                uint32_t rtable_id = 0;
                uint32_t req_id = cc_client_->InitiateRTableWriteDataBlocks(
                        server_id, thread_id_, &rtable_id,
                        backing_mem_ + offset,
                        dbname_, file_number_,
                        size);
                WRITE_requests_.push_back(req_id);
                server_ids_.push_back(server_id);
                rtable_ids_.push_back(rtable_id);
                n = 0;
                offset = 0;
                size = 0;
                group_id += 1;
                server_id += 1;
                server_id %= nova::NovaConfig::config->my_server_id;
            }

            Slice key = it->key();
            Slice value = it->value();

            BlockHandle handle;
            s = handle.DecodeFrom(&value);
            RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
            if (n == 0) {
                offset = handle.offset();
            }
            size += handle.size();
            n++;
            RDMA_ASSERT(offset + size == handle.offset() + handle.size());
            it->Next();
        }
        delete it;
    }

    std::vector<RTableHandle> NovaCCMemFile::Persist() {
        std::vector<RTableHandle> handles;
        uint32_t reqs[server_ids_.size()];
        for (int i = 0; i < server_ids_.size(); i++) {
            std::vector<SSTableRTablePair> pairs;
            SSTableRTablePair pair;
            pair.rtable_id = rtable_ids_[i];
            pair.sstable_id = fname_;
            pairs.push_back(pair);
            reqs[i] = cc_client_->InitiatePersist(server_ids_[i], pairs);
        }

        for (int i = 0; i < server_ids_.size(); i++) {
            CCResponse response;
            while (!cc_client_->IsDone(reqs[i], &response));

            RDMA_ASSERT(response.rtable_handles.size() == 1);
            handles.push_back(response.rtable_handles[0]);
        }
        return handles;
    }

    void NovaCCMemFile::PullWRITEDataBlockRequests(bool block) {
        // Wait for all writes to complete.
        for (int i = 0; i < WRITE_requests_.size(); i++) {
            uint32_t req_id = WRITE_requests_[i];
            CCResponse response;
            while (block && !cc_client_->IsDone(req_id, &response)) {
            }
            if (rtable_ids_[i] == 0) {
                rtable_ids_[i] = response.rtable_id;
            }
        }
    }

    void
    NovaCCMemFile::Finalize(const std::vector<RTableHandle> &rtable_handles) {
        Status s;
        int file_size = used_size_;
        Slice footer_input(backing_mem_ + file_size - Footer::kEncodedLength,
                           Footer::kEncodedLength);

        Footer footer;
        s = footer.DecodeFrom(&footer_input);
        RDMA_ASSERT(s.ok()) << fmt::format("footer", s.ToString());

        Options opt(options_);
        BlockBuilder index_block_builder(&opt);
        Iterator *it = index_block_->NewIterator(options_.comparator);
        it->SeekToFirst();
        char handle_buf[RTableHandle::HandleSize()];

        int num_data_blocks_in_group = num_data_blocks_ /
                                       nova::NovaCCConfig::cc_config->num_rtable_num_servers_scatter_data_blocks;
        int nblocks_in_group[nova::NovaCCConfig::cc_config->num_rtable_num_servers_scatter_data_blocks];
        for (int i = 0;
             i <
             nova::NovaCCConfig::cc_config->num_rtable_num_servers_scatter_data_blocks; i++) {
            nblocks_in_group[i] = num_data_blocks_in_group;
        }
        nblocks_in_group[
                nova::NovaCCConfig::cc_config->num_rtable_num_servers_scatter_data_blocks -
                1] +=
                num_data_blocks_ %
                nova::NovaCCConfig::cc_config->num_rtable_num_servers_scatter_data_blocks;

        RTableHandle db_handle = rtable_handles[0];
        uint64_t relative_offset = 0;
        int group_id = 0;
        int n = 0;

        while (it->Valid()) {
            if (n == nblocks_in_group[group_id]) {
                group_id++;
                db_handle = rtable_handles[group_id];

                n = 0;
                relative_offset = 0;
            }

            Slice key = it->key();
            Slice value = it->value();
            BlockHandle handle;
            s = handle.DecodeFrom(&value);
            RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());

            if (n == 0) {
                relative_offset = handle.offset();
            }

            db_handle.offset += (handle.offset() - relative_offset);
            db_handle.size = handle.size();
            db_handle.EncodeHandle(handle_buf);
            index_block_builder.Add(key, Slice(handle_buf,
                                               RTableHandle::HandleSize()));
            it->Next();
            n++;
        }
        uint32_t size = WriteBlock(&index_block_builder,
                                   footer.index_handle().offset());
        BlockHandle new_idx_handle = {};
        new_idx_handle.set_offset(footer.index_handle().offset());
        new_idx_handle.set_size(size);

        // Add new footer.
        Footer new_footer;
        new_footer.set_metaindex_handle(footer.metaindex_handle());
        new_footer.set_index_handle(new_idx_handle);
        std::string footer_encoding;
        new_footer.EncodeTo(&footer_encoding);

        uint32_t footer_start = new_idx_handle.offset() + new_idx_handle.size();
        Write(footer_start, footer_encoding);
        uint32_t new_file_size = new_footer.metaindex_handle().size() +
                                 new_footer.index_handle().size() +
                                 footer_encoding.size();

        WritableFile *writable_file;
        EnvFileMetadata meta;
        s = env_->NewWritableFile(fname_, meta, &writable_file);
        RDMA_ASSERT(s.ok());
        uint32_t start = footer.metaindex_handle().offset();
        Slice sstable_rtable(backing_mem_ + start, new_file_size);
        writable_file->Append(sstable_rtable);
        writable_file->Flush();
        writable_file->Close();
        delete writable_file;
        writable_file = nullptr;
    }

    uint32_t
    NovaCCMemFile::WriteBlock(BlockBuilder *block, uint64_t offset) {
        // File format contains a sequence of blocks where each block has:
        //    block_data: uint8[n]
        //    type: uint8
        //    crc: uint32
        Slice raw = block->Finish();

        Slice block_contents;
        CompressionType type = options_.compression;
        std::string compressed;
        switch (type) {
            case kNoCompression:
                block_contents = raw;
                break;
            case kSnappyCompression: {
                if (port::Snappy_Compress(raw.data(), raw.size(),
                                          &compressed) &&
                    compressed.size() < raw.size() - (raw.size() / 8u)) {
                    block_contents = compressed;
                } else {
                    // Snappy not supported, or compressed less than 12.5%, so just
                    // store uncompressed form
                    block_contents = raw;
                    type = kNoCompression;
                }
                break;
            }
        }
        uint32_t size = WriteRawBlock(block_contents, type, offset);
        block->Reset();
        return size;
    }

    uint32_t NovaCCMemFile::WriteRawBlock(const Slice &block_contents,
                                          CompressionType type,
                                          uint64_t offset) {
        Write(offset, block_contents);
        char trailer[kBlockTrailerSize];
        trailer[0] = type;
        uint32_t crc = crc32c::Value(block_contents.data(),
                                     block_contents.size());
        crc = crc32c::Extend(crc, trailer,
                             1);  // Extend crc to cover block type
        EncodeFixed32(trailer + 1, crc32c::Mask(crc));
        Write(offset + block_contents.size(),
              Slice(trailer, kBlockTrailerSize));
        return block_contents.size() + kBlockTrailerSize;
    }


    NovaCCRandomAccessFile::NovaCCRandomAccessFile(
            Env *env, const std::string &dbname, uint64_t file_number,
            const leveldb::FileMetaData &meta, leveldb::CCClient *dc_client,
            leveldb::MemManager *mem_manager, Options options,
            uint64_t thread_id,
            bool prefetch_all) : env_(env), dbname_(dbname),
                                 file_number_(file_number),
                                 meta_(meta), dc_client_(dc_client),
                                 mem_manager_(mem_manager),
                                 options_(options),
                                 thread_id_(thread_id),
                                 prefetch_all_(prefetch_all) {
        RDMA_ASSERT(mem_manager_);
        RDMA_ASSERT(dc_client_);

        Status s = env_->NewRandomAccessFile(TableFileName(dbname, file_number),
                                             &local_ra_file_);
        RDMA_ASSERT(s.ok()) << s.ToString();
    }

    NovaCCRandomAccessFile::~NovaCCRandomAccessFile() {
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

    Status NovaCCRandomAccessFile::Read(const RTableHandle &rtable_handle,
                                        uint64_t offset, size_t n,
                                        leveldb::Slice *result,
                                        char *scratch) {
        RDMA_ASSERT(scratch);
        if (rtable_handle.size == 0) {
            return local_ra_file_->Read(rtable_handle, offset, n, result,
                                        scratch);
        }

        // RTable handle. Read it.
        char *ptr = nullptr;
        if (!prefetch_all_ && backing_mem_block_ == nullptr) {
            uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                      MAX_BLOCK_SIZE);
            backing_mem_block_ = mem_manager_->ItemAlloc(thread_id_, scid);
            RDMA_ASSERT(backing_mem_block_) << "Running out of memory";
        }

        uint64_t local_offset = 0;
        if (prefetch_all_) {
            if (backing_mem_table_ == nullptr) {
                RDMA_ASSERT(ReadAll().ok());
            }
            uint64_t id =
                    (((uint64_t) rtable_handle.server_id) << 32) |
                    rtable_handle.rtable_id;
            DataBlockRTableLocalBuf &buf = rtable_local_offset_[id];
            local_offset =
                    buf.local_offset + (rtable_handle.offset - buf.offset);

            RDMA_ASSERT(local_offset);
            ptr = &backing_mem_table_[local_offset];
        } else {
            uint32_t req_id = dc_client_->InitiateRTableReadDataBlock(
                    rtable_handle,
                    backing_mem_block_);
            while (!dc_client_->IsDone(req_id, nullptr));
        }
        memcpy(scratch, ptr, n);
        *result = Slice(scratch, n);
        return Status::OK();
    }

    Status NovaCCRandomAccessFile::ReadAll() {
        uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                  meta_.actual_file_size);
        backing_mem_table_ = mem_manager_->ItemAlloc(thread_id_, scid);
        RDMA_ASSERT(backing_mem_table_) << "Running out of memory";
        uint64_t offset = 0;

        uint32_t reqs[meta_.data_block_group_handles.size()];
        for (int i = 0; i < meta_.data_block_group_handles.size(); i++) {
            RTableHandle &handle = meta_.data_block_group_handles[i];
            uint64_t id =
                    (((uint64_t) handle.server_id) << 32) |
                    handle.rtable_id;
            reqs[i] = dc_client_->InitiateRTableReadSSTableDataBlock(
                    handle.server_id,
                    dbname_,
                    file_number_,
                    handle.size,
                    backing_mem_table_ + offset);
            offset += handle.size;
            DataBlockRTableLocalBuf buf;
            buf.offset = handle.offset;
            buf.size = handle.size;
            buf.local_offset = (uint64_t) (backing_mem_table_ + offset);
            rtable_local_offset_[id] = buf;
        }

        // Wait for all reads to complete.
        for (int i = 0; i < meta_.data_block_group_handles.size(); i++) {
            while (!dc_client_->IsDone(reqs[i], nullptr));
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