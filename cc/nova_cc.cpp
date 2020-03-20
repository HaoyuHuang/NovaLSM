
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

#define MAX_BLOCK_SIZE 10240

namespace leveldb {
    namespace {
        bool dc_stats_comparator(const DCStatsStatus &s1,
                                 const DCStatsStatus &s2) {
            return s1.response.dc_queue_depth < s2.response.dc_queue_depth;
        }
    }

    NovaCCMemFile::NovaCCMemFile(Env *env, const Options &options,
                                 uint64_t file_number,
                                 MemManager *mem_manager,
                                 CCClient *cc_client,
                                 const std::string &dbname,
                                 uint64_t thread_id,
                                 uint64_t file_size, unsigned int *rand_seed)
            : env_(env), options_(options), file_number_(file_number),
              fname_(TableFileName(dbname, file_number)),
              mem_manager_(mem_manager),
              cc_client_(cc_client),
              dbname_(dbname), thread_id_(thread_id),
              allocated_size_(file_size), rand_seed_(rand_seed),
              MemFile(nullptr, "", false) {
        RDMA_ASSERT(mem_manager);
        RDMA_ASSERT(cc_client);

        // Only used for flushing SSTables.
        // Policy.
        uint32_t scid = mem_manager->slabclassid(thread_id, file_size);
        backing_mem_ = mem_manager->ItemAlloc(thread_id, scid);
        RDMA_ASSERT(backing_mem_) << "Running out of memory " << file_size;

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
        RTableHandle index_handle = {};
        index_handle.offset = footer.index_handle().offset();
        index_handle.size = footer.index_handle().size();
        s = Table::ReadBlock(buf, contents, ReadOptions(),
                             index_handle, &index_block_contents);
        RDMA_ASSERT(s.ok());

        index_block_ = new Block(index_block_contents,
                                 file_number_,
                                 footer.index_handle().offset());
        // 4 KB 250 = 1 MB
        int min_num_data_blocks_in_group = num_data_blocks_ /
                                           nova::NovaCCConfig::cc_config->num_rtable_num_servers_scatter_data_blocks;
        if (num_data_blocks_ <= 250) {
            min_num_data_blocks_in_group = num_data_blocks_;
        }

        uint32_t assigned_blocks = 0;
        while (assigned_blocks < num_data_blocks_) {
            int remaining_blocks = num_data_blocks_ - assigned_blocks;
            if (remaining_blocks < min_num_data_blocks_in_group) {
                if (nblocks_in_group_.empty()) {
                    nblocks_in_group_.push_back(remaining_blocks);
                } else {
                    nblocks_in_group_[nblocks_in_group_.size() -
                                      1] += remaining_blocks;
                }
                break;
            }
            nblocks_in_group_.push_back(min_num_data_blocks_in_group);
            assigned_blocks += min_num_data_blocks_in_group;
        }

        Iterator *it = index_block_->NewIterator(options_.comparator);
        it->SeekToFirst();
        int n = 0;
        int offset = 0;
        int size = 0;
        int group_id = 0;

        auto client = reinterpret_cast<NovaBlockCCClient *> (cc_client_);
        int scatter_dcs[nblocks_in_group_.size()];

        // Pull stats from all DCs.
        if (nova::NovaConfig::config->scatter_policy ==
            nova::ScatterPolicy::SCATTER_DC_STATS) {
            for (int i = 0;
                 i < nova::NovaCCConfig::cc_config->dc_servers.size(); i++) {
                uint32_t server_id = nova::NovaCCConfig::cc_config->dc_servers[i].server_id;
                uint32_t req_id = client->InitiateReadDCStats(
                        nova::NovaCCConfig::cc_config->dc_servers[i].server_id);
                DCStatsStatus status;
                status.remote_dc_id = server_id;
                status.req_id = req_id;
                dc_stats_status_.push_back(status);
            }

            for (int i = 0;
                 i < dc_stats_status_.size(); i++) {
                client->Wait();
            }

            for (int i = 0;
                 i < dc_stats_status_.size(); i++) {
                RDMA_ASSERT(client->IsDone(dc_stats_status_[i].req_id,
                                           &dc_stats_status_[i].response,
                                           nullptr));
            }
            // sort the dc stats.
            std::sort(dc_stats_status_.begin(), dc_stats_status_.end(),
                      dc_stats_comparator);
            for (int i = 0; i < nblocks_in_group_.size(); i++) {
                scatter_dcs[i] = dc_stats_status_[i].remote_dc_id;
            }
        } else {
            // Random.
            uint32_t start_dc_id = rand_r(rand_seed_) %
                                   nova::NovaCCConfig::cc_config->dc_servers.size();
            for (int i = 0; i < nblocks_in_group_.size(); i++) {
                scatter_dcs[i] = nova::NovaCCConfig::cc_config->dc_servers[start_dc_id].server_id;
                start_dc_id = (start_dc_id + 1) %
                              nova::NovaCCConfig::cc_config->dc_servers.size();
            }
        }

        uint32_t sid = 0;
        uint32_t dbid = 0;
        nova::ParseDBIndexFromDBName(dbname_, &sid, &dbid);

        while (it->Valid()) {
            Slice key = it->key();
            Slice value = it->value();

            BlockHandle handle;
            s = handle.DecodeFrom(&value);
            // Size + crc.
            handle.set_size(handle.size() + kBlockTrailerSize);
            RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
            if (n == 0) {
                offset = handle.offset();
            }
            size += handle.size();
            n++;
            RDMA_ASSERT(offset + size == handle.offset() + handle.size());
            it->Next();

            if (n == nblocks_in_group_[group_id]) {
                uint32_t rtable_id = 0;
                client->set_dbid(dbid);
                uint32_t req_id = client->InitiateRTableWriteDataBlocks(
                        scatter_dcs[group_id], thread_id_, &rtable_id,
                        backing_mem_ + offset,
                        dbname_, file_number_,
                        size, false);
                RDMA_LOG(rdmaio::DEBUG)
                    << fmt::format(
                            "t[{}]: Initiate WRITE data blocks s:{} req:{} db:{} fn:{}",
                            thread_id_, scatter_dcs[group_id], req_id,
                            dbname_, file_number_);

                PersistStatus status = {};
                status.remote_server_id = scatter_dcs[group_id];
                status.WRITE_req_id = req_id;
                status.result_handle = {};
                status_.push_back(status);

                n = 0;
                offset = 0;
                size = 0;
                group_id += 1;
            }
        }
        RDMA_ASSERT(group_id == nblocks_in_group_.size());
        RDMA_ASSERT(n == 0)
            << fmt::format("Contain {} data blocks. Read {} data blocks",
                           num_data_blocks_, n);
        delete it;
    }

    void NovaCCMemFile::WaitForPersistingDataBlocks() {
        auto client = reinterpret_cast<NovaBlockCCClient *> (cc_client_);
        for (int i = 0; i < nblocks_in_group_.size(); i++) {
            client->Wait();
        }
    }

    uint32_t
    NovaCCMemFile::Finalize() {
        auto client = reinterpret_cast<NovaBlockCCClient *> (cc_client_);
        // Wait for all writes to complete.
        for (int i = 0; i < status_.size(); i++) {
            uint32_t req_id = status_[i].WRITE_req_id;
            CCResponse response = {};
            RDMA_ASSERT(client->IsDone(req_id, &response, nullptr));
            RDMA_ASSERT(response.rtable_handles.size() == 1)
                << fmt::format("{} {}", req_id, response.rtable_handles.size());
            status_[i].result_handle = response.rtable_handles[0];
        }

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

        RTableHandle db_handle = status_[0].result_handle;
        RTableHandle index_handle = db_handle;
        uint64_t relative_offset = 0;
        int group_id = 0;
        int n = 0;
        char handle_buf[RTableHandle::HandleSize()];
        uint64_t filter_block_offset = 0;
        while (it->Valid()) {
            Slice key = it->key();
            Slice value = it->value();

//            leveldb::ParsedInternalKey ikey;
//            leveldb::ParseInternalKey(key, &ikey);
            BlockHandle handle;
            s = handle.DecodeFrom(&value);

            RDMA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());

            if (n == 0) {
                relative_offset = handle.offset();
            }

            filter_block_offset =
                    handle.offset() + handle.size() + kBlockTrailerSize;

            index_handle.offset =
                    (handle.offset() - relative_offset) + db_handle.offset;
            // Does not include crc.
            index_handle.size = handle.size();
            index_handle.EncodeHandle(handle_buf);
            index_block_builder.Add(key, Slice(handle_buf,
                                               RTableHandle::HandleSize()));

//            RDMA_LOG(rdmaio::DEBUG)
//                << fmt::format(
//                        "ikey:{} off:{} size:{} rserver:{} rtable:{} roff:{} rsize:{}",
//                        ikey.user_key.ToString(),
//                        handle.offset(),
//                        handle.size(), index_handle.server_id,
//                        index_handle.rtable_id, index_handle.offset,
//                        index_handle.size);


            it->Next();
            n++;

            if (n == nblocks_in_group_[group_id]) {
                // Cover the block handle in the RTable.
                RDMA_ASSERT(db_handle.offset + db_handle.size ==
                            index_handle.offset + index_handle.size +
                            kBlockTrailerSize);
                group_id++;
                n = 0;
                relative_offset = 0;
                if (group_id == status_.size()) {
                    RDMA_ASSERT(!it->Valid());
                    break;
                }
                db_handle = status_[group_id].result_handle;
                index_handle = db_handle;
            }
        }

        RDMA_ASSERT(n == 0)
            << fmt::format("Contain {} data blocks. Read {} data blocks",
                           num_data_blocks_, n);

        // Rewrite index handle for filter block.
        uint32_t filter_block_size =
                footer.metaindex_handle().offset() - filter_block_offset -
                kBlockTrailerSize;
        uint64_t new_file_size = filter_block_size + kBlockTrailerSize;
        const uint64_t rewrite_start_offset =
                footer.metaindex_handle().offset() - new_file_size;

        BlockHandle new_filter_handle = {};
        new_filter_handle.set_offset(0);
        new_filter_handle.set_size(filter_block_size);
        BlockHandle new_meta_handle = {};
        BlockHandle new_idx_handle = {};
        {
            // rewrite meta index block.
            BlockBuilder meta_index_block(&options_);
            // Add mapping from "filter.Name" to location of filter data
            std::string key = "filter.";
            key.append(options_.filter_policy->Name());
            std::string handle_encoding;
            new_filter_handle.EncodeTo(&handle_encoding);
            meta_index_block.Add(key, handle_encoding);
            uint32_t size = WriteBlock(&meta_index_block,
                                       rewrite_start_offset + new_file_size);
            new_meta_handle.set_offset(new_file_size);
            new_meta_handle.set_size(size - kBlockTrailerSize);
            new_file_size += size;
        }

        //Rewrite index block.
        {
            uint32_t size = WriteBlock(&index_block_builder,
                                       rewrite_start_offset + new_file_size);
            new_idx_handle.set_offset(new_file_size);
            new_idx_handle.set_size(size - kBlockTrailerSize);
            new_file_size += size;
        }

        // Add new footer.
        Footer new_footer;
        new_footer.set_metaindex_handle(new_meta_handle);
        new_footer.set_index_handle(new_idx_handle);
        std::string new_footer_encoding;
        new_footer.EncodeTo(&new_footer_encoding);
        Write(rewrite_start_offset + new_file_size, new_footer_encoding);
        new_file_size += new_footer_encoding.size();

        RDMA_ASSERT(rewrite_start_offset + new_file_size < allocated_size_);

        RDMA_LOG(rdmaio::DEBUG) << fmt::format(
                    "New SSTable {} size:{} old-start-offset:{} filter-block-size:{} meta_index_block:{}:{}. index_handle:{}:{}",
                    fname_, new_file_size, rewrite_start_offset,
                    filter_block_size,
                    new_meta_handle.offset(), new_meta_handle.size(),
                    new_idx_handle.offset(), new_idx_handle.size());

        WritableFile *writable_file;
        EnvFileMetadata meta = {};
        s = env_->NewWritableFile(fname_, meta, &writable_file);
        RDMA_ASSERT(s.ok());
        Slice sstable_rtable(backing_mem_ + rewrite_start_offset,
                             new_file_size);
        s = writable_file->Append(sstable_rtable);
        RDMA_ASSERT(s.ok());
        s = writable_file->Flush();
        RDMA_ASSERT(s.ok());
        s = writable_file->Sync();
        RDMA_ASSERT(s.ok());
        s = writable_file->Close();
        RDMA_ASSERT(s.ok());
        delete writable_file;
        writable_file = nullptr;

        if (PERSIST_META_BLOCKS_TO_RTABLE) {
            uint32_t dc_id = rand_r(rand_seed_) %
                                   nova::NovaCCConfig::cc_config->dc_servers.size();
            dc_id = nova::NovaCCConfig::cc_config->dc_servers[dc_id].server_id;
            uint32_t req_id = client->InitiateRTableWriteDataBlocks(dc_id,
                                                                    thread_id_,
                                                                    nullptr,
                                                                    backing_mem_ +
                                                                    rewrite_start_offset,
                                                                    dbname_,
                                                                    file_number_,
                                                                    new_file_size, /*is_meta_blocks=*/
                                                                    true);
            client->Wait();
            CCResponse response = {};
            RDMA_ASSERT(client->IsDone(req_id, &response, nullptr));
            RDMA_ASSERT(response.rtable_handles.size() == 1)
                << fmt::format("{} {}", req_id, response.rtable_handles.size());
            meta_block_handle_ = response.rtable_handles[0];
        }
        return new_file_size;
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
        // Make sure the last byte is not 0.
        trailer[kBlockTrailerSize - 1] = '!';
        EncodeFixed32(trailer + 1, crc32c::Mask(crc));
        Write(offset + block_contents.size(),
              Slice(trailer, kBlockTrailerSize));
        return block_contents.size() + kBlockTrailerSize;
    }


    NovaCCRandomAccessFile::NovaCCRandomAccessFile(
            Env *env, const std::string &dbname, uint64_t file_number,
            const leveldb::FileMetaData &meta, leveldb::CCClient *dc_client,
            leveldb::MemManager *mem_manager,
            uint64_t thread_id,
            bool prefetch_all) : env_(env), dbname_(dbname),
                                 file_number_(file_number),
                                 meta_(meta),
                                 mem_manager_(mem_manager),
                                 thread_id_(thread_id),
                                 prefetch_all_(prefetch_all) {
//        prefetch_all_ = false;
        RDMA_ASSERT(mem_manager_);

        uint32_t server_id = 0;
        nova::ParseDBIndexFromDBName(dbname, &server_id, &dbid_);
        Status s = env_->NewRandomAccessFile(TableFileName(dbname, file_number),
                                             &local_ra_file_);

        auto dc = reinterpret_cast<leveldb::NovaBlockCCClient *>(dc_client);
        RDMA_ASSERT(dc);
        dc->set_dbid(dbid_);

        if (prefetch_all_) {
            RDMA_ASSERT(ReadAll(dc_client).ok());
        }
        RDMA_ASSERT(s.ok()) << s.ToString();
    }

    Status NovaCCRandomAccessFile::Read(
            const leveldb::ReadOptions &read_options,
            const leveldb::RTableHandle &rtable_handle, uint64_t offset,
            size_t n, leveldb::Slice *result, char *scratch) {
        RDMA_ASSERT(scratch);
        if (rtable_handle.rtable_id == 0) {
            return local_ra_file_->Read(rtable_handle, offset, n, result,
                                        scratch);
        }

        // RTable handle. Read it.
        char *ptr = nullptr;
        uint64_t local_offset = 0;
        if (prefetch_all_) {
            RDMA_ASSERT(backing_mem_table_);
            uint64_t id =
                    (((uint64_t) rtable_handle.server_id) << 32) |
                    rtable_handle.rtable_id;
            DataBlockRTableLocalBuf &buf = rtable_local_offset_[id];
            local_offset =
                    buf.local_offset + (offset - buf.offset);
            ptr = &backing_mem_table_[local_offset];
            memcpy(scratch, ptr, n);
            *result = Slice(scratch, n);
        } else {
            RDMA_ASSERT(n < MAX_BLOCK_SIZE);
            char *backing_mem_block = nullptr;
            int buf_id = -1;

            // Search the first available buf.
            mutex_.lock();
            for (int i = 0; i < backing_mem_blocks_.size(); i++) {
                if (!backing_mem_blocks_[i].is_using) {
                    buf_id = i;
                    break;
                }
            }

            if (buf_id == -1) {
                uint32_t scid = mem_manager_->slabclassid(
                        read_options.thread_id,
                        MAX_BLOCK_SIZE);
                backing_mem_block = mem_manager_->ItemAlloc(
                        read_options.thread_id, scid);
                DataBlockBuf buf = {};
                buf.is_using = true;
                buf.thread_id = read_options.thread_id;
                buf.buf = backing_mem_block;
                backing_mem_blocks_.emplace_back(buf);
                buf_id = backing_mem_blocks_.size() - 1;
            }
            backing_mem_blocks_[buf_id].is_using = true;
            backing_mem_block = backing_mem_blocks_[buf_id].buf;
            mutex_.unlock();
            RDMA_ASSERT(backing_mem_block);
            auto dc = reinterpret_cast<leveldb::NovaBlockCCClient *>(read_options.dc_client);
            dc->set_dbid(dbid_);
            uint32_t req_id = dc->InitiateRTableReadDataBlock(
                    rtable_handle, offset, n, backing_mem_block);
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("t[{}]: CCRead req:{} start db:{} fn:{} s:{}",
                               read_options.thread_id,
                               req_id, dbid_, file_number_, n);
            dc->Wait();

            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("t[{}]: CCRead req:{} complete db:{} fn:{} s:{}",
                               read_options.thread_id,
                               req_id, dbid_, file_number_, n);
            RDMA_ASSERT(dc->IsDone(req_id, nullptr, nullptr));
            RDMA_ASSERT(nova::IsRDMAWRITEComplete(backing_mem_block, n))
                << fmt::format("t[{}]: {}", read_options.thread_id, req_id);

            ptr = backing_mem_block;
            memcpy(scratch, ptr, n);
            *result = Slice(scratch, n);

            // Return the buf.
            mutex_.lock();
            backing_mem_blocks_[buf_id].is_using = false;
            mutex_.unlock();
        }
        return Status::OK();
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

        for (auto &it : backing_mem_blocks_) {
            RDMA_ASSERT(!it.is_using);
            uint32_t scid = mem_manager_->slabclassid(it.thread_id,
                                                      MAX_BLOCK_SIZE);
            mem_manager_->FreeItem(it.thread_id, it.buf, scid);
        }
    }

    Status NovaCCRandomAccessFile::Read(const RTableHandle &rtable_handle,
                                        uint64_t offset, size_t n,
                                        leveldb::Slice *result,
                                        char *scratch) {
        RDMA_ASSERT(false);
        return Status::OK();
    }

    Status NovaCCRandomAccessFile::ReadAll(CCClient *dc_client) {
        uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                  meta_.file_size);
        backing_mem_table_ = mem_manager_->ItemAlloc(thread_id_, scid);
        RDMA_ASSERT(backing_mem_table_) << "Running out of memory";
        uint64_t offset = 0;

        uint32_t reqs[meta_.data_block_group_handles.size()];
        auto dc = reinterpret_cast<leveldb::NovaBlockCCClient *>(dc_client);
        dc->set_dbid(dbid_);

        for (int i = 0; i < meta_.data_block_group_handles.size(); i++) {
            RTableHandle &handle = meta_.data_block_group_handles[i];
            uint64_t id =
                    (((uint64_t) handle.server_id) << 32) |
                    handle.rtable_id;
            reqs[i] = dc->InitiateRTableReadDataBlock(handle,
                                                      handle.offset,
                                                      handle.size,
                                                      backing_mem_table_ +
                                                      offset);
            DataBlockRTableLocalBuf buf = {};
            buf.offset = handle.offset;
            buf.size = handle.size;
            buf.local_offset = offset;
            rtable_local_offset_[id] = buf;
            offset += handle.size;
        }

        // Wait for all reads to complete.
        for (int i = 0; i < meta_.data_block_group_handles.size(); i++) {
            dc->Wait();
        }
        offset = 0;
        for (int i = 0; i < meta_.data_block_group_handles.size(); i++) {
            RTableHandle &handle = meta_.data_block_group_handles[i];
            RDMA_ASSERT(dc->IsDone(reqs[i], nullptr, nullptr));
            RDMA_ASSERT(nova::IsRDMAWRITEComplete(backing_mem_table_ + offset,
                                                  handle.size));
            offset += handle.size;
        }
        return Status::OK();
    }
}