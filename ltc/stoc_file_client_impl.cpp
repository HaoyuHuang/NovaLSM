
//
// Created by Haoyu Huang on 1/11/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <semaphore.h>
#include <leveldb/table.h>
#include <table/block.h>
#include <table/block_builder.h>
#include <util/crc32c.h>

#include "stoc_file_client_impl.h"
#include "storage_selector.h"
#include "db/filename.h"
#include "common/nova_config.h"

namespace leveldb {
    StoCWritableFileClient::StoCWritableFileClient(Env *env,
                                                   const Options &options,
                                                   uint64_t file_number,
                                                   MemManager *mem_manager,
                                                   StoCClient *stoc_client,
                                                   const std::string &dbname,
                                                   uint64_t thread_id,
                                                   uint64_t file_size,
                                                   unsigned int *rand_seed,
                                                   std::string &filename)
            : mem_env_(env), options_(options), file_number_(file_number),
              fname_debug_only_(filename),
              mem_manager_(mem_manager),
              stoc_client_(stoc_client),
              dbname_(dbname), thread_id_(thread_id),
              allocated_size_(file_size), rand_seed_(rand_seed),
              MemFile(nullptr, "", false) {
        NOVA_ASSERT(mem_manager);
        NOVA_ASSERT(stoc_client);
        meta_block_handles_.resize(nova::NovaConfig::config->number_of_sstable_metadata_replicas);
        data_replica_status_.resize(nova::NovaConfig::config->number_of_sstable_data_replicas);
        // Only used for flushing SSTables.
        // Policy.
        NOVA_LOG(rdmaio::DEBUG) << fmt::format("create file w {}", filename);
        uint32_t scid = mem_manager->slabclassid(thread_id, file_size);
        backing_mem_ = mem_manager->ItemAlloc(thread_id, scid);
        NOVA_ASSERT(backing_mem_) << "Running out of memory " << file_size;
        NOVA_LOG(rdmaio::DEBUG) << fmt::format(
                    "Create remote memory file tid:{} fname:{} size:{}",
                    thread_id, fname_debug_only_, file_size);
    }

    StoCWritableFileClient::~StoCWritableFileClient() {
        if (backing_mem_) {
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("close file w {}", fname_debug_only_);
            uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                      allocated_size_);
            mem_manager_->FreeItem(thread_id_, backing_mem_, scid);

            NOVA_LOG(rdmaio::DEBUG) << fmt::format(
                        "Free remote memory file tid:{} fn:{} size:{}",
                        thread_id_, fname_debug_only_, allocated_size_);
        }
        if (parity_block_backing_mem_) {
            uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                      parity_block_size_);
            mem_manager_->FreeItem(thread_id_, parity_block_backing_mem_, scid);
            NOVA_LOG(rdmaio::DEBUG) << fmt::format(
                        "Free parity memory file tid:{} fn:{} size:{}",
                        thread_id_, fname_debug_only_, parity_block_size_);
        }
        if (index_block_) {
            delete index_block_;
        }
    }

    Status
    StoCWritableFileClient::Read(uint64_t offset, size_t n,
                                 leveldb::Slice *result,
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

    char *StoCWritableFileClient::Buf() {
        return backing_mem_ + used_size_;
    }

    Status StoCWritableFileClient::Append(uint32_t size) {
        NOVA_ASSERT(used_size_ + size < allocated_size_)
            << fmt::format(
                    "ccremotememfile[{}]: fn:{} db:{} alloc_size:{} used_size:{} data size:{}",
                    thread_id_, fname_debug_only_, dbname_, allocated_size_,
                    used_size_,
                    size);
        used_size_ += size;
        return Status::OK();
    }

    Status
    StoCWritableFileClient::SyncAppend(const leveldb::Slice &data,
                                       const std::vector<uint32_t> &stoc_ids) {
        char *buf = backing_mem_ + used_size_;
        NOVA_ASSERT(used_size_ + data.size() < allocated_size_)
            << fmt::format(
                    "writablefile[{}]: fn:{} db:{} alloc_size:{} used_size:{} data size:{}",
                    thread_id_, fname_debug_only_, dbname_, allocated_size_,
                    used_size_,
                    data.size());
        uint32_t stoc_file_id;
        auto client = reinterpret_cast<StoCBlockClient *> (stoc_client_);
        std::vector<uint32_t> reqs;
        for (int replica_id = 0; replica_id < stoc_ids.size(); replica_id++) {
            uint32_t stoc_id = stoc_ids[replica_id];
            uint32_t req_id = client->InitiateAppendBlock(stoc_id, 0,
                                                          &stoc_file_id, buf,
                                                          dbname_, 0,
                                                          replica_id,
                                                          data.size(), FileInternalType::kFileData);
            reqs.push_back(req_id);
        }
        for (auto reqid : reqs) {
            client->Wait();
        }

        for (auto reqid : reqs) {
            StoCResponse response;
            NOVA_ASSERT(client->IsDone(reqid, &response, nullptr));
        }
        used_size_ += data.size();
        return Status::OK();
    }

    Status StoCWritableFileClient::Append(const leveldb::Slice &data) {
        char *buf = backing_mem_ + used_size_;
        NOVA_ASSERT(used_size_ + data.size() < allocated_size_)
            << fmt::format(
                    "ccremotememfile[{}]: fn:{} db:{} alloc_size:{} used_size:{} data size:{}",
                    thread_id_, fname_debug_only_, dbname_, allocated_size_,
                    used_size_,
                    data.size());
        memcpy(buf, data.data(), data.size());
        used_size_ += data.size();
        return Status::OK();
    }

    Status StoCWritableFileClient::Write(char *backing_mem,
                                         uint64_t offset,
                                         const Slice &data,
                                         uint64_t allocated_size,
                                         uint64_t *used_size) {
        assert(offset + data.size() < allocated_size);
        memcpy(backing_mem + offset, data.data(), data.size());
        if (offset + data.size() > *used_size) {
            *used_size = offset + data.size();
        }
        return Status::OK();
    }

    Status
    StoCWritableFileClient::Write(uint64_t offset, const leveldb::Slice &data) {
        assert(offset + data.size() < allocated_size_);
        memcpy(backing_mem_ + offset, data.data(), data.size());
        if (offset + data.size() > used_size_) {
            used_size_ = offset + data.size();
        }
        return Status::OK();
    }

    Status StoCWritableFileClient::Fsync() {
        NOVA_ASSERT(used_size_ == meta_.file_size) << fmt::format(
                    "ccremotememfile[{}]: fn:{} db:{} alloc_size:{} used_size:{}",
                    thread_id_, fname_debug_only_, dbname_, allocated_size_,
                    used_size_);
        Format();
        return Status::OK();
    }


    void StoCWritableFileClient::Format() {
        Status s;
        int file_size = used_size_;
        Slice footer_input(backing_mem_ + file_size - Footer::kEncodedLength,
                           Footer::kEncodedLength);
        Footer footer;
        s = footer.DecodeFrom(&footer_input);
        NOVA_ASSERT(s.ok()) << fmt::format("footer", s.ToString());
        // Read the index block
        BlockContents index_block_contents;
        const char *index_block_buf =
                backing_mem_ + footer.index_handle().offset();
        Slice contents(index_block_buf, footer.index_handle().size());
        StoCBlockHandle index_handle = {};
        index_handle.offset = footer.index_handle().offset();
        index_handle.size = footer.index_handle().size();
        s = Table::ReadBlock(index_block_buf, contents, ReadOptions(),
                             index_handle, &index_block_contents);
        NOVA_ASSERT(s.ok());
        index_block_ = new Block(index_block_contents,
                                 file_number_,
                                 footer.index_handle().offset(), true);
        if (num_data_blocks_ >= nova::NovaConfig::config->num_stocs_scatter_data_blocks) {
            int min_num_data_blocks_in_group =
                    num_data_blocks_ / nova::NovaConfig::config->num_stocs_scatter_data_blocks;
            int remaining = num_data_blocks_ % nova::NovaConfig::config->num_stocs_scatter_data_blocks;
            uint32_t assigned_blocks = 0;
            for (int i = 0; i < nova::NovaConfig::config->num_stocs_scatter_data_blocks; i++) {
                int nblocks = min_num_data_blocks_in_group;
                if (remaining > 0) {
                    nblocks += 1;
                    remaining -= 1;
                }
                nblocks_in_group_.push_back(nblocks);
                assigned_blocks += nblocks;
            }
            NOVA_ASSERT(assigned_blocks == num_data_blocks_);
            std::string out;
            for (auto n : nblocks_in_group_) {
                out += std::to_string(n);
                out += ",";
            }
            if (nblocks_in_group_.size() >
                nova::NovaConfig::config->num_stocs_scatter_data_blocks) {
                NOVA_ASSERT(false)
                    << fmt::format("{} {} {}", out, num_data_blocks_,
                                   min_num_data_blocks_in_group);
            }
        } else {
            nblocks_in_group_.push_back(num_data_blocks_);
        }
        Iterator *it = index_block_->NewIterator(options_.comparator);
        it->SeekToFirst();
        int n = 0;
        int offset = 0;
        int size = 0;
        int group_id = 0;
        auto client = reinterpret_cast<StoCBlockClient *> (stoc_client_);

        uint32_t num_stocs_to_select = 0;
        if (nova::NovaConfig::config->number_of_sstable_data_replicas > 1) {
            nblocks_in_group_.clear();
            nblocks_in_group_.push_back(num_data_blocks_);
            num_stocs_to_select = nova::NovaConfig::config->number_of_sstable_data_replicas;
        } else {
            num_stocs_to_select = nblocks_in_group_.size();
            if (nova::NovaConfig::config->use_parity_for_sstable_data_blocks) {
                num_stocs_to_select += 1;
            }
            num_stocs_to_select = std::max(num_stocs_to_select,
                                           nova::NovaConfig::config->number_of_sstable_metadata_replicas);
        }

        StorageSelector selector(rand_seed_);
        selector.SelectStorageServers(client,
                                      nova::NovaConfig::config->scatter_policy,
                                      num_stocs_to_select,
                                      &stocs_to_store_fragments_);
        uint32_t dbid = 0;
        nova::ParseDBIndexFromDBName(dbname_, &dbid);
        std::vector<BlockHandle> data_fragments;
        while (it->Valid()) {
            Slice key = it->key();
            Slice value = it->value();

            BlockHandle handle;
            s = handle.DecodeFrom(&value);
            // Size + crc.
            handle.set_size(handle.size() + kBlockTrailerSize);
            NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
            if (n == 0) {
                offset = handle.offset();
            }
            size += handle.size();
            n++;
            NOVA_ASSERT(offset + size == handle.offset() + handle.size());
            it->Next();

            if (n == nblocks_in_group_[group_id]) {
                for (int replica_id = 0; replica_id <
                                         nova::NovaConfig::config->number_of_sstable_data_replicas; replica_id++) {
                    uint32_t remote_stoc_id = 0;
                    if (nova::NovaConfig::config->number_of_sstable_data_replicas > 1) {
                        remote_stoc_id = stocs_to_store_fragments_[replica_id];
                    } else {
                        remote_stoc_id = stocs_to_store_fragments_[group_id];
                    }

                    uint32_t stoc_file_id = 0;
                    uint32_t req_id = client->InitiateAppendBlock(
                            remote_stoc_id, thread_id_, &stoc_file_id,
                            backing_mem_ + offset,
                            dbname_, file_number_, replica_id,
                            size, FileInternalType::kFileData);
                    BlockHandle data_fragment;
                    data_fragment.set_offset(offset);
                    data_fragment.set_size(size);
                    data_fragments.push_back(data_fragment);
                    NOVA_LOG(rdmaio::DEBUG)
                        << fmt::format(
                                "t[{}]: Initiated WRITE data blocks {} s:{} req:{} db:{} fn:{} replica:{}",
                                thread_id_, n, remote_stoc_id, req_id,
                                dbname_, file_number_, replica_id);

                    PersistStatus status = {};
                    status.remote_server_id = remote_stoc_id;
                    status.WRITE_req_id = req_id;
                    status.result_handle = {};
                    data_replica_status_[replica_id].persist_statuses.push_back(status);
                }
                n = 0;
                offset = 0;
                size = 0;
                group_id += 1;
            }
        }
        if (nova::NovaConfig::config->use_parity_for_sstable_data_blocks) {
            NOVA_ASSERT(group_id < stocs_to_store_fragments_.size());
            // figure out max size.
            // allocate memory.
            // do xor.
            for (const auto &data_fragment : data_fragments) {
                if (data_fragment.size() > parity_block_size_) {
                    parity_block_size_ = data_fragment.size();
                }
            }
            auto scid = mem_manager_->slabclassid(0, parity_block_size_);
            parity_block_backing_mem_ = mem_manager_->ItemAlloc(0, scid);
            for (int i = 0; i < parity_block_size_; i++) {
                uint8_t byte = 0;
                for (const auto &data_fragment : data_fragments) {
                    if (data_fragment.size() <= parity_block_size_) {
                        byte ^= (uint8_t) backing_mem_[data_fragment.offset() + i];
                    }
                }
                parity_block_backing_mem_[i] = byte;
            }
            uint32_t remote_stoc_id = stocs_to_store_fragments_[group_id];
            uint32_t stoc_file_id = 0;
            uint32_t req_id = client->InitiateAppendBlock(
                    remote_stoc_id, thread_id_, &stoc_file_id,
                    parity_block_backing_mem_,
                    dbname_, file_number_, 0,
                    parity_block_size_, FileInternalType::kFileParity);
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format(
                        "t[{}]: Initiated WRITE parity blocks {} s:{} req:{} db:{} fn:{} replica:{}",
                        thread_id_, parity_block_size_, remote_stoc_id, req_id,
                        dbname_, file_number_, 0);
            parity_persist_status_.remote_server_id = remote_stoc_id;
            parity_persist_status_.WRITE_req_id = req_id;
            parity_persist_status_.result_handle = {};
        }

        NOVA_ASSERT(group_id == nblocks_in_group_.size()) << fmt::format(
                    "t[{}]: {} db:{} fn:{} {} {}", thread_id_,
                    num_data_blocks_, dbname_, file_number_, group_id,
                    nblocks_in_group_.size());
        NOVA_ASSERT(n == 0)
            << fmt::format(
                    "t[{}]: {} db:{} fn:{} {} {}:{}", thread_id_,
                    num_data_blocks_, dbname_, file_number_, group_id,
                    nblocks_in_group_.size());
        delete it;
    }

    StoCBlockHandle StoCWritableFileClient::parity_block_handle() {
        return parity_persist_status_.result_handle;
    }

    void StoCWritableFileClient::Validate(const std::vector<leveldb::FileReplicaMetaData> &replicas,
                                          const StoCBlockHandle &parity_block_handle) {
        StorageSelector selector(rand_seed_);
        selector.ValidateReplicas(replicas, parity_block_handle);
    }

    std::vector<leveldb::FileReplicaMetaData>
    StoCWritableFileClient::replicas() {
        std::vector<leveldb::FileReplicaMetaData> replicas;
        replicas.resize(meta_block_handles_.size());
        for (int replica_id = 0; replica_id < meta_block_handles_.size(); replica_id++) {
            replicas[replica_id].meta_block_handle = meta_block_handles_[replica_id];
            uint32_t data_replica_id = replica_id;
            if (data_replica_status_.size() == 1) {
                data_replica_id = 0;
            }
            for (int j = 0; j < data_replica_status_[data_replica_id].persist_statuses.size(); j++) {
                replicas[replica_id].data_block_group_handles.push_back(
                        data_replica_status_[data_replica_id].persist_statuses[j].result_handle);
            }
        }
        return replicas;
    }

    void StoCWritableFileClient::WaitForPersistingDataBlocks() {
        auto client = reinterpret_cast<StoCBlockClient *> (stoc_client_);
        for (int i = 0; i < nblocks_in_group_.size() * data_replica_status_.size(); i++) {
            client->Wait();
        }
        if (nova::NovaConfig::config->use_parity_for_sstable_data_blocks) {
            client->Wait();
        }
    }

    uint32_t
    StoCWritableFileClient::Finalize() {
        auto client = reinterpret_cast<StoCBlockClient *> (stoc_client_);
        // Wait for all writes to complete.
        for (int replica_id = 0; replica_id < data_replica_status_.size(); replica_id++) {
            FileReplicaPersistStatus &status = data_replica_status_[replica_id];
            for (int i = 0; i < status.persist_statuses.size(); i++) {
                uint32_t req_id = status.persist_statuses[i].WRITE_req_id;
                StoCResponse response = {};
                NOVA_ASSERT(client->IsDone(req_id, &response, nullptr));
                NOVA_ASSERT(response.stoc_block_handles.size() == 1)
                    << fmt::format("{} {}", req_id,
                                   response.stoc_block_handles.size());
                status.persist_statuses[i].result_handle = response.stoc_block_handles[0];
            }
        }

        if (nova::NovaConfig::config->use_parity_for_sstable_data_blocks) {
            uint32_t req_id = parity_persist_status_.WRITE_req_id;
            StoCResponse response = {};
            NOVA_ASSERT(client->IsDone(req_id, &response, nullptr));
            NOVA_ASSERT(response.stoc_block_handles.size() == 1)
                << fmt::format("{} {}", req_id, response.stoc_block_handles.size());
            parity_persist_status_.result_handle = response.stoc_block_handles[0];
        }

        struct MetaBlockStatus {
            char *backing_mem = nullptr;
            uint32_t scid = 0;
            uint32_t req_id = 0;
            uint64_t new_file_size = 0;
        };
        std::vector<MetaBlockStatus> metablock_replica_status;
        std::vector<uint32_t> random_metablock_stocs;
        StorageSelector selector(rand_seed_);
        nova::ScatterPolicy scatter_policy = nova::NovaConfig::config->scatter_policy;
        if (scatter_policy != nova::ScatterPolicy::LOCAL) {
            scatter_policy = nova::ScatterPolicy::RANDOM;
        }
        selector.SelectStorageServers(client,
                                      scatter_policy,
                                      meta_block_handles_.size(),
                                      &random_metablock_stocs);

        for (int replica_id = 0; replica_id < meta_block_handles_.size(); replica_id++) {
            uint32_t stoc_id = stocs_to_store_fragments_[replica_id];
            if (nova::NovaConfig::config->number_of_sstable_data_replicas == 1) {
                stoc_id = random_metablock_stocs[replica_id];
            }
            MetaBlockStatus status = {};
            status.new_file_size = WriteMetaDataBlock(stoc_id,
                                                      replica_id,
                                                      &status.backing_mem,
                                                      &status.scid,
                                                      &status.req_id);
            metablock_replica_status.push_back(status);
        }
        for (int replica_id = 0; replica_id < meta_block_handles_.size(); replica_id++) {
            client->Wait();
        }
        uint64_t new_file_size = 0;
        for (int replica_id = 0; replica_id < meta_block_handles_.size(); replica_id++) {
            StoCResponse response = {};
            NOVA_ASSERT(client->IsDone(metablock_replica_status[replica_id].req_id, &response, nullptr));
            NOVA_ASSERT(response.stoc_block_handles.size() == 1)
                << fmt::format("{} {}",
                               metablock_replica_status[replica_id].req_id,
                               response.stoc_block_handles.size());
            meta_block_handles_[replica_id] = response.stoc_block_handles[0];
            const MetaBlockStatus &status = metablock_replica_status[replica_id];
            mem_manager_->FreeItem(0, status.backing_mem, status.scid);
            new_file_size = status.new_file_size;
        }
        NOVA_ASSERT(new_file_size != 0);
        return new_file_size;
    }

    uint64_t
    StoCWritableFileClient::WriteMetaDataBlock(uint32_t stoc_id,
                                               uint32_t replica_id,
                                               char **allocated_mem,
                                               uint32_t *allocated_scid,
                                               uint32_t *req_id) {
        auto client = reinterpret_cast<StoCBlockClient *> (stoc_client_);
        Status s;
        int file_size = used_size_;
        Slice footer_input(backing_mem_ + file_size - Footer::kEncodedLength,
                           Footer::kEncodedLength);
        Footer footer;
        s = footer.DecodeFrom(&footer_input);
        NOVA_ASSERT(s.ok()) << fmt::format("footer", s.ToString());
        Options opt(options_);
        BlockBuilder index_block_builder(&opt);
        Iterator *it = index_block_->NewIterator(options_.comparator);
        it->SeekToFirst();
        uint32_t data_replica_id = replica_id;
        if (data_replica_status_.size() == 1) {
            data_replica_id = 0;
        }
        const auto &replica = data_replica_status_[data_replica_id];
        StoCBlockHandle current_block_handle = replica.persist_statuses[0].result_handle;
        StoCBlockHandle index_handle = current_block_handle;
        uint64_t relative_offset = 0;
        int group_id = 0;
        int n = 0;
        char handle_buf[StoCBlockHandle::HandleSize()];
        uint64_t filter_block_start_offset = 0;
        while (it->Valid()) {
            Slice key = it->key();
            Slice value = it->value();
            BlockHandle handle;
            s = handle.DecodeFrom(&value);
            NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
            if (n == 0) {
                relative_offset = handle.offset();
            }
            filter_block_start_offset =
                    handle.offset() + handle.size() + kBlockTrailerSize;
            index_handle.offset =
                    (handle.offset() - relative_offset) +
                    current_block_handle.offset;
            // Does not include crc.
            index_handle.size = handle.size();
            index_handle.EncodeHandle(handle_buf);
            index_block_builder.Add(key, Slice(handle_buf,
                                               StoCBlockHandle::HandleSize()));
            it->Next();
            n++;
            if (n == nblocks_in_group_[group_id]) {
                // Cover the block handle in the StoC file.
                NOVA_ASSERT(current_block_handle.offset +
                            current_block_handle.size ==
                            index_handle.offset + index_handle.size +
                            kBlockTrailerSize);
                group_id++;
                n = 0;
                relative_offset = 0;
                if (group_id == replica.persist_statuses.size()) {
                    NOVA_ASSERT(!it->Valid());
                    break;
                }
                current_block_handle = replica.persist_statuses[group_id].result_handle;
                index_handle = current_block_handle;
            }
        }
        NOVA_ASSERT(n == 0)
            << fmt::format("Contain {} data blocks. Read {} data blocks",
                           num_data_blocks_, n);
        // Rewrite index handle after filter block.
        uint32_t filter_block_size =
                footer.metaindex_handle().offset() - filter_block_start_offset -
                kBlockTrailerSize;
        uint64_t new_file_size = filter_block_size + kBlockTrailerSize;
        // point to start of filter block.
        const uint64_t rewrite_start_offset = filter_block_start_offset;

        uint64_t metablock_size =
                used_size_ - rewrite_start_offset + METABLOCK_SIZE_PADDING;
        uint32_t scid = mem_manager_->slabclassid(0, metablock_size);
        char *backing_mem = mem_manager_->ItemAlloc(0, scid);
        *allocated_mem = backing_mem;
        *allocated_scid = scid;

        uint64_t allocated_size = metablock_size;
        uint64_t used_size = 0;
        // Copy filter block.
        memcpy(backing_mem, backing_mem_ + rewrite_start_offset, new_file_size);

        BlockHandle new_filter_handle = {};
        new_filter_handle.set_offset(0);
        new_filter_handle.set_size(filter_block_size);
        BlockHandle new_metaindex_handle = {};
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
                                       new_file_size, backing_mem,
                                       allocated_size, &used_size);
            new_metaindex_handle.set_offset(new_file_size);
            new_metaindex_handle.set_size(size - kBlockTrailerSize);
            new_file_size += size;
        }
        //Rewrite index block.
        {
            uint32_t size = WriteBlock(&index_block_builder,
                                       new_file_size, backing_mem,
                                       allocated_size, &used_size);
            new_idx_handle.set_offset(new_file_size);
            new_idx_handle.set_size(size - kBlockTrailerSize);
            new_file_size += size;
        }
        // Add new footer.
        Footer new_footer;
        new_footer.set_metaindex_handle(new_metaindex_handle);
        new_footer.set_index_handle(new_idx_handle);
        std::string new_footer_encoding;
        new_footer.EncodeTo(&new_footer_encoding);
        Write(backing_mem, new_file_size, new_footer_encoding,
              allocated_size, &used_size);
        new_file_size += new_footer_encoding.size();
        NOVA_ASSERT(rewrite_start_offset + new_file_size < allocated_size_);
        NOVA_LOG(rdmaio::DEBUG) << fmt::format(
                    "New SSTable {} size:{} old-start-offset:{} filter-block-size:{} meta_index_block:{}:{}. index_handle:{}:{}",
                    fname_debug_only_, new_file_size, rewrite_start_offset,
                    filter_block_size,
                    new_metaindex_handle.offset(), new_metaindex_handle.size(),
                    new_idx_handle.offset(), new_idx_handle.size());
        WritableFile *writable_file;
        EnvFileMetadata meta = {};
        s = mem_env_->NewWritableFile(TableFileName(dbname_, file_number_, FileInternalType::kFileData, replica_id),
                                      meta, &writable_file);
        NOVA_ASSERT(s.ok());
        Slice meta_sstable(backing_mem, new_file_size);
        s = writable_file->Append(meta_sstable);
        NOVA_ASSERT(s.ok());
        s = writable_file->Flush();
        NOVA_ASSERT(s.ok());
        s = writable_file->Sync();
        NOVA_ASSERT(s.ok());
        s = writable_file->Close();
        NOVA_ASSERT(s.ok());
        delete writable_file;
        writable_file = nullptr;
        {
            *req_id = client->InitiateAppendBlock(stoc_id,
                                                  thread_id_,
                                                  nullptr,
                                                  backing_mem,
                                                  dbname_,
                                                  file_number_,
                                                  replica_id,
                                                  new_file_size,
                                                  FileInternalType::kFileMetadata);
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format(
                        "t[{}]: Initiated WRITE meta blocks s:{} req:{} db:{} fn:{} replica:{}",
                        thread_id_, stoc_id, *req_id, dbname_, file_number_,
                        replica_id);
        }
        return new_file_size;
    }

    uint32_t
    StoCWritableFileClient::WriteBlock(BlockBuilder *block, uint64_t offset,
                                       char *backing_mem,
                                       uint64_t allocated_size,
                                       uint64_t *used_size) {
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
        uint32_t size = WriteRawBlock(block_contents, type, offset, backing_mem,
                                      allocated_size, used_size);
        block->Reset();
        return size;
    }

    uint32_t StoCWritableFileClient::WriteRawBlock(const Slice &block_contents,
                                                   CompressionType type,
                                                   uint64_t offset,
                                                   char *backing_mem,
                                                   uint64_t allocated_size,
                                                   uint64_t *used_size) {
        Write(backing_mem, offset, block_contents, allocated_size, used_size);
        char trailer[kBlockTrailerSize];
        trailer[0] = type;
        uint32_t crc = crc32c::Value(block_contents.data(),
                                     block_contents.size());
        crc = crc32c::Extend(crc, trailer,
                             1);  // Extend crc to cover block type
        // Make sure the last byte is not 0.
        trailer[kBlockTrailerSize - 1] = '!';
        EncodeFixed32(trailer + 1, crc32c::Mask(crc));
        Write(backing_mem, offset + block_contents.size(),
              Slice(trailer, kBlockTrailerSize), allocated_size, used_size);
        return block_contents.size() + kBlockTrailerSize;
    }


    StoCRandomAccessFileClientImpl::StoCRandomAccessFileClientImpl(
            Env *env, const Options &options, const std::string &dbname,
            uint64_t file_number, uint32_t replica_id,
            const leveldb::FileMetaData *meta,
            leveldb::StoCClient *stoc_client,
            leveldb::MemManager *mem_manager,
            uint64_t thread_id, bool prefetch_all, std::string &filename)
            : env_(env),
              dbname_(dbname),
              file_number_(file_number),
              meta_(meta),
              mem_manager_(mem_manager),
              thread_id_(thread_id),
              prefetch_all_(prefetch_all),
              filename(filename) {
        if (prefetch_all) {
            NOVA_LOG(rdmaio::DEBUG) << fmt::format("create file {}", filename);
        }
        NOVA_ASSERT(mem_manager_);
        nova::ParseDBIndexFromDBName(dbname, &dbid_);
        Status s;
        auto stoc_block_client = reinterpret_cast<leveldb::StoCBlockClient *>(stoc_client);
        NOVA_ASSERT(stoc_block_client);
        {
            auto metafile = TableFileName(dbname, file_number, FileInternalType::kFileData, replica_id);
            if (!env_->FileExists(metafile)) {
                NOVA_LOG(rdmaio::INFO)
                    << fmt::format("Fetch missing metadata db:{} fd:{} file {}", dbname, file_number,
                                   meta->DebugString());
                std::vector<const FileMetaData *> files;
                files.push_back(meta);
                FetchMetadataFiles(files, dbname, options, stoc_block_client, env_);
            }
            s = env_->NewRandomAccessFile(metafile, &local_ra_file_);
        }
        if (prefetch_all_) {
            NOVA_ASSERT(ReadAll(stoc_client).ok());
        }
        NOVA_ASSERT(s.ok()) << s.ToString();
    }

    Status StoCRandomAccessFileClientImpl::Read(
            const leveldb::ReadOptions &read_options,
            const leveldb::StoCBlockHandle &block_handle, uint64_t offset,
            size_t n, leveldb::Slice *result, char *scratch) {
        NOVA_ASSERT(scratch);
        if (block_handle.stoc_file_id == 0) {
            return local_ra_file_->Read(block_handle, offset, n, result,
                                        scratch);
        }
        // StoC handle. Read it.
        char *ptr = nullptr;
        uint64_t local_offset = 0;
        if (prefetch_all_) {
            NOVA_ASSERT(backing_mem_table_);
            uint64_t id =
                    (((uint64_t) block_handle.server_id) << 32) |
                    block_handle.stoc_file_id;
            DataBlockStoCFileLocalBuf &buf = stoc_local_offset_[id];
            local_offset =
                    buf.local_offset + (offset - buf.offset);
            ptr = &backing_mem_table_[local_offset];
            memcpy(scratch, ptr, n);
            *result = Slice(scratch, n);
        } else {
            NOVA_ASSERT(n < MAX_BLOCK_SIZE);
            char *backing_mem_block = read_options.rdma_backing_mem;
            if (block_handle.server_id == nova::NovaConfig::config->my_server_id) {
                backing_mem_block = scratch;
            }
            NOVA_ASSERT(backing_mem_block);
            auto stoc_client = reinterpret_cast<leveldb::StoCBlockClient *>(read_options.stoc_client);
            uint32_t req_id = stoc_client->InitiateReadDataBlock(
                    block_handle, offset, n, backing_mem_block, n, "", true);
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("t[{}]: CCRead req:{} start db:{} fn:{} s:{}",
                               read_options.thread_id,
                               req_id, dbid_, file_number_, n);
            stoc_client->Wait();
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("t[{}]: CCRead req:{} complete db:{} fn:{} s:{}",
                               read_options.thread_id,
                               req_id, dbid_, file_number_, n);
            NOVA_ASSERT(nova::IsRDMAWRITEComplete(backing_mem_block, n))
                << fmt::format("t[{}]: {}", read_options.thread_id, req_id);
            ptr = backing_mem_block;
            if (block_handle.server_id != nova::NovaConfig::config->my_server_id) {
                memcpy(scratch, ptr, n);
            }
            *result = Slice(scratch, n);
        }
        return Status::OK();
    }

    StoCRandomAccessFileClientImpl::~StoCRandomAccessFileClientImpl() {
        if (prefetch_all_) {
            NOVA_LOG(rdmaio::DEBUG) << fmt::format("close file {}", filename);
        }
        if (local_ra_file_) {
            delete local_ra_file_;
        }

        if (backing_mem_table_) {
            uint32_t scid = mem_manager_->slabclassid(thread_id_, meta_->file_size);
            mem_manager_->FreeItem(thread_id_, backing_mem_table_, scid);
            backing_mem_table_ = nullptr;
        }
    }

    Status StoCRandomAccessFileClientImpl::Read(
            const StoCBlockHandle &stoc_block_handle,
            uint64_t offset, size_t n,
            leveldb::Slice *result, char *scratch) {
        NOVA_ASSERT(false);
        return Status::OK();
    }

    Status StoCRandomAccessFileClientImpl::ReadAll(StoCClient *stoc_client) {
        uint32_t scid = mem_manager_->slabclassid(thread_id_, meta_->file_size);
        backing_mem_table_ = mem_manager_->ItemAlloc(thread_id_, scid);
        NOVA_ASSERT(backing_mem_table_) << "Running out of memory";
        uint64_t offset = 0;
        uint32_t reqs[meta_->block_replica_handles[0].data_block_group_handles.size()];
        auto stoc_block_client = reinterpret_cast<leveldb::StoCBlockClient *>(stoc_client);
        for (int i = 0; i <
                        meta_->block_replica_handles[0].data_block_group_handles.size(); i++) {
            const StoCBlockHandle &handle = meta_->block_replica_handles[0].data_block_group_handles[i];
            NOVA_ASSERT(offset + handle.size <= meta_->file_size);
            uint64_t id =
                    (((uint64_t) handle.server_id) << 32) | handle.stoc_file_id;
            reqs[i] = stoc_block_client->InitiateReadDataBlock(handle,
                                                               handle.offset,
                                                               handle.size,
                                                               backing_mem_table_ +
                                                               offset,
                                                               handle.size,
                                                               "", false);
            DataBlockStoCFileLocalBuf buf = {};
            buf.offset = handle.offset;
            buf.size = handle.size;
            buf.local_offset = offset;
            stoc_local_offset_[id] = buf;
            offset += handle.size;
        }
        // Wait for all reads to complete.
        for (int i = 0; i < meta_->block_replica_handles[0].data_block_group_handles.size(); i++) {
            stoc_block_client->Wait();
        }
        offset = 0;
        for (int i = 0; i <
                        meta_->block_replica_handles[0].data_block_group_handles.size(); i++) {
            const StoCBlockHandle &handle = meta_->block_replica_handles[0].data_block_group_handles[i];
            NOVA_ASSERT(nova::IsRDMAWRITEComplete(backing_mem_table_ + offset, handle.size));
            offset += handle.size;
        }
        return Status::OK();
    }
}