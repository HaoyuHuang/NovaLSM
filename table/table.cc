// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/crc32c.h"
#include <fmt/core.h>
#include <db/dbformat.h>
#include <common/nova_console_logging.h>
#include <unordered_map>
#include "leveldb/table.h"

#include "common/nova_console_logging.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

    struct Table::Rep {
        ~Rep() {
            delete filter;
            delete[] filter_data;
            delete index_block;
        }

        Options options;
        Status status;
        const FileMetaData *meta;
        RandomAccessFile *file;
        uint64_t cache_id;
        uint32_t replica_id;
        FilterBlockReader *filter;
        const char *filter_data;
        int level;
        uint32_t db_index;
        uint64_t file_number;
        std::unordered_map<uint64_t, uint64_t> stoc_file_data_relative_offset;

        BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
        Block *index_block;
    };

    Status Table::Open(const Options &options,
                       const ReadOptions &read_options,
                       const FileMetaData *meta,
                       RandomAccessFile *file,
                       uint64_t size, int level,
                       uint64_t file_number, uint32_t replica_id, Table **table,
                       DBProfiler *db_profiler) {
        *table = nullptr;
        if (size < Footer::kEncodedLength) {
            return Status::Corruption("file is too short to be an sstable");
        }

        char footer_space[Footer::kEncodedLength];
        Slice footer_input;
        StoCBlockHandle h = {};
        h.offset = size - Footer::kEncodedLength;
        h.size = Footer::kEncodedLength;

        auto f = reinterpret_cast<StoCRandomAccessFileClient *>(file);
        Status s = f->Read(read_options, h, size - Footer::kEncodedLength,
                           Footer::kEncodedLength,
                           &footer_input, footer_space);
        NOVA_ASSERT(footer_input.size() == Footer::kEncodedLength)
            << fmt::format("{} {} {} {} {}", size, h.DebugString(), footer_input.size(),
                           Footer::kEncodedLength, meta->DebugString());
        if (!s.ok()) return s;

        Footer footer;
        s = footer.DecodeFrom(&footer_input);
        if (!s.ok()) return s;

        // Read the index block
        *table = new Table();
        BlockContents index_block_contents;
        if (s.ok()) {
            StoCBlockHandle h = {};
            h.offset = footer.index_handle().offset();
            h.size = footer.index_handle().size();
            s = (*table)->ReadBlock(file, read_options, h,
                                    &index_block_contents);
        }

        if (s.ok()) {
            // We've successfully read the footer and the index block: we're
            // ready to serve requests.
            Block *index_block = new Block(index_block_contents,
                                           file_number,
                                           footer.index_handle().offset());
            Rep *rep = new Table::Rep;
            rep->options = options;
            rep->file = file;
            rep->meta = meta;
            rep->metaindex_handle = footer.metaindex_handle();
            rep->index_block = index_block;
            rep->replica_id = replica_id;
            rep->cache_id = (options.block_cache ? options.block_cache->NewId()
                                                 : 0);
            rep->filter_data = nullptr;
            rep->file_number = file_number;
            rep->level = level;
            rep->filter = nullptr;
            (*table)->rep_ = rep;
            (*table)->ReadMeta(footer);
            (*table)->db_profiler_ = db_profiler;
            uint64_t offset = 0;
            for (const auto &handle : meta->block_replica_handles[replica_id].data_block_group_handles) {
                auto sid = static_cast<uint64_t>(handle.server_id);
                uint64_t id = (sid << 32) | handle.stoc_file_id;
                NOVA_ASSERT(rep->stoc_file_data_relative_offset.find(id) ==
                            rep->stoc_file_data_relative_offset.end());
                rep->stoc_file_data_relative_offset[id] = offset;
                offset += handle.size;
            }
        }
        return s;
    }

    void Table::ReadMeta(const Footer &footer) {
        if (rep_->options.filter_policy == nullptr) {
            return;  // Do not need any metadata
        }

        // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
        // it is an empty block.
        ReadOptions opt;
        if (rep_->options.paranoid_checks) {
            opt.verify_checksums = true;
        }
        BlockContents contents;
        StoCBlockHandle h = {};
        h.offset = footer.metaindex_handle().offset();
        h.size = footer.metaindex_handle().size();
        NOVA_ASSERT(ReadBlock(rep_->file, opt, h, &contents).ok());
        Block *meta = new Block(contents,
                                rep_->file_number,
                                footer.metaindex_handle().offset());

        Iterator *iter = meta->NewIterator(BytewiseComparator());
        std::string key = "filter.";
        key.append(rep_->options.filter_policy->Name());
        iter->Seek(key);
        NOVA_ASSERT(iter->Valid() && iter->key() == Slice(key));
        ReadFilter(iter->value());
        delete iter;
        delete meta;
    }

    void Table::ReadFilter(const Slice &filter_handle_value) {
        Slice v = filter_handle_value;
        BlockHandle filter_handle;
        NOVA_ASSERT(filter_handle.DecodeFrom(&v).ok());
        // We might want to unify with ReadBlock() if we start
        // requiring checksum verification in Table::Open.
        ReadOptions opt;
        if (rep_->options.paranoid_checks) {
            opt.verify_checksums = true;
        }
        BlockContents block;
        StoCBlockHandle h = {};
        h.offset = filter_handle.offset();
        h.size = filter_handle.size();
        NOVA_ASSERT(ReadBlock(rep_->file, opt, h, &block).ok());
        if (block.heap_allocated) {
            rep_->filter_data = block.data.data();  // Will need to delete later
        }
        rep_->filter = new FilterBlockReader(rep_->options.filter_policy,
                                             block.data);
    }

    Table::~Table() { delete rep_; }

    static void DeleteBlock(void *arg, void *ignored) {
        delete reinterpret_cast<Block *>(arg);
    }

    static void DeleteCachedBlock(const Slice &key, void *value) {
        Block *block = reinterpret_cast<Block *>(value);
        delete block;
    }

    static void ReleaseBlock(void *arg, void *h) {
        Cache *cache = reinterpret_cast<Cache *>(arg);
        Cache::Handle *handle = reinterpret_cast<Cache::Handle *>(h);
        cache->Release(handle);
    }

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
    Iterator *
    Table::DataBlockReader(void *arg, void *arg2, BlockReadContext context,
                           const ReadOptions &options,
                           const Slice &index_value,
                           std::string *next_key) {
        Table *table = reinterpret_cast<Table *>(arg);
        Cache *block_cache = table->rep_->options.block_cache;
        Block *block = nullptr;
        Cache::Handle *cache_handle = nullptr;
        StoCBlockHandle stoc_block_handle = {};
        Slice input = index_value;
        Status s;
        stoc_block_handle.DecodeHandle(input.data());
        // One replica with one data fragment.
        // Overwrite with latest server id and stoc file id.
        if (table->rep_->meta->block_replica_handles.size() == 1 &&
            table->rep_->meta->block_replica_handles[0].data_block_group_handles.size() == 1) {
            stoc_block_handle.server_id = table->rep_->meta->block_replica_handles[0].data_block_group_handles[0].server_id;
            stoc_block_handle.stoc_file_id = table->rep_->meta->block_replica_handles[0].data_block_group_handles[0].stoc_file_id;
        }

        // We intentionally allow extra stuff in index_value so that we
        // can add more features in the future.
        bool cache_hit = false;
        bool insert = false;
        BlockContents contents;
        if (block_cache != nullptr) {
            char cache_key_buffer[8 + StoCBlockHandle::HandleSize()];
            EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
            stoc_block_handle.EncodeHandle(cache_key_buffer + 8);
            Slice key(cache_key_buffer, sizeof(cache_key_buffer));
            cache_handle = block_cache->Lookup(key);
            if (cache_handle != nullptr) {
                block = reinterpret_cast<Block *>(block_cache->Value(
                        cache_handle));
                cache_hit = true;
            } else {
                s = table->ReadBlock(table->rep_->file, options,
                                     stoc_block_handle,
                                     &contents);
                if (s.ok()) {
                    block = new Block(contents, table->rep_->file_number,
                                      stoc_block_handle.offset);
                    if (contents.cachable && options.fill_cache) {
                        cache_handle = block_cache->Insert(key, block,
                                                           block->size(),
                                                           &DeleteCachedBlock);
                        insert = true;
                    }
                }
            }
        } else {
            s = table->ReadBlock(table->rep_->file, options, stoc_block_handle, &contents);
            if (s.ok()) {
                block = new Block(contents, table->rep_->file_number, stoc_block_handle.offset);
            }
        }

        NOVA_ASSERT(s.ok())
            <<
            fmt::format(
                    "{} Cache hit {} Insert {} fn:{} rs:{} rr:{} roff:{} rsize:{} meta:{}",
                    s.ToString(),
                    cache_hit,
                    insert,
                    table->rep_->file_number,
                    stoc_block_handle.server_id,
                    stoc_block_handle.stoc_file_id,
                    stoc_block_handle.offset, stoc_block_handle.size, table->rep_->meta->DebugString());

        if (table->db_profiler_ != nullptr) {
            Access access = {
                    .trace_type = TraceType::DATA_BLOCK,
                    .access_caller = context.caller,
                    .block_id = block->block_id(),
                    .sstable_id = block->file_number(),
                    .level = table->rep_->level,
                    .size = block->size()
            };
            table->db_profiler_->Trace(access);
        }

        Iterator *iter;
        if (block != nullptr) {
            iter = block->NewIterator(table->rep_->options.comparator);
            if (cache_handle == nullptr) {
                iter->RegisterCleanup(&DeleteBlock, block, nullptr);
            } else {
                iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
            }
        } else {
            iter = NewErrorIterator(s);
        }
        return iter;
    }

    Iterator *
    Table::NewIterator(AccessCaller caller, const ReadOptions &options) const {
        BlockReadContext context = {
                .caller = caller,
                .file_number = rep_->file_number,
                .level = rep_->level,
        };

        // Access index block.
        if (db_profiler_ != nullptr) {
            Access access = {
                    .trace_type = TraceType::INDEX_BLOCK,
                    .access_caller = caller,
                    .block_id = rep_->index_block->block_id(),
                    .sstable_id = rep_->file_number,
                    .level = rep_->level,
                    .size = rep_->index_block->size(),
            };
            db_profiler_->Trace(access);
        }

        return NewTwoLevelIterator(
                rep_->index_block->NewIterator(rep_->options.comparator),
                context,
                &Table::DataBlockReader, const_cast<Table *>(this), nullptr,
                options);
    }

    uint64_t Table::TranslateToDataBlockOffset(const leveldb::StoCBlockHandle &handle) {
        if (rep_->stoc_file_data_relative_offset.size() == 1) {
            return handle.offset;
        }
        uint64_t sid = handle.server_id;
        uint64_t id = (sid << 32) | handle.stoc_file_id;
        auto it = rep_->stoc_file_data_relative_offset.find(id);
        NOVA_ASSERT(it != rep_->stoc_file_data_relative_offset.end())
            << fmt::format("DB[{}]: handle:{} meta:{}", rep_->db_index, handle.DebugString(),
                           rep_->meta->DebugString());
        return it->second + handle.offset;
    }

    Status
    Table::InternalGet(const ReadOptions &options, const Slice &k, void *arg,
                       void (*handle_result)(void *, const Slice &,
                                             const Slice &)) {
        // Access index block.
        if (db_profiler_ != nullptr) {
            Access access = {
                    .trace_type = TraceType::INDEX_BLOCK,
                    .access_caller = AccessCaller::kUserGet,
                    .block_id = rep_->index_block->block_id(),
                    .sstable_id = rep_->file_number,
                    .level = rep_->level,
                    .size = rep_->index_block->size(),
            };
            db_profiler_->Trace(access);
        }

        Status s;
        Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
        iiter->Seek(k);
        if (iiter->Valid()) {
            Slice handle_value = iiter->value();
            FilterBlockReader *filter = rep_->filter;
            StoCBlockHandle handle;
            bool found = true;
            bool key_doest_not_exist = false;
            uint64_t data_block_offset = 0;
            NOVA_ASSERT(filter != nullptr);
            NOVA_ASSERT(StoCBlockHandle::DecodeHandle(&handle_value, &handle));
            // Not found
            if (db_profiler_ != nullptr) {
                Access access = {
                        .trace_type = TraceType::FILTER_BLOCK,
                        .access_caller = AccessCaller::kUserGet,
                        .block_id = 0,
                        .sstable_id = rep_->file_number,
                        .level = rep_->level,
                        .size = rep_->filter->size()
                };
                db_profiler_->Trace(access);
            }
            data_block_offset = TranslateToDataBlockOffset(handle);
            if (!filter->KeyMayMatch(data_block_offset, k)) {
                found = false;
                key_doest_not_exist = true;
            }
            if (found) {
                BlockReadContext context{
                        .caller = AccessCaller::kUserGet,
                        .file_number = rep_->file_number,
                        .level = rep_->level
                };
                Iterator *block_iter = DataBlockReader(this, nullptr, context,
                                                       options,
                                                       iiter->value(), nullptr);
                block_iter->Seek(k);
                if (block_iter->Valid()) {
                    if (handle_result) {
                        (*handle_result)(arg, block_iter->key(),
                                         block_iter->value());
                    }
                    if (BytewiseComparator()->Compare(
                            ExtractUserKey(block_iter->key()),
                            ExtractUserKey(k)) == 0 &&
                        key_doest_not_exist) {
                        NOVA_LOG(rdmaio::INFO)
                            << fmt::format(
                                    "found:{} handle:{} offset:{} k:{} key:{} debug:{} meta:{} ",
                                    found,
                                    handle.DebugString(),
                                    data_block_offset,
                                    ExtractUserKey(
                                            block_iter->key()).ToString(),
                                    ExtractUserKey(k).ToString(),
                                    filter->DebugString(data_block_offset, k),
                                    rep_->meta->DebugString());
                    }
                }
                s = block_iter->status();
                delete block_iter;
            }
        }
        if (s.ok()) {
            s = iiter->status();
        }
        delete iiter;
        return s;
    }

    Status
    Table::ReadBlock(const char *buf, const Slice &contents,
                     const ReadOptions &options,
                     const StoCBlockHandle &handle, BlockContents *result) {
        size_t n = static_cast<size_t>(handle.size);
        Status s;

        // Check the crc of the type and the block contents
        const char *data = contents.data();  // Pointer to where Read put the data
        if (options.verify_checksums) {
            const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
            const uint32_t actual = crc32c::Value(data, n + 1);
            if (actual != crc) {
                s = Status::Corruption("block checksum mismatch");
                return s;
            }
        }
        char type = data[n];
        switch (type) {
            case kNoCompression:
                if (data != buf) {
                    // File implementation gave us pointer to some other data.
                    // Use it directly under the assumption that it will be live
                    // while the file is open.
                    delete[] buf;
                    result->data = Slice(data, n);
                    result->heap_allocated = false;
                    result->cachable = false;  // Do not double-cache
                } else {
                    result->data = Slice(buf, n);
                    result->heap_allocated = true;
                    result->cachable = true;
                }

                // Ok
                break;
            case kSnappyCompression: {
                size_t ulength = 0;
                if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
                    delete[] buf;
                    return Status::Corruption(
                            "corrupted compressed block contents");
                }
                char *ubuf = new char[ulength];
                if (!port::Snappy_Uncompress(data, n, ubuf)) {
                    delete[] buf;
                    delete[] ubuf;
                    return Status::Corruption(
                            "corrupted compressed block contents");
                }
                delete[] buf;
                result->data = Slice(ubuf, ulength);
                result->heap_allocated = true;
                result->cachable = true;
                break;
            }
            default:
                delete[] buf;
                return Status::Corruption(fmt::format("bad block type {}", type));
        }
        return Status::OK();
    }

    Status Table::ReadBlock(leveldb::RandomAccessFile *file,
                            const leveldb::ReadOptions &options,
                            const StoCBlockHandle &stoc_block_handle,
                            leveldb::BlockContents *result) {
        result->data = Slice();
        result->cachable = false;
        result->heap_allocated = false;

        // Read the block contents as well as the type/crc footer.
        // See table_builder.ltc for the code that built this structure.
        size_t n = static_cast<size_t>(stoc_block_handle.size);
        char *buf = new char[n + kBlockTrailerSize];
        Slice contents;
        auto f = reinterpret_cast<StoCRandomAccessFileClient *>(file);
        Status s = f->Read(options, stoc_block_handle, stoc_block_handle.offset, n + kBlockTrailerSize, &contents, buf);
        if (!s.ok()) {
            delete[] buf;
            return s;
        }
        if (contents.size() != n + kBlockTrailerSize) {
            delete[] buf;
            return Status::Corruption("truncated block read");
        }
        return ReadBlock(buf, contents, options, stoc_block_handle, result);
    }

    uint64_t Table::ApproximateOffsetOf(const Slice &key) const {
        Iterator *index_iter =
                rep_->index_block->NewIterator(rep_->options.comparator);
        index_iter->Seek(key);
        uint64_t result;
        if (index_iter->Valid()) {
            BlockHandle handle;
            Slice input = index_iter->value();
            Status s = handle.DecodeFrom(&input);
            if (s.ok()) {
                result = handle.offset();
            } else {
                // Strange: we can't decode the block handle in the index block.
                // We'll just return the offset of the metaindex block, which is
                // close to the whole file size for this case.
                result = rep_->metaindex_handle.offset();
            }
        } else {
            // key is past the last key in the file.  Approximate the offset
            // by returning the offset of the metaindex block (which is
            // right near the end of the file).
            result = rep_->metaindex_handle.offset();
        }
        delete index_iter;
        return result;
    }

}  // namespace leveldb
