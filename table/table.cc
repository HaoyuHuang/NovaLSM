// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/crc32c.h"
#include <fmt/core.h>
#include <db/dbformat.h>
#include <nova/logging.hpp>
#include "leveldb/table.h"

#include "nova/logging.hpp"

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
        RandomAccessFile *file;
        uint64_t cache_id;
        FilterBlockReader *filter;
        const char *filter_data;
        int level;
        uint64_t file_number;

        BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
        Block *index_block;
    };

    Status Table::Open(const Options &options, const ReadOptions &read_options,
                       RandomAccessFile *file,
                       uint64_t size, int level,
                       uint64_t file_number, Table **table,
                       DBProfiler *db_profiler) {
        *table = nullptr;
        if (size < Footer::kEncodedLength) {
            return Status::Corruption("file is too short to be an sstable");
        }

        char footer_space[Footer::kEncodedLength];
        Slice footer_input;
        RTableHandle h = {};
        h.offset = size - Footer::kEncodedLength;
        h.size = Footer::kEncodedLength;

        auto f = reinterpret_cast<CCRandomAccessFile *>(file);
        Status s = f->Read(read_options, h, size - Footer::kEncodedLength,
                           Footer::kEncodedLength,
                           &footer_input, footer_space);
        if (!s.ok()) return s;

        Footer footer;
        s = footer.DecodeFrom(&footer_input);
        if (!s.ok()) return s;

        // Read the index block
        *table = new Table();
        BlockContents index_block_contents;
        if (s.ok()) {
            RTableHandle h = {};
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
            rep->metaindex_handle = footer.metaindex_handle();
            rep->index_block = index_block;
            rep->cache_id = (options.block_cache ? options.block_cache->NewId()
                                                 : 0);
            rep->filter_data = nullptr;
            rep->file_number = file_number;
            rep->level = level;
            rep->filter = nullptr;
            (*table)->rep_ = rep;
            (*table)->ReadMeta(footer);
            (*table)->db_profiler_ = db_profiler;

//            auto it = index_block->NewIterator(options.comparator);
//            it->SeekToFirst();
//            while (it->Valid()) {
//                Slice key = it->key();
//                Slice value = it->value();
//
//                leveldb::ParsedInternalKey ikey;
//                leveldb::ParseInternalKey(key, &ikey);
//                RTableHandle handle;
//                handle.DecodeHandle(value.data());
//                RDMA_LOG(rdmaio::DEBUG)
//                    << fmt::format("key:{} handle:{} {} {} {}",
//                                   ikey.user_key.ToString(), handle.server_id,
//                                   handle.rtable_id, handle.offset,
//                                   handle.size);
//                it->Next();
//            }
//            delete it;

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
        RTableHandle h = {};
        h.offset = footer.metaindex_handle().offset();
        h.size = footer.metaindex_handle().size();
        if (!ReadBlock(rep_->file, opt, h, &contents).ok()) {
            // Do not propagate errors since meta info is not needed for operation
            return;
        }
        Block *meta = new Block(contents,
                                rep_->file_number,
                                footer.metaindex_handle().offset());

        Iterator *iter = meta->NewIterator(BytewiseComparator());
        std::string key = "filter.";
        key.append(rep_->options.filter_policy->Name());
        iter->Seek(key);
        if (iter->Valid() && iter->key() == Slice(key)) {
            ReadFilter(iter->value());
        }
        delete iter;
        delete meta;
    }

    void Table::ReadFilter(const Slice &filter_handle_value) {
        Slice v = filter_handle_value;
        BlockHandle filter_handle;
        if (!filter_handle.DecodeFrom(&v).ok()) {
            return;
        }

        // We might want to unify with ReadBlock() if we start
        // requiring checksum verification in Table::Open.
        ReadOptions opt;
        if (rep_->options.paranoid_checks) {
            opt.verify_checksums = true;
        }
        BlockContents block;
        RTableHandle h = {};
        h.offset = filter_handle.offset();
        h.size = filter_handle.size();
        if (!ReadBlock(rep_->file, opt, h, &block).ok()) {
            return;
        }
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
    Iterator *Table::DataBlockReader(void *arg, BlockReadContext context,
                                     const ReadOptions &options,
                                     const Slice &index_value) {
        Table *table = reinterpret_cast<Table *>(arg);
        Cache *block_cache = table->rep_->options.block_cache;
        Block *block = nullptr;
        Cache::Handle *cache_handle = nullptr;

        // It is an RTableHandle.
        RTableHandle rtable_handle = {};
        Slice input = index_value;
        Status s;
        rtable_handle.DecodeHandle(input.data());
        // We intentionally allow extra stuff in index_value so that we
        // can add more features in the future.
        bool cache_hit = false;
        bool insert = false;
        BlockContents contents;
        if (block_cache != nullptr) {
            char cache_key_buffer[8 + RTableHandle::HandleSize()];
            EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
            rtable_handle.EncodeHandle(cache_key_buffer + 8);
            Slice key(cache_key_buffer, sizeof(cache_key_buffer));
            cache_handle = block_cache->Lookup(key);
            if (cache_handle != nullptr) {
                block = reinterpret_cast<Block *>(block_cache->Value(
                        cache_handle));
                cache_hit = true;
            } else {
                s = table->ReadBlock(table->rep_->file, options, rtable_handle,
                                     &contents);
                if (s.ok()) {
                    block = new Block(contents, table->rep_->file_number,
                                      rtable_handle.offset);
                    if (contents.cachable && options.fill_cache) {
                        cache_handle = block_cache->Insert(key, block,
                                                           block->size(),
                                                           &DeleteCachedBlock);
                        insert = true;
                    }
                }
            }
        } else {
            s = table->ReadBlock(table->rep_->file, options, rtable_handle,
                                 &contents);
            if (s.ok()) {
                block = new Block(contents, table->rep_->file_number,
                                  rtable_handle.offset);
            }
        }

        RDMA_ASSERT(s.ok()) <<
                            fmt::format(
                                    "{} Cache hit {} Insert {} cs:{} cc:{} fn:{} rs:{} rr:{} roff:{} rsize:{}",
                                    s.ToString(),
                                    cache_hit,
                                    insert, block_cache->TotalCharge(),
                                    block_cache->TotalCapacity(),
                                    table->rep_->file_number,
                                    rtable_handle.server_id,
                                    rtable_handle.rtable_id,
                                    rtable_handle.offset, rtable_handle.size);

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
                &Table::DataBlockReader, const_cast<Table *>(this), options);
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
        Iterator *iiter = rep_->index_block->NewIterator(
                rep_->options.comparator);
        iiter->Seek(k);
        if (iiter->Valid()) {
            Slice handle_value = iiter->value();
            FilterBlockReader *filter = rep_->filter;
            BlockHandle handle;
            bool found = true;
            // TODO: Support filter block.
//            if (filter != nullptr && handle.DecodeFrom(&handle_value).ok()) {
//                // Not found
//                if (db_profiler_ != nullptr) {
//                    Access access = {
//                            .trace_type = TraceType::FILTER_BLOCK,
//                            .access_caller = AccessCaller::kUserGet,
//                            .block_id = 0,
//                            .sstable_id = rep_->file_number,
//                            .level = rep_->level,
//                            .size = rep_->filter->size()
//                    };
//                    db_profiler_->Trace(access);
//                }
//
//                if (!filter->KeyMayMatch(handle.offset(), k)) {
//                    found = false;
//                }
//            }
            if (found) {
                BlockReadContext context{
                        .caller = AccessCaller::kUserGet,
                        .file_number = rep_->file_number,
                        .level = rep_->level
                };
                Iterator *block_iter = DataBlockReader(this, context,
                                                       options,
                                                       iiter->value());
                block_iter->Seek(k);
                if (block_iter->Valid()) {
                    (*handle_result)(arg, block_iter->key(),
                                     block_iter->value());
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
                     const RTableHandle &handle, BlockContents *result) {
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

        switch (data[n]) {
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
                return Status::Corruption("bad block type");
        }
        return Status::OK();
    }

    Status Table::ReadBlock(leveldb::RandomAccessFile *file,
                            const leveldb::ReadOptions &options,
                            const RTableHandle &rtable_handle,
                            leveldb::BlockContents *result) {
        result->data = Slice();
        result->cachable = false;
        result->heap_allocated = false;

        // Read the block contents as well as the type/crc footer.
        // See table_builder.cc for the code that built this structure.
        size_t n = static_cast<size_t>(rtable_handle.size);
        char *buf = new char[n + kBlockTrailerSize];
        Slice contents;
        auto f = reinterpret_cast<CCRandomAccessFile *>(file);
        Status s = f->Read(options, rtable_handle, rtable_handle.offset,
                           n + kBlockTrailerSize, &contents,
                           buf);
        if (!s.ok()) {
            delete[] buf;
            return s;
        }
        if (contents.size() != n + kBlockTrailerSize) {
            delete[] buf;
            return Status::Corruption("truncated block read");
        }
        return ReadBlock(buf, contents, options, rtable_handle, result);
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
