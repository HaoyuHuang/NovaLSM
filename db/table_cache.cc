// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <fmt/core.h>
#include "db/table_cache.h"

#include "ltc/stoc_file_client_impl.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

    struct TableAndFile {
        RandomAccessFile *file = nullptr;
        Table *table = nullptr;
    };

    static void DeleteEntry(const Slice &key, void *value) {
        TableAndFile *tf = reinterpret_cast<TableAndFile *>(value);
        delete tf->table;
        delete tf->file;
        delete tf;
    }

    static void UnrefEntry(void *arg1, void *arg2) {
        Cache *cache = reinterpret_cast<Cache *>(arg1);
        Cache::Handle *h = reinterpret_cast<Cache::Handle *>(arg2);
        cache->Release(h);
    }

    TableCache::TableCache(const std::string &dbname, const Options &options,
                           int entries, DBProfiler *db_profiler)
            : env_(options.env),
              dbname_(dbname),
              options_(options),
              cache_(NewLRUCache(entries)), db_profiler_(db_profiler) {}

    TableCache::~TableCache() { delete cache_; }

//    bool
//    TableCache::IsTableCached(AccessCaller caller, const FileMetaData *meta) {
//        Status s;
//        char buf[1 + sizeof(meta->number)];
//
//        if (caller == AccessCaller::kCompaction) {
//            buf[0] = 'c';
//        } else {
//            buf[0] = 'u';
//        }
//        EncodeFixed64(buf + 1, meta->number);
//        Slice key(buf, 1 + sizeof(meta->number));
//        auto *handle = cache_->Lookup(key);
//        if (handle) {
//            cache_->Release(handle);
//            return true;
//        }
//        return false;
//    }

    Status
    TableCache::FindTable(AccessCaller caller, const ReadOptions &options,
                          const FileMetaData *meta,
                          uint64_t file_number, uint32_t replica_id,
                          uint64_t file_size, int level,
                          Cache::Handle **handle, bool force_insert) {
        Status s;
        char buf[1 + 8 + 4];
        if (caller == AccessCaller::kCompaction) {
            buf[0] = 'c';
        } else {
            buf[0] = 'u';
        }
        EncodeFixed64(buf + 1, file_number);
        EncodeFixed32(buf + 9, replica_id);
        Slice key(buf, 1 + 8 + 4);
        StoCRandomAccessFileClientImpl *file = nullptr;
        if (force_insert) {
            Table *table = nullptr;
            bool prefetch_all = false;
            if (caller == AccessCaller::kCompaction) {
                prefetch_all = true;
            }
            std::string filename = TableFileName(dbname_, file_number, FileInternalType::kFileData,
                                                 replica_id);
            file = new StoCRandomAccessFileClientImpl(env_, options_, dbname_,
                                                      file_number,
                                                      replica_id,
                                                      meta,
                                                      options.stoc_client,
                                                      options.mem_manager,
                                                      options.thread_id,
                                                      prefetch_all,
                                                      filename);
            s = Table::Open(options_, options, meta, file, file_size, level,
                            file_number, replica_id, &table, db_profiler_);
            NOVA_ASSERT(s.ok())
                << fmt::format("file:{} status:{}", meta->DebugString(),
                               s.ToString());
            TableAndFile *tf = new TableAndFile;
            tf->file = file;
            tf->table = table;
            *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
            return s;
        }

        auto fname = TableFileName(dbname_, file_number, FileInternalType::kFileData, 0);
        if (nova::NovaConfig::config->cfgs.size() > 1) {
            NOVA_ASSERT(env_->LockFile(fname, file_number).ok());
        }
        *handle = cache_->Lookup(key);
        bool cache_hit = true;
        if (*handle) {
            cache_hit = true;
            // Check if the file is deleted.
            TableAndFile *tf = reinterpret_cast<TableAndFile *>(cache_->Value(*handle));
            file = dynamic_cast<StoCRandomAccessFileClientImpl *>(tf->file);
        } else {
            cache_hit = false;
        }

        if (!cache_hit) {
            Table *table = nullptr;
            bool prefetch_all = false;
            if (caller == AccessCaller::kCompaction) {
                prefetch_all = true;
            }
            std::string filename = TableFileName(dbname_, file_number, FileInternalType::kFileData, replica_id);
            file = new StoCRandomAccessFileClientImpl(env_, options_, dbname_,
                                                      file_number,
                                                      replica_id,
                                                      meta,
                                                      options.stoc_client,
                                                      options.mem_manager,
                                                      options.thread_id,
                                                      prefetch_all,
                                                      filename);
            s = Table::Open(options_, options, meta, file, file_size, level,
                            file_number, replica_id, &table, db_profiler_);
            NOVA_ASSERT(s.ok())
                << fmt::format("file:{} status:{}", meta->DebugString(),
                               s.ToString());
            TableAndFile *tf = new TableAndFile;
            tf->file = file;
            tf->table = table;
            *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
        }
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("table cache hit {} fn:{} cs:{} ltc:{}", cache_hit,
                           file_number, cache_->TotalCharge(),
                           cache_->TotalCapacity());
        if (nova::NovaConfig::config->cfgs.size() > 1) {
            NOVA_ASSERT(env_->UnlockFile(fname, file_number).ok());
        }
        return s;
    }

    Iterator *
    TableCache::NewIterator(AccessCaller caller, const ReadOptions &options,
                            const FileMetaData *meta,
                            uint64_t file_number, uint32_t replica_id,
                            int level, uint64_t file_size, Table **tableptr) {
        if (tableptr != nullptr) {
            *tableptr = nullptr;
        }
        Cache::Handle *handle = nullptr;
        Status s = FindTable(caller, options, meta, file_number, replica_id,
                             file_size, level, &handle);
        if (!s.ok()) {
            return NewErrorIterator(s);
        }
        Table *table = reinterpret_cast<TableAndFile *>(cache_->Value(
                handle))->table;

        Iterator *result = table->NewIterator(caller, options);
        result->RegisterCleanup(&UnrefEntry, cache_, handle);
        if (tableptr != nullptr) {
            *tableptr = table;
        }
        return result;
    }

    Status TableCache::Get(const ReadOptions &options, const FileMetaData *meta,
                           uint64_t file_number, uint32_t replica_id,
                           uint64_t file_size, int level,
                           const Slice &k, void *arg,
                           void (*handle_result)(void *, const Slice &,
                                                 const Slice &)) {
        Cache::Handle *handle = nullptr;
        Status s = FindTable(AccessCaller::kUserGet, options, meta, file_number,
                             replica_id, file_size, level, &handle);
        if (s.ok()) {
            TableAndFile *tf = reinterpret_cast<TableAndFile *>(cache_->Value(
                    handle));
            Table *t = tf->table;
            s = t->InternalGet(options, k, arg, handle_result);
            cache_->Release(handle);
        }
        return s;
    }

    void TableCache::Evict(uint64_t file_number, bool compaction_file_only) {
        char buf[1 + 8 + 4];
        buf[0] = 'c';
        for (int replica_id = 0; replica_id <
                                 nova::NovaConfig::config->number_of_sstable_metadata_replicas; replica_id++) {
            EncodeFixed64(buf + 1, file_number);
            EncodeFixed32(buf + 9, replica_id);
            cache_->Erase(Slice(buf, 1 + 8 + 4));
        }

        if (compaction_file_only) {
            return;
        }

        buf[0] = 'u';
        for (int replica_id = 0; replica_id <
                                 nova::NovaConfig::config->number_of_sstable_metadata_replicas; replica_id++) {
            EncodeFixed64(buf + 1, file_number);
            EncodeFixed32(buf + 9, replica_id);
            cache_->Erase(Slice(buf, 1 + 8 + 4));
        }
    }

}  // namespace leveldb
