// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

    void DeleteEntry(const Slice &key, void *value) {
        TableAndFile *tf = reinterpret_cast<TableAndFile *>(value);
        delete tf->table;
        delete tf->file;
        delete tf;
    }

    void UnrefEntry(void *arg1, void *arg2) {
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

    Status
    TableCache::FindTable(const nova::GlobalSSTableHandle &th,
                          Cache::Handle **handle) {
        Status s;
        char buf[sizeof(th.table_id)];
        EncodeFixed64(buf, th.table_id);
        Slice key(buf, sizeof(buf));
        *handle = cache_->Lookup(key);
        if (*handle == nullptr) {
            std::string fname = TableFileName(dbname_, th.table_id);
            RandomAccessFile *file = nullptr;
            Table *table = nullptr;
            s = env_->NewRandomAccessFile(fname, &file);
            if (!s.ok()) {
                std::string old_fname = SSTTableFileName(dbname_, th.table_id);
                if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
                    s = Status::OK();
                }
            }
            if (s.ok()) {
                // TODO: Fix.
                s = Table::Open(options_, file, th.configuration_id,
                                th.partition_id, th.table_id,
                                &table, db_profiler_);
            }

            if (!s.ok()) {
                assert(table == nullptr);
                delete file;
                // We do not cache error results so that if the error is transient,
                // or somebody repairs the file, we recover automatically.
            } else {
                TableAndFile *tf = new TableAndFile;
                tf->file = file;
                tf->table = table;
                *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
            }
        }
        return s;
    }

    Iterator *
    TableCache::NewIterator(AccessCaller caller, const ReadOptions &options,
                            const nova::GlobalSSTableHandle &th,
                            Table **tableptr) {
        if (tableptr != nullptr) {
            *tableptr = nullptr;
        }

        Cache::Handle *handle = nullptr;
        Status s = FindTable(th, &handle);
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

    Status
    TableCache::Get(const ReadOptions &options,
                    const nova::GlobalSSTableHandle &th,
                    const Slice &k,
                    void *arg,
                    void (*handle_result)(void *, const Slice &,
                                          const Slice &)) {
        Cache::Handle *handle = nullptr;
        Status s = FindTable(th, &handle);
        if (s.ok()) {
            Table *t = reinterpret_cast<TableAndFile *>(cache_->Value(
                    handle))->table;
            s = t->InternalGet(options, k, arg, handle_result);
            cache_->Release(handle);
        }
        return s;
    }

    void TableCache::Evict(const nova::GlobalSSTableHandle &th) {
        char buf[th.size()];
        th.Encode(buf);
        cache_->Erase(Slice(buf, sizeof(buf)));
    }

}  // namespace leveldb
