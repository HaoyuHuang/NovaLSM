
//
// Created by Haoyu Huang on 1/7/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_dc.h"
#include "nova/logging.hpp"
#include "util/coding.h"
#include "db/filename.h"

namespace leveldb {
    static void DeleteEntry(const Slice &key, void *value) {
        RandomAccessFile *tf = reinterpret_cast<RandomAccessFile *>(value);
        delete tf;
    }

    static void UnrefEntry(void *arg1, void *arg2) {
        Cache *cache = reinterpret_cast<Cache *>(arg1);
        Cache::Handle *h = reinterpret_cast<Cache::Handle *>(arg2);
        cache->Release(h);
    }

    NovaDiskComponent::NovaDiskComponent(leveldb::Env *env,
                                         Cache *cache,
                                         const std::vector<std::string> &dbs)
            : env_(env),
              cache_(cache) {
        for (const auto &db : dbs) {
            db_tables_[db] = new DCTables;
        }
    }

    Status
    NovaDiskComponent::FindTable(const std::string &dbname,
                                 uint64_t file_number,
                                 leveldb::Cache::Handle **handle) {
        Status s;
        char buf[4 + dbname.size() + 4];
        char *ptr = buf;
        ptr += EncodeStr(ptr, dbname);
        EncodeFixed64(ptr, file_number);
        Slice key(buf, sizeof(buf));
        *handle = cache_->Lookup(key);
        if (*handle == nullptr) {
            std::string fname = TableFileName(dbname, file_number);
            RandomAccessFile *file = nullptr;
            s = env_->NewRandomAccessFile(fname, &file);
            if (!s.ok()) {
                std::string old_fname = SSTTableFileName(dbname, file_number);
                if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
                    s = Status::OK();
                }
            }

            if (!s.ok()) {
                delete file;
                // We do not cache error results so that if the error is transient,
                // or somebody repairs the file, we recover automatically.
            } else {
                *handle = cache_->Insert(key, file, 1, &DeleteEntry);
            }
        }
        return s;
    }

    uint64_t NovaDiskComponent::ReadBlocks(const std::string &dbname,
                                           uint64_t file_number,
                                           const std::vector<leveldb::DCBlockHandle> &block_handls,
                                           char *buf) {
        char *result_buf = buf;
        Cache::Handle *handle = nullptr;
        Status s = FindTable(dbname, file_number, &handle);
        RDMA_ASSERT(s.ok());
        RandomAccessFile *table = reinterpret_cast<RandomAccessFile *>(cache_->Value(
                handle));
        uint64_t ts = 0;
        for (const auto &handle : block_handls) {
            Slice result;
            RDMA_ASSERT(table->Read(handle.offset, handle.size, &result,
                                    result_buf).ok());
            result_buf += handle.size;
            ts += handle.size;
        }
        return ts;
    }

    void NovaDiskComponent::FlushSSTable(const std::string &dbname,
                                         uint64_t file_number, char *buf,
                                         uint64_t table_size) {
        DCTables *dc_tables = db_tables_[dbname];
        std::string tablename = TableFileName(dbname, file_number);
        dc_tables->mutex.lock();
        dc_tables->tables[tablename].file_size = table_size;
        dc_tables->mutex.unlock();

        WritableFile *file;
        RDMA_ASSERT(env_->NewWritableFile(TableFileName(dbname, file_number),
                                          {.level = 0}, &file).ok());
        RDMA_ASSERT(file->Append(Slice(buf, table_size)).ok());
        file->Close();
        delete file;
    }

    uint64_t NovaDiskComponent::TableSize(const std::string &dbname,
                                          uint64_t file_number) {
        DCTables *dc_tables = db_tables_[dbname];
        std::string tablename = TableFileName(dbname, file_number);
        dc_tables->mutex.lock();
        uint64_t size = dc_tables->tables[tablename].file_size;
        dc_tables->mutex.unlock();
        return size;
    }

    uint64_t NovaDiskComponent::ReadSSTable(const std::string &dbname,
                                            uint64_t file_number, char *buf,
                                            uint64_t size) {
        SequentialFile *file;
        RDMA_ASSERT(env_->NewSequentialFile(TableFileName(dbname, file_number),
                                            &file).ok());
        Slice s;
        RDMA_ASSERT(file->Read(size, &s, buf).ok());
        delete file;
    }


}