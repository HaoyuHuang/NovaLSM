
//
// Created by Haoyu Huang on 1/7/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "table/format.h"
#include "nova_dc.h"
#include "nova/logging.hpp"
#include "util/coding.h"
#include "db/filename.h"
#include <fmt/core.h>

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

    void NovaDiskComponent::DeleteTable(const std::string &dbname,
                                        uint64_t file_number) {
        DCTables *dc_tables = db_tables_[dbname];
        std::string tablename = TableFileName(dbname, file_number);
        dc_tables->mutex.lock();
        dc_tables->tables.erase(tablename);
        dc_tables->mutex.unlock();

        char buf[4 + dbname.size() + 8];
        char *ptr = buf;
        ptr += EncodeStr(ptr, dbname);
        EncodeFixed64(ptr, file_number);
        Slice key(buf, sizeof(buf));
        cache_->Erase(key);
        env_->DeleteFile(TableFileName(dbname, file_number));
    }

    Status
    NovaDiskComponent::FindTable(const std::string &dbname,
                                 uint64_t file_number,
                                 leveldb::Cache::Handle **handle) {
        Status s;
        char buf[4 + dbname.size() + 8];
        char *ptr = buf;
        ptr += EncodeStr(ptr, dbname);
        EncodeFixed64(ptr, file_number);
        Slice key(buf, sizeof(buf));
        *handle = cache_->Lookup(key);
        if (*handle == nullptr) {
            std::string fname = TableFileName(dbname, file_number);
            RandomAccessFile *file = nullptr;
            s = env_->NewRandomAccessFile(fname, &file);
            RDMA_ASSERT(s.ok())
                << fmt::format("db{} fn:{} status:{}", dbname, file_number,
                               s.ToString());
            *handle = cache_->Insert(key, file, 1, &DeleteEntry);
        }
        return s;
    }

    uint64_t NovaDiskComponent::ReadBlocks(const std::string &dbname,
                                           uint64_t file_number,
                                           const std::vector<leveldb::CCBlockHandle> &block_handles,
                                           char *buf) {
        char *result_buf = buf;
        Cache::Handle *handle = nullptr;
        Status s = FindTable(dbname, file_number, &handle);
        RDMA_ASSERT(s.ok());
        RandomAccessFile *table = reinterpret_cast<RandomAccessFile *>(cache_->Value(
                handle));
        RDMA_ASSERT(s.ok());
        uint64_t ts = 0;
        for (const auto &bh : block_handles) {
            Slice result;
            RTableHandle h;
            RDMA_ASSERT(
                    table->Read(h, bh.offset, bh.size, &result, result_buf).ok());
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format(
                        "read block off:{} size:{} read:{} from db:{} fn:{}",
                        bh.offset, bh.size, result.size(), dbname, file_number);
            result_buf += result.size();
            ts += result.size();
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
                                          {.level = 1}, &file).ok());
        RDMA_ASSERT(file->Append(Slice(buf, table_size)).ok());
//        file->Flush();
//        file->Sync();
        file->Close();
        delete file;

        // Verify checksum.
//        char footer_space[Footer::kEncodedLength];
//        std::vector<DCBlockHandle> bhs;
//        bhs.push_back({
//                              .offset = table_size - Footer::kEncodedLength,
//                              .size = Footer::kEncodedLength
//                      });
//        ReadBlocks(dbname, file_number, bhs, footer_space);
//        Footer footer;
//        Slice footer_input(footer_space, Footer::kEncodedLength);
//        Status s = footer.DecodeFrom(&footer_input);
//        RDMA_ASSERT(s.ok()) << s.ToString();
        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format("footer is correct. fn:{} off:{} size:{}",
                           file_number, table_size - Footer::kEncodedLength,
                           Footer::kEncodedLength);
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
        char *result_buf = buf;
        Cache::Handle *handle = nullptr;
        Status s = FindTable(dbname, file_number, &handle);
        RDMA_ASSERT(s.ok());
        RandomAccessFile *table = reinterpret_cast<RandomAccessFile *>(cache_->Value(
                handle));
        RDMA_ASSERT(s.ok());
        Slice result;
        RTableHandle h;
        RDMA_ASSERT(
                table->Read(h, 0, size, &result, result_buf).ok());
        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format(
                    "read SSTable off:{} size:{} read:{} from db:{} fn:{}",
                    0, size, result.size(), dbname, file_number);
        RDMA_ASSERT(result.size() == size)
            << fmt::format("Size mismatch db:{} fn:{} expected:{} actual:{}",
                           dbname, file_number, size, result.size());
        return result.size();
    }


}