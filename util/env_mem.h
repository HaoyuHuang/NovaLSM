
//
// Created by Haoyu Huang on 12/29/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_ENV_MEM_H
#define LEVELDB_ENV_MEM_H

#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/env_posix_test_helper.h"
#include "util/posix_logger.h"

namespace leveldb {
    class MemFile {
    public:
        explicit MemFile(Env *env, const std::string &fn,
                         bool _is_lock_file = false);

        // No copying allowed.
        MemFile(const MemFile &) = delete;

        void operator=(const MemFile &) = delete;

        // Private since only Unref() should be used to delete it.
        ~MemFile() { assert(refs_ == 0); }

        void Ref();

        bool is_lock_file() const { return is_lock_file_; }

        bool Lock();

        void Unlock();

        void Unref();

        virtual uint64_t Size() const { return size_; }

        virtual Status
        Read(uint64_t offset, size_t n, Slice *result, char *scratch);

        virtual Status Write(uint64_t offset, const Slice &data);

        virtual Status Append(const Slice &data);

        virtual Status Fsync();

        uint64_t ModifiedTime() const { return modified_time_; }

        virtual const char *backing_mem() { return data_.data(); }

    private:
        uint64_t Now();

        Env *env_;
        const std::string fn_;
        mutable port::Mutex mutex_;
        int refs_;
        bool is_lock_file_;
        bool locked_;

        // Data written into this file, all bytes before fsynced_bytes are
        // persistent.
        std::string data_;
        std::atomic<uint64_t> size_;
        std::atomic<uint64_t> modified_time_;
        std::atomic<uint64_t> fsynced_bytes_;
    };

    class MemSequentialFile : public SequentialFile {
    public:
        explicit MemSequentialFile(MemFile *file);

        ~MemSequentialFile() override { file_->Unref(); }

        Status Read(size_t n, Slice *result, char *scratch) override;

        Status Skip(uint64_t n) override;

    private:
        MemFile *file_;
        size_t pos_;
    };

    class MemRandomAccessFile : public RandomAccessFile {
    public:
        MemRandomAccessFile(MemFile *file);

        ~MemRandomAccessFile() override { file_->Unref(); }

        Status Read(const StoCBlockHandle &stoc_block_handle, uint64_t offset, size_t n, Slice *result,
                    char *scratch) override;
        MemFile *file_;
    private:
    };

    class MemRandomRWFile : public RandomAccessFile {
    public:
        MemRandomRWFile(MemFile *file);

        ~MemRandomRWFile() override { file_->Unref(); }

        Status Read(const StoCBlockHandle &stoc_block_handle, uint64_t offset, size_t n, Slice *result,
                    char *scratch) override;

    private:
        MemFile *file_;
    };

    class MemWritableFile : public WritableFile {
    public:
        MemWritableFile(MemFile *file);

        ~MemWritableFile() override {
//            file_->Unref();
        }

        Status Append(const Slice &data) override;

        Status Close() override;

        Status Flush() override;

        Status Sync() override;

        MemFile *mem_file() { return file_; }

    private:
        MemFile *file_;
    };

    class MemEnvFileLock : public FileLock {
    public:
        explicit MemEnvFileLock(const std::string &fname);

        std::string FileName() const { return fname_; }

    private:
        const std::string fname_;
    };
}

#endif //LEVELDB_ENV_MEM_H
