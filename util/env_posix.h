
//
// Created by Haoyu Huang on 12/29/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_ENV_POSIX_H
#define LEVELDB_ENV_POSIX_H

#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <list>

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
#include <map>

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/env_posix_test_helper.h"
#include "util/posix_logger.h"
#include "env_mem.h"

namespace leveldb {

#define kWritableFileBufferSize 65536

    // Helper class to limit resource usage to avoid exhaustion.
// Currently used to limit read-only file descriptors and mmap file usage
// so that we do not run out of file descriptors or virtual memory, or run into
// kernel performance problems for very large databases.
    class Limiter {
    public:
        // Limit maximum number of resources to |max_acquires|.
        Limiter(int max_acquires);

        Limiter(const Limiter &) = delete;

        Limiter operator=(const Limiter &) = delete;

        // If another resource is available, acquire it and return true.
        // Else return false.
        bool Acquire();

        // Release a resource acquired by a previous call to Acquire() that returned
        // true.
        void Release();

    private:
        // The number of available resources.
        //
        // This is a counter and is not tied to the invariants of any other class, so
        // it can be operated on safely using std::memory_order_relaxed.
        std::atomic<int> acquires_allowed_;
    };

// Implements sequential read access in a file using read().
//
// Instances of this class are thread-friendly but not thread-safe, as required
// by the SequentialFile API.
    class PosixSequentialFile final : public SequentialFile {
    public:
        PosixSequentialFile(std::string filename, int fd);

        ~PosixSequentialFile() override;

        Status Read(size_t n, Slice *result, char *scratch) override;

        Status Skip(uint64_t n) override;

    private:
        const int fd_;
        const std::string filename_;
    };

// Implements random read access in a file using pread().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
    class PosixRandomAccessFile final : public RandomAccessFile {
    public:
        // The new instance takes ownership of |fd|. |fd_limiter| must outlive this
        // instance, and will be used to determine if .
        PosixRandomAccessFile(std::string filename, int fd,
                              Limiter *fd_limiter);

        ~PosixRandomAccessFile() override;

        Status
        Read(const StoCBlockHandle &stoc_block_handle, uint64_t offset, size_t n,
             Slice *result,
             char *scratch) override;

    private:
        const bool has_permanent_fd_;  // If false, the file is opened on every read.
        const int fd_;                 // -1 if has_permanent_fd_ is false.
        Limiter *const fd_limiter_;
        const std::string filename_;
    };

// Implements random read access in a file using mmap().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
    class PosixMmapReadableFile final : public RandomAccessFile {
    public:
        // mmap_base[0, length-1] points to the memory-mapped contents of the file. It
        // must be the result of a successful call to mmap(). This instances takes
        // over the ownership of the region.
        //
        // |mmap_limiter| must outlive this instance. The caller must have already
        // aquired the right to use one mmap region, which will be released when this
        // instance is destroyed.
        PosixMmapReadableFile(std::string filename, char *mmap_base,
                              size_t length,
                              Limiter *mmap_limiter);

        ~PosixMmapReadableFile() override;

        Status
        Read(const StoCBlockHandle &stoc_block_handle, uint64_t offset, size_t n,
             Slice *result, char *scratch) override;

    private:
        char *const mmap_base_;
        const size_t length_;
        Limiter *const mmap_limiter_;
        const std::string filename_;
    };

    class PosixReadWriteFile final : public ReadWriteFile {
    public:
        PosixReadWriteFile(std::string filename, int fd);

        ~PosixReadWriteFile();

        Status
        Read(const StoCBlockHandle &stoc_block_handle, uint64_t offset, size_t n,
             Slice *result, char *scratch) override;

        Status Append(const Slice &data) override;

        Status Close() override;

        Status Flush() override;

        Status Sync() override;
    private:
        Status WriteUnbuffered(const char *data, size_t size);

        // buf_[0, pos_ - 1] contains data to be written to fd_.
        int fd_;

        const bool is_manifest_ = false;  // True if the file's name starts with MANIFEST.
        const std::string filename_;
        const std::string dirname_;  // The directory of filename_.
    };

    class PosixWritableFile final : public WritableFile {
    public:
        PosixWritableFile(std::string filename, int fd);

        ~PosixWritableFile() override;

        Status Append(const Slice &data) override;

        Status Close() override;

        Status Flush() override;

        Status Sync() override;

        // Ensures that all the caches associated with the given file descriptor's
        // data are flushed all the way to durable media, and can withstand power
        // failures.
        //
        // The path argument is only used to populate the description string in the
        // returned Status if an error occurs.
        static Status SyncFd(int fd, const std::string &fd_path);

        // Returns the directory name in a path pointing to a file.
        //
        // Returns "." if the path does not contain any directory separator.
        static std::string Dirname(const std::string &filename);

        // Extracts the file name from a path pointing to a file.
        //
        // The returned Slice points to |filename|'s data buffer, so it is only valid
        // while |filename| is alive and unchanged.
        static Slice Basename(const std::string &filename);

        // True if the given file is a manifest file.
        static bool IsManifest(const std::string &filename);

    private:
        Status FlushBuffer();

        Status WriteUnbuffered(const char *data, size_t size);

        Status SyncDirIfManifest();

        // buf_[0, pos_ - 1] contains data to be written to fd_.
        char buf_[kWritableFileBufferSize];
        size_t pos_;
        int fd_;

        const bool is_manifest_;  // True if the file's name starts with MANIFEST.
        const std::string filename_;
        const std::string dirname_;  // The directory of filename_.
    };

    int LockOrUnlock(int fd, bool lock);

// Instances are thread-safe because they are immutable.
    class PosixFileLock : public FileLock {
    public:
        PosixFileLock(int fd, std::string filename);

        int fd() const { return fd_; }

        const std::string &filename() const { return filename_; }

        uint32_t num_waiting_ = 0;
        std::mutex mu_;
    private:
        const int fd_;
        const std::string filename_;
    };

// Tracks the files locked by PosixEnv::LockFile().
//
// We maintain a separate set instead of relying on fcntl(F_SETLK) because
// fcntl(F_SETLK) does not provide any protection against multiple uses from the
// same process.
//
// Instances are thread-safe because all member data is guarded by a mutex.
    class PosixLockTable {
    public:
        PosixLockTable();

        void Lock(const std::string &fname, uint64_t fd);

        void Unlock(const std::string &fname, uint64_t fd);
    private:
        port::Mutex mu_;
        std::unordered_map<uint64_t, PosixFileLock*> fd_lock_;
    };

    class PosixEnv : public Env {
    public:
        PosixEnv();

        ~PosixEnv() override;

        Status NewSequentialFile(const std::string &filename,
                                 SequentialFile **result) override;

        Status NewRandomAccessFile(const std::string &filename,
                                   RandomAccessFile **result) override;

        Status NewWritableFile(const std::string &filename,
                               const EnvFileMetadata &metadata,
                               WritableFile **result) override;

        Status NewReadWriteFile(const std::string &fname,
                                const EnvFileMetadata &metadata,
                                ReadWriteFile **result) override;

        Status NewAppendableFile(const std::string &filename,
                                 WritableFile **result) override;

        bool FileExists(const std::string &filename) override;

        Status GetChildren(const std::string &directory_path,
                           std::vector<std::string> *result) override;

        Status DeleteFile(const std::string &filename) override;

        Status CreateDir(const std::string &dirname) override;

        Status DeleteDir(const std::string &dirname) override;

        Status
        GetFileSize(const std::string &filename, uint64_t *size) override;

        Status RenameFile(const std::string &from,
                          const std::string &to) override;

        Status
        LockFile(const std::string &filename, uint64_t fd) override;

        Status UnlockFile(const std::string &fname, uint64_t fd) override;

        void StartThread(void (*thread_main)(void *thread_main_arg),
                         void *thread_main_arg) override;

        Status GetTestDirectory(std::string *result) override;

        Status
        NewLogger(const std::string &filename, Logger **result) override;

        uint64_t NowMicros() override;

        void SleepForMicroseconds(int micros) override;

    private:
        std::string NormalizePath(const std::string &path);

        void DeleteFileInternal(const std::string &path);

        bool HasMemSSTables();

        PosixLockTable lock_table_;
        Limiter mmap_limiter_;  // Thread-safe.
        Limiter fd_limiter_;    // Thread-safe.

        typedef std::map<std::string, MemFile *> FileSystem;
        port::Mutex mutex_;
        FileSystem file_map_;  // Protected by mutex_.
    };

// Return the maximum number of concurrent mmaps.
    int MaxMmaps();

// Return the maximum number of read-only files to keep open.
    int MaxOpenFiles();
}
#endif //LEVELDB_ENV_POSIX_H
