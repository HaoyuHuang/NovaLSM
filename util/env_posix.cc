// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>
#include "env_posix.h"
#include "mutexlock.h"

namespace leveldb {

    namespace {

// Set by EnvPosixTestHelper::SetReadOnlyMMapLimit() and MaxOpenFiles().
        int g_open_read_only_file_limit = -1;

// Up to 1000 mmap regions for 64-bit binaries; none for 32-bit.
        constexpr const int kDefaultMmapLimit = (sizeof(void *) >= 8) ? 1000
                                                                      : 0;

// Can be set using EnvPosixTestHelper::SetReadOnlyMMapLimit().
        int g_mmap_limit = kDefaultMmapLimit;

// Common flags defined for all posix open operations
#if defined(HAVE_O_CLOEXEC)
        constexpr const int kOpenBaseFlags = O_CLOEXEC;
#else
        constexpr const int kOpenBaseFlags = 0;
#endif  // defined(HAVE_O_CLOEXEC)

        Status PosixError(const std::string &context, int error_number) {
            if (error_number == ENOENT) {
                return Status::NotFound(context, std::strerror(error_number));
            } else {
                return Status::IOError(context, std::strerror(error_number));
            }
        }
    }

    // Limit maximum number of resources to |max_acquires|.
    Limiter::Limiter(int max_acquires) : acquires_allowed_(max_acquires) {}


    // If another resource is available, acquire it and return true.
    // Else return false.
    bool Limiter::Acquire() {
        int old_acquires_allowed =
                acquires_allowed_.fetch_sub(1,
                                            std::memory_order_relaxed);

        if (old_acquires_allowed > 0) return true;

        acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    // Release a resource acquired by a previous call to Acquire() that returned
    // true.
    void Limiter::Release() {
        acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
    }

    PosixSequentialFile::PosixSequentialFile(std::string filename, int fd)
            : fd_(fd), filename_(filename) {}

    PosixSequentialFile::~PosixSequentialFile() { close(fd_); }

    Status PosixSequentialFile::Read(size_t n, Slice *result, char *scratch) {
        Status status;
        while (true) {
            ::ssize_t read_size = ::read(fd_, scratch, n);
            if (read_size < 0) {  // Read error.
                if (errno == EINTR) {
                    continue;  // Retry
                }
                status = PosixError(filename_, errno);
                break;
            }
            *result = Slice(scratch, read_size);
            break;
        }
        return status;
    }

    Status PosixSequentialFile::Skip(uint64_t n) {
        if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
            return PosixError(filename_, errno);
        }
        return Status::OK();
    }

    PosixRandomAccessFile::PosixRandomAccessFile(std::string filename, int fd,
                                                 Limiter *fd_limiter)
            : has_permanent_fd_(fd_limiter->Acquire()),
              fd_(has_permanent_fd_ ? fd : -1),
              fd_limiter_(fd_limiter),
              filename_(std::move(filename)) {
        if (!has_permanent_fd_) {
            assert(fd_ == -1);
            ::close(fd);  // The file will be opened on every read.
        }
    }

    PosixRandomAccessFile::~PosixRandomAccessFile() {
        if (has_permanent_fd_) {
            assert(fd_ != -1);
            ::close(fd_);
            fd_limiter_->Release();
        }
    }

    Status PosixRandomAccessFile::Read(const StoCBlockHandle &stoc_block_handle,
                                       uint64_t offset, size_t n, Slice *result,
                                       char *scratch) {
        int fd = fd_;
        if (!has_permanent_fd_) {
            fd = ::open(filename_.c_str(), O_RDONLY | kOpenBaseFlags);
            if (fd < 0) {
                return PosixError(filename_, errno);
            }
        }

        assert(fd != -1);

        Status status;
        ssize_t read_size = ::pread(fd, scratch, n,
                                    static_cast<off_t>(offset));
        *result = Slice(scratch, (read_size < 0) ? 0 : read_size);
        if (read_size < 0) {
            // An error: return a non-ok status.
            status = PosixError(filename_, errno);
        }
        if (!has_permanent_fd_) {
            // Close the temporary file descriptor opened earlier.
            assert(fd != fd_);
            ::close(fd);
        }
        return status;
    }

    PosixMmapReadableFile::PosixMmapReadableFile(std::string filename,
                                                 char *mmap_base,
                                                 size_t length,
                                                 Limiter *mmap_limiter)
            : mmap_base_(mmap_base),
              length_(length),
              mmap_limiter_(mmap_limiter),
              filename_(std::move(filename)) {}

    PosixMmapReadableFile::~PosixMmapReadableFile() {
        ::munmap(static_cast<void *>(mmap_base_), length_);
        mmap_limiter_->Release();
    }

    Status PosixMmapReadableFile::Read(const StoCBlockHandle &stoc_block_handle,
                                       uint64_t offset, size_t n, Slice *result,
                                       char *scratch) {
        if (offset + n > length_) {
            *result = Slice();
            return PosixError(filename_, EINVAL);
        }
        if (scratch) {
            memcpy(scratch, mmap_base_ + offset, n);
            *result = Slice(scratch, n);
        } else {
            *result = Slice(mmap_base_ + offset, n);
        }
        return Status::OK();
    }

    PosixReadWriteFile::PosixReadWriteFile(std::string filename, int fd)
            : fd_(fd),
              is_manifest_(false),
              filename_(std::move(filename)),
              dirname_(PosixWritableFile::Dirname(filename_)) {}

    PosixReadWriteFile::~PosixReadWriteFile() {
        if (fd_ >= 0) {
            // Ignoring any potential errors
            Close();
        }
    }

    Status PosixReadWriteFile::Read(const StoCBlockHandle &stoc_block_handle,
                                    uint64_t offset, size_t n, Slice *result,
                                    char *scratch) {
        assert(fd_ != -1);

        Status status;
        ssize_t read_size = ::pread(fd_, scratch, n,
                                    static_cast<off_t>(offset));
        *result = Slice(scratch, (read_size < 0) ? 0 : read_size);
        if (read_size < 0) {
            // An error: return a non-ok status.
            status = PosixError(filename_, errno);
        }
        return status;
    }

    Status PosixReadWriteFile::Append(const Slice &data) {
        size_t write_size = data.size();
        const char *write_data = data.data();
        if (write_size == 0) {
            return Status::OK();
        }
        return WriteUnbuffered(write_data, write_size);
    }

    Status PosixReadWriteFile::Close() {
        Status status;
        const int close_result = ::close(fd_);
        if (close_result < 0 && status.ok()) {
            status = PosixError(filename_, errno);
        }
        fd_ = -1;
        return status;
    }

    Status PosixReadWriteFile::Flush() { return Status::OK(); }

    Status PosixReadWriteFile::Sync() {
        return PosixWritableFile::SyncFd(fd_, filename_);
    }

    Status PosixReadWriteFile::WriteUnbuffered(const char *data, size_t size) {
        while (size > 0) {
            ssize_t write_result = ::write(fd_, data, size);
            if (write_result < 0) {
                if (errno == EINTR) {
                    continue;  // Retry
                }
                return PosixError(filename_, errno);
            }
            data += write_result;
            size -= write_result;
        }
        return Status::OK();
    }

    PosixWritableFile::PosixWritableFile(std::string filename, int fd)
            : pos_(0),
              fd_(fd),
              is_manifest_(IsManifest(filename)),
              filename_(std::move(filename)),
              dirname_(Dirname(filename_)) {}

    PosixWritableFile::~PosixWritableFile() {
        if (fd_ >= 0) {
            // Ignoring any potential errors
            Close();
        }
    }

    Status PosixWritableFile::Append(const Slice &data) {
        size_t write_size = data.size();
        const char *write_data = data.data();

        // Fit as much as possible into buffer.
        size_t copy_size = std::min(write_size,
                                    kWritableFileBufferSize - pos_);
        std::memcpy(buf_ + pos_, write_data, copy_size);
        write_data += copy_size;
        write_size -= copy_size;
        pos_ += copy_size;
        if (write_size == 0) {
            return Status::OK();
        }

        // Can't fit in buffer, so need to do at least one write.
        Status status = FlushBuffer();
        if (!status.ok()) {
            return status;
        }

        // Small writes go to buffer, large writes are written directly.
        if (write_size < kWritableFileBufferSize) {
            std::memcpy(buf_, write_data, write_size);
            pos_ = write_size;
            return Status::OK();
        }
        return WriteUnbuffered(write_data, write_size);
    }

    Status PosixWritableFile::Close() {
        Status status = FlushBuffer();
        const int close_result = ::close(fd_);
        if (close_result < 0 && status.ok()) {
            status = PosixError(filename_, errno);
        }
        fd_ = -1;
        return status;
    }

    Status PosixWritableFile::Flush() { return FlushBuffer(); }

    Status PosixWritableFile::Sync() {
        // Ensure new files referred to by the manifest are in the filesystem.
        //
        // This needs to happen before the manifest file is flushed to disk, to
        // avoid crashing in a state where the manifest refers to files that are not
        // yet on disk.
        Status status = SyncDirIfManifest();
        if (!status.ok()) {
            return status;
        }

        status = FlushBuffer();
        if (!status.ok()) {
            return status;
        }

        return SyncFd(fd_, filename_);
    }

    Status PosixWritableFile::FlushBuffer() {
        Status status = WriteUnbuffered(buf_, pos_);
        pos_ = 0;
        return status;
    }

    Status PosixWritableFile::WriteUnbuffered(const char *data, size_t size) {
        while (size > 0) {
            ssize_t write_result = ::write(fd_, data, size);
            if (write_result < 0) {
                if (errno == EINTR) {
                    continue;  // Retry
                }
                return PosixError(filename_, errno);
            }
            data += write_result;
            size -= write_result;
        }
        return Status::OK();
    }

    Status PosixWritableFile::SyncDirIfManifest() {
        Status status;
        if (!is_manifest_) {
            return status;
        }

        int fd = ::open(dirname_.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
            status = PosixError(dirname_, errno);
        } else {
            status = SyncFd(fd, dirname_);
            ::close(fd);
        }
        return status;
    }

    // Ensures that all the caches associated with the given file descriptor's
    // data are flushed all the way to durable media, and can withstand power
    // failures.
    //
    // The path argument is only used to populate the description string in the
    // returned Status if an error occurs.
    Status PosixWritableFile::SyncFd(int fd, const std::string &fd_path) {
#if HAVE_FULLFSYNC
        // On macOS and iOS, fsync() doesn't guarantee durability past power
        // failures. fcntl(F_FULLFSYNC) is required for that purpose. Some
        // filesystems don't support fcntl(F_FULLFSYNC), and require a fallback to
        // fsync().
        if (::fcntl(fd, F_FULLFSYNC) == 0) {
          return Status::OK();
        }
#endif  // HAVE_FULLFSYNC

#if HAVE_FDATASYNC
        bool sync_success = ::fdatasync(fd) == 0;
#else
        bool sync_success = ::fsync(fd) == 0;
#endif  // HAVE_FDATASYNC

        if (sync_success) {
            return Status::OK();
        }
        return PosixError(fd_path, errno);
    }

    // Returns the directory name in a path pointing to a file.
    //
    // Returns "." if the path does not contain any directory separator.
    std::string PosixWritableFile::Dirname(const std::string &filename) {
        std::string::size_type separator_pos = filename.rfind('/');
        if (separator_pos == std::string::npos) {
            return std::string(".");
        }
        // The filename component should not contain a path separator. If it does,
        // the splitting was done incorrectly.
        assert(filename.find('/', separator_pos + 1) ==
               std::string::npos);

        return filename.substr(0, separator_pos);
    }

    // Extracts the file name from a path pointing to a file.
    //
    // The returned Slice points to |filename|'s data buffer, so it is only valid
    // while |filename| is alive and unchanged.
    Slice PosixWritableFile::Basename(const std::string &filename) {
        std::string::size_type separator_pos = filename.rfind('/');
        if (separator_pos == std::string::npos) {
            return Slice(filename);
        }
        // The filename component should not contain a path separator. If it does,
        // the splitting was done incorrectly.
        assert(filename.find('/', separator_pos + 1) ==
               std::string::npos);

        return Slice(filename.data() + separator_pos + 1,
                     filename.length() - separator_pos - 1);
    }

    // True if the given file is a manifest file.
    bool PosixWritableFile::IsManifest(const std::string &filename) {
        return Basename(filename).starts_with("MANIFEST");
    }

    int LockOrUnlock(int fd, bool lock) {
        errno = 0;
        struct ::flock file_lock_info;
        std::memset(&file_lock_info, 0, sizeof(file_lock_info));
        file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK);
        file_lock_info.l_whence = SEEK_SET;
        file_lock_info.l_start = 0;
        file_lock_info.l_len = 0;  // Lock/unlock entire file.
        return ::fcntl(fd, F_SETLK, &file_lock_info);
    }

    PosixFileLock::PosixFileLock(int fd, std::string filename)
            : fd_(fd), filename_(std::move(filename)) {}

    PosixLockTable::PosixLockTable() {
    }

    void PosixLockTable::Lock(const std::string &fname, uint64_t fd) {
        PosixFileLock *lock = nullptr;
        mu_.Lock();
        auto pair = fd_lock_.find(fd);
        if (pair == fd_lock_.end()) {
            lock = new PosixFileLock(fd, fname);
            fd_lock_[fd] = lock;
        } else {
            lock = pair->second;
        }
        lock->num_waiting_ += 1;
        mu_.Unlock();

        lock->mu_.lock();
    }

    void PosixLockTable::Unlock(const std::string &fname, uint64_t fd) {
//        NOVA_LOG(rdmaio::INFO) << fmt::format("!!!!!!!!!UnLock {}:{}", fname, fd);
        mu_.Lock();
        NOVA_ASSERT(fd_lock_.find(fd) != fd_lock_.end());
        auto lock = fd_lock_[fd];
        lock->num_waiting_ -= 1;
        lock->mu_.unlock();
        if (lock->num_waiting_ == 0) {
            fd_lock_.erase(fd);
            delete lock;
        }
        mu_.Unlock();
    }

    PosixEnv::~PosixEnv() {
        for (auto i = file_map_.begin();
             i != file_map_.end(); ++i) {
            i->second->Unref();
        }

        static const char msg[] =
                "PosixEnv singleton destroyed. Unsupported behavior!\n";
        std::fwrite(msg, 1, sizeof(msg), stderr);
        std::abort();
    }

    std::string PosixEnv::NormalizePath(const std::string &path) {
        std::string dst;
        for (auto c : path) {
            if (!dst.empty() && c == '/' && dst.back() == '/') {
                continue;
            }
            dst.push_back(c);
        }
        return dst;
    }

    Status PosixEnv::NewSequentialFile(const std::string &filename,
                                       SequentialFile **result) {
        *result = nullptr;
        if (HasMemSSTables()) {
            auto fn = NormalizePath(filename);
            MutexLock lock(&mutex_);
            if (file_map_.find(fn) != file_map_.end()) {
                auto *f = file_map_[fn];
                if (f->is_lock_file()) {
                    return Status::InvalidArgument(fn,
                                                   "Cannot open a lock file.");
                }
                *result = new MemSequentialFile(f);
            }
            if (*result != nullptr) {
                return Status::OK();
            }
        }

        int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }

        *result = new PosixSequentialFile(filename, fd);
        return Status::OK();
    }

    Status PosixEnv::NewRandomAccessFile(const std::string &filename,
                                         RandomAccessFile **result) {
        *result = nullptr;
        if (HasMemSSTables()) {
            auto fn = NormalizePath(filename);
            MutexLock lock(&mutex_);
            if (file_map_.find(fn) != file_map_.end()) {
                auto *f = file_map_[fn];
                if (f->is_lock_file()) {
                    return Status::InvalidArgument(fn,
                                                   "Cannot open a lock file.");
                }
                *result = new MemRandomAccessFile(f);
            }
            if (*result != nullptr) {
                return Status::OK();
            }
            return Status::NotFound(fn, " Not found");
        }

        int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
            return PosixError(filename, errno);
        }

        *result = new PosixRandomAccessFile(filename, fd,
                                            &fd_limiter_);
        return Status::OK();

//        if (!mmap_limiter_.Acquire()) {
//
//        }
//
//        uint64_t file_size;
//        Status status = GetFileSize(filename, &file_size);
//        if (status.ok()) {
//            void *mmap_base =
//                    ::mmap(/*addr=*/nullptr, file_size, PROT_READ,
//                                    MAP_SHARED, fd, 0);
//            if (mmap_base != MAP_FAILED) {
//                *result = new PosixMmapReadableFile(filename,
//                                                    reinterpret_cast<char *>(mmap_base),
//                                                    file_size,
//                                                    &mmap_limiter_);
//            } else {
//                status = PosixError(filename, errno);
//            }
//        }
//        ::close(fd);
//        if (!status.ok()) {
//            mmap_limiter_.Release();
//        }
//        return status;
    }

    void PosixEnv::DeleteFileInternal(const std::string &path) {
        assert(path == NormalizePath(path));
        const auto &pair = file_map_.find(path);
        if (pair != file_map_.end()) {
            pair->second->Unref();
            file_map_.erase(path);
        }
    }

    bool PosixEnv::HasMemSSTables() {
        return env_options_.sstable_mode ==
               NovaSSTableMode::SSTABLE_MEM ||
               env_options_.sstable_mode ==
               NovaSSTableMode::SSTABLE_HYBRID;
    }

    Status PosixEnv::NewWritableFile(const std::string &filename,
                                     const EnvFileMetadata &metadata,
                                     WritableFile **result) {
        auto fn = NormalizePath(filename);
        bool memtable = env_options_.sstable_mode ==
                        NovaSSTableMode::SSTABLE_MEM ||
                        (env_options_.sstable_mode ==
                         NovaSSTableMode::SSTABLE_HYBRID &&
                         metadata.level == 0);
        if (memtable) {
            mutex_.Lock();
            if (file_map_.find(fn) != file_map_.end()) {
                DeleteFileInternal(fn);
            }
            MemFile *file = new MemFile(this, fn, false);
            file->Ref();
            file_map_[fn] = file;
            *result = new MemWritableFile(file);
            mutex_.Unlock();
            return Status::OK();
        }
        int fd = ::open(filename.c_str(),
                        O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags,
                        0644);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }

        *result = new PosixWritableFile(filename, fd);
        return Status::OK();
    }

    Status PosixEnv::NewReadWriteFile(const std::string &filename,
                                      const EnvFileMetadata &metadata,
                                      ReadWriteFile **result) {
        int fd = ::open(filename.c_str(),
                        O_RDWR | O_CREAT | kOpenBaseFlags,
                        0644);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }

        *result = new PosixReadWriteFile(filename, fd);
        return Status::OK();
    }

    Status PosixEnv::NewAppendableFile(const std::string &filename,
                                       WritableFile **result) {
        int fd = ::open(filename.c_str(),
                        O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags,
                        0644);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }

        *result = new PosixWritableFile(filename, fd);
        return Status::OK();
    }

    bool PosixEnv::FileExists(const std::string &filename) {
        auto fn = NormalizePath(filename);
        {
            MutexLock lock(&mutex_);
            if (file_map_.find(fn) != file_map_.end()) {
                // File exists
                return true;
            }
            // Now also check if fn exists as a dir
            for (const auto &iter : file_map_) {
                const std::string &filename = iter.first;
                if (filename.size() >= fn.size() + 1 &&
                    filename[fn.size()] == '/' &&
                    Slice(filename).starts_with(Slice(fn))) {
                    return true;
                }
            }
        }
        return ::access(filename.c_str(), F_OK) == 0;
    }

    Status PosixEnv::GetChildren(const std::string &directory_path,
                                 std::vector<std::string> *result) {
        result->clear();
        auto d = NormalizePath(directory_path);
        {
            MutexLock lock(&mutex_);
            for (const auto &iter : file_map_) {
                const std::string &filename = iter.first;

                if (filename == d) {
                    // found.
                } else if (filename.size() >= d.size() + 1 &&
                           filename[d.size()] == '/' &&
                           Slice(filename).starts_with(Slice(d))) {
                    size_t next_slash = filename.find('/', d.size() + 1);
                    if (next_slash != std::string::npos) {
                        result->push_back(
                                filename.substr(d.size() + 1,
                                                next_slash - d.size() - 1));
                    } else {
                        result->push_back(filename.substr(d.size() + 1));
                    }
                }
            }
        }

        // Also look at the actual directory.
        ::DIR *dir = ::opendir(directory_path.c_str());
        if (dir == nullptr) {
            return PosixError(directory_path, errno);
        }
        struct ::dirent *entry;
        while ((entry = ::readdir(dir)) != nullptr) {
            result->emplace_back(entry->d_name);
        }
        ::closedir(dir);
        // Remove duplicates.
        result->erase(std::unique(result->begin(), result->end()),
                      result->end());
        return Status::OK();
    }

    Status PosixEnv::DeleteFile(const std::string &filename) {
        auto fn = NormalizePath(filename);
        {
            MutexLock lock(&mutex_);
            if (file_map_.find(fn) != file_map_.end()) {
                DeleteFileInternal(fn);
            }
        }
        if (::unlink(filename.c_str()) != 0) {
            return PosixError(filename, errno);
        }
        return Status::OK();
    }

    Status PosixEnv::CreateDir(const std::string &dirname) {
        auto dn = NormalizePath(dirname);
        if (file_map_.find(dn) == file_map_.end()) {
            MemFile *file = new MemFile(this, dn, false);
            file->Ref();
            file_map_[dn] = file;
        }
        if (::mkdir(dirname.c_str(), 0755) != 0) {
            return PosixError(dirname, errno);
        }
        return Status::OK();
    }

    Status PosixEnv::DeleteDir(const std::string &dirname) {
        auto fn = NormalizePath(dirname);
        {
            MutexLock lock(&mutex_);
            if (file_map_.find(fn) != file_map_.end()) {
                DeleteFileInternal(fn);
            }
        }
        if (::rmdir(dirname.c_str()) != 0) {
            return PosixError(dirname, errno);
        }
        return Status::OK();
    }

    Status
    PosixEnv::GetFileSize(const std::string &filename, uint64_t *size) {
        {
            auto fn = NormalizePath(filename);
            MutexLock lock(&mutex_);
            auto iter = file_map_.find(fn);
            if (iter != file_map_.end()) {
                *size = iter->second->Size();
                return Status::OK();
            }
        }

        struct ::stat file_stat;
        if (::stat(filename.c_str(), &file_stat) != 0) {
            *size = 0;
            return PosixError(filename, errno);
        }
        *size = file_stat.st_size;
        return Status::OK();
    }

    Status PosixEnv::RenameFile(const std::string &from,
                                const std::string &to) {
        auto s = NormalizePath(from);
        auto t = NormalizePath(to);
        {
            MutexLock lock(&mutex_);
            if (file_map_.find(s) != file_map_.end()) {
                DeleteFileInternal(t);
                file_map_[t] = file_map_[s];
                file_map_.erase(s);
                return Status::OK();
            }
        }
        if (std::rename(from.c_str(), to.c_str()) != 0) {
            return PosixError(from, errno);
        }
        return Status::OK();
    }

    Status
    PosixEnv::LockFile(const std::string &filename, uint64_t fd) {
        lock_table_.Lock(filename, fd);
        return Status::OK();
    }

    Status PosixEnv::UnlockFile(const std::string &fname, uint64_t fd) {
        lock_table_.Unlock(fname, fd);
        return Status::OK();
    }

    void PosixEnv::StartThread(void (*thread_main)(void *thread_main_arg),
                               void *thread_main_arg) {
        std::thread new_thread(thread_main, thread_main_arg);
        new_thread.detach();
    }

    Status PosixEnv::GetTestDirectory(std::string *result) {
        const char *env = std::getenv("TEST_TMPDIR");
        if (env && env[0] != '\0') {
            *result = env;
        } else {
            char buf[100];
            std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d",
                          static_cast<int>(::geteuid()));
            *result = buf;
        }

        // The CreateDir status is ignored because the directory may already exist.
        CreateDir(*result);

        return Status::OK();
    }

    Status
    PosixEnv::NewLogger(const std::string &filename, Logger **result) {
        int fd = ::open(filename.c_str(),
                        O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags,
                        0644);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }

        std::FILE *fp = ::fdopen(fd, "w");
        if (fp == nullptr) {
            ::close(fd);
            *result = nullptr;
            return PosixError(filename, errno);
        } else {
            *result = new PosixLogger(fp);
            return Status::OK();
        }
    }

    uint64_t PosixEnv::NowMicros() {
        static constexpr uint64_t kUsecondsPerSecond = 1000000;
        struct ::timeval tv;
        ::gettimeofday(&tv, nullptr);
        return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond +
               tv.tv_usec;
    }

    void PosixEnv::SleepForMicroseconds(int micros) {
        std::this_thread::sleep_for(std::chrono::microseconds(micros));
    }


// Return the maximum number of concurrent mmaps.
    int MaxMmaps() { return g_mmap_limit; }

// Return the maximum number of read-only files to keep open.
    int MaxOpenFiles() {
        if (g_open_read_only_file_limit >= 0) {
            return g_open_read_only_file_limit;
        }
        struct ::rlimit rlim;
        if (::getrlimit(RLIMIT_NOFILE, &rlim)) {
            // getrlimit failed, fallback to hard-coded default.
            g_open_read_only_file_limit = 50;
        } else if (rlim.rlim_cur == RLIM_INFINITY) {
            g_open_read_only_file_limit = std::numeric_limits<int>::max();
        } else {
            // Allow use of 20% of available file descriptors for read-only files.
            g_open_read_only_file_limit = rlim.rlim_cur / 5;
        }
        return g_open_read_only_file_limit;
    }

    PosixEnv::PosixEnv()
            :
            mmap_limiter_(MaxMmaps()),
            fd_limiter_(MaxOpenFiles()) {}


    namespace {

// Wraps an Env instance whose destructor is never created.
//
// Intended usage:
//   using PlatformSingletonEnv = SingletonEnv<PlatformEnv>;
//   void ConfigurePosixEnv(int param) {
//     PlatformSingletonEnv::AssertEnvNotInitialized();
//     // set global configuration flags.
//   }
//   Env* Env::Default() {
//     static PlatformSingletonEnv default_env;
//     return default_env.env();
//   }
        template<typename EnvType>
        class SingletonEnv {
        public:
            SingletonEnv() {
#if !defined(NDEBUG)
                env_initialized_.store(true,
                                       std::memory_order::memory_order_relaxed);
#endif  // !defined(NDEBUG)
                static_assert(sizeof(env_storage_) >= sizeof(EnvType),
                              "env_storage_ will not fit the Env");
                static_assert(
                        alignof(decltype(env_storage_)) >= alignof(EnvType),
                        "env_storage_ does not meet the Env's alignment needs");
                new(&env_storage_) EnvType();
            }

            ~SingletonEnv() = default;

            SingletonEnv(const SingletonEnv &) = delete;

            SingletonEnv &operator=(const SingletonEnv &) = delete;

            Env *env() { return reinterpret_cast<Env *>(&env_storage_); }

            static void AssertEnvNotInitialized() {
#if !defined(NDEBUG)
                assert(!env_initialized_.load(
                        std::memory_order::memory_order_relaxed));
#endif  // !defined(NDEBUG)
            }

        private:
            typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type
                    env_storage_;
#if !defined(NDEBUG)
            static std::atomic<bool> env_initialized_;
#endif  // !defined(NDEBUG)
        };

#if !defined(NDEBUG)
        template<typename EnvType>
        std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif  // !defined(NDEBUG)

        using PosixDefaultEnv = SingletonEnv<PosixEnv>;

    }  // namespace

    void EnvPosixTestHelper::SetReadOnlyFDLimit(int limit) {
        PosixDefaultEnv::AssertEnvNotInitialized();
        g_open_read_only_file_limit = limit;
    }

    void EnvPosixTestHelper::SetReadOnlyMMapLimit(int limit) {
        PosixDefaultEnv::AssertEnvNotInitialized();
        g_mmap_limit = limit;
    }

    Env *Env::Default() {
        static PosixDefaultEnv env_container;
        return env_container.env();
    }

}  // namespace leveldb
