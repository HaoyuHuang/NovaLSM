
//
// Created by Haoyu Huang on 12/29/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "env_mem.h"

namespace leveldb {

    MemFile::MemFile(Env *env, const std::string &fn,
                     bool _is_lock_file)
            : env_(env),
              fn_(fn),
              refs_(0),
              is_lock_file_(_is_lock_file),
              locked_(false),
              size_(0),
              modified_time_(Now()),
              fsynced_bytes_(0) {}

    void MemFile::Ref() {
        mutex_.Lock();
        ++refs_;
        mutex_.Unlock();
    }

    bool MemFile::Lock() {
        assert(is_lock_file_);
        mutex_.Lock();
        bool ret = !locked_;
        if (!locked_) {
            locked_ = true;
        }
        mutex_.Unlock();
        return ret;
    }

    void MemFile::Unlock() {
        assert(is_lock_file_);
        mutex_.Lock();
        locked_ = false;
        mutex_.Unlock();
    }

    void MemFile::Unref() {
        bool do_delete = false;
        {
            mutex_.Lock();
            --refs_;
            assert(refs_ >= 0);
            if (refs_ <= 0) {
                do_delete = true;
            }
            mutex_.Unlock();
        }

        if (do_delete) {
            delete this;
        }
    }

    Status MemFile::Read(uint64_t offset, size_t n, Slice *result,
                         char *scratch) {
        mutex_.Lock();
        const uint64_t available = Size() - std::min(Size(), offset);
        size_t offset_ = static_cast<size_t>(offset);
        if (n > available) {
            n = static_cast<size_t>(available);
        }
        if (n == 0) {
            *result = Slice();
            mutex_.Unlock();
            return Status::OK();
        }
        if (scratch) {
            memcpy(scratch, &(data_[offset_]), n);
            *result = Slice(scratch, n);
        } else {
            *result = Slice(&(data_[offset_]), n);
        }
        mutex_.Unlock();
        return Status::OK();
    }

    Status MemFile::Write(uint64_t offset, const Slice &data) {
        mutex_.Lock();
        size_t offset_ = static_cast<size_t>(offset);
        if (offset + data.size() > data_.size()) {
            data_.resize(offset_ + data.size());
        }
        data_.replace(offset_, data.size(), data.data(), data.size());
        size_ = data_.size();
        modified_time_ = Now();
        mutex_.Unlock();
        return Status::OK();
    }

    Status MemFile::Append(const Slice &data) {
        mutex_.Lock();
        data_.append(data.data(), data.size());
        size_ = data_.size();
        modified_time_ = Now();
        mutex_.Unlock();
        return Status::OK();
    }

    Status MemFile::Fsync() {
        fsynced_bytes_ = size_.load();
        return Status::OK();
    }

    uint64_t MemFile::Now() {
        struct ::timeval now_timeval;
        ::gettimeofday(&now_timeval, nullptr);
        int64_t unix_time = now_timeval.tv_sec;
        return static_cast<uint64_t>(unix_time);
    }

    MemSequentialFile::MemSequentialFile(MemFile *file) : file_(file), pos_(0) {
        file_->Ref();
    }

    Status MemSequentialFile::Read(size_t n, Slice *result, char *scratch) {
        Status s = file_->Read(pos_, n, result, scratch);
        if (s.ok()) {
            pos_ += result->size();
        }
        return s;
    }

    Status MemSequentialFile::Skip(uint64_t n) {
        if (pos_ > file_->Size()) {
            return Status::IOError("pos_ > file_->Size()");
        }
        const uint64_t available = file_->Size() - pos_;
        if (n > available) {
            n = available;
        }
        pos_ += static_cast<size_t>(n);
        return Status::OK();
    }

    MemRandomAccessFile::MemRandomAccessFile(MemFile *file) : file_(
            file) { file_->Ref(); }


    Status MemRandomAccessFile::Read(const StoCBlockHandle &stoc_block_handle,
                                     uint64_t offset, size_t n, Slice *result,
                                     char *scratch) {
        return file_->Read(offset, n, result, scratch);
    }

    MemRandomRWFile::MemRandomRWFile(MemFile *file) : file_(
            file) { file_->Ref(); }

    Status
    MemRandomRWFile::Read(const StoCBlockHandle &stoc_block_handle, uint64_t offset,
                          size_t n, Slice *result,
                          char *scratch) {
        return file_->Read(offset, n, result, scratch);
    }

    MemWritableFile::MemWritableFile(MemFile *file)
            : file_(file) {
//        file_->Ref();
    }

    Status MemWritableFile::Append(const Slice &data) {
        return file_->Append(data);
    }

    Status MemWritableFile::Close() { return Status::OK(); }

    Status MemWritableFile::Flush() { return Status::OK(); }

    Status MemWritableFile::Sync() { return file_->Fsync(); }

    MemEnvFileLock::MemEnvFileLock(const std::string &fname) : fname_(
            fname) {}

}
