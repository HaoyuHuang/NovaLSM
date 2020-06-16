
//
// Created by Haoyu Huang on 2/12/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <fmt/core.h>
#include "mock_rtable.h"

#include "bench_common.h"

namespace nova {
    MockRTable::MockRTable(leveldb::Env *env, std::string table_path,
                           uint32_t max_rtable_size,
                           uint32_t max_num_rtables) : env_(env),
                                                       table_path_(table_path),
                                                       max_rtable_size_(
                                                               max_rtable_size),
                                                       max_num_rtables_(
                                                               max_num_rtables) {
        leveldb::EnvFileMetadata meta = {};
        leveldb::Status s = env_->NewReadWriteFile(
                fmt::format("{}/rtable-{}", table_path_, rtable_id_), meta,
                &writable_file_);
        NOVA_ASSERT(s.ok()) << s.ToString();
    }


    bool MockRTable::Read(uint64_t size) {
        if (live_files_.size() <= 0) {
            return false;
        }

        auto file = live_fds_[rand() % live_fds_.size()];
        uint64_t offset = rand() % (max_rtable_size_ - 4096);
        leveldb::Slice result;
        NOVA_ASSERT(file->Read({}, offset, 4096, &result, buf).ok());
    }

    void MockRTable::CreateNewFile() {
        if (size_ >= max_rtable_size_) {

//            writable_file_->Sync();
//            leveldb::Status s = writable_file_->Close();
//            RDMA_ASSERT(s.ok()) << s.ToString();

            live_files_.push_back(rtable_id_);
            live_fds_.push_back(writable_file_);

            rtable_id_ += 1;
            leveldb::EnvFileMetadata meta = {};
            leveldb::Status s = env_->NewReadWriteFile(
                    fmt::format("{}/rtable-{}", table_path_, rtable_id_), meta,
                    &writable_file_);
            NOVA_ASSERT(s.ok()) << s.ToString();

            size_ = 0;
        }


        if (live_files_.size() == max_num_rtables_) {
            uint32_t id = live_files_.front();
            auto file = live_fds_.front();
            file->Close();
            leveldb::Status s = env_->DeleteFile(
                    fmt::format("{}/rtable-{}", table_path_, id));
            NOVA_ASSERT(s.ok()) << s.ToString();
            live_files_.erase(live_files_.begin());
            live_fds_.erase(live_fds_.begin());
        }

    }

    void MockRTable::Persist(char *buf, uint64_t size) {
        mutex_.lock();
        CreateNewFile();
        size_ += size;
        leveldb::Status s = writable_file_->Append(leveldb::Slice(buf, size));
        NOVA_ASSERT(s.ok()) << s.ToString();
        s = writable_file_->Sync();
        NOVA_ASSERT(s.ok()) << s.ToString();
        mutex_.unlock();
    }
}