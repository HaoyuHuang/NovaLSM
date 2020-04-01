
//
// Created by Haoyu Huang on 2/12/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_MOCK_RTABLE_H
#define LEVELDB_MOCK_RTABLE_H

#include "leveldb/env.h"
#include <list>

namespace nova {

    class MockRTable {
    public:
        MockRTable(leveldb::Env *env, std::string table_path,
                   uint32_t max_rtable_size,
                   uint32_t max_num_rtables);

        void Persist(char *buf, uint64_t size);

        bool Read(uint64_t size);

    private:
        void CreateNewFile();

        std::mutex mutex_;
        std::string table_path_;

        uint64_t size_ = 0;
        uint32_t max_num_rtables_;
        uint32_t max_rtable_size_;
        uint32_t rtable_id_ = 0;
        leveldb::Env *env_;
        leveldb::ReadWriteFile *writable_file_ = nullptr;
        std::vector<uint32_t> live_files_;
        std::vector<leveldb::ReadWriteFile *> live_fds_;
        char buf[4096];

    };
}


#endif //LEVELDB_MOCK_RTABLE_H
