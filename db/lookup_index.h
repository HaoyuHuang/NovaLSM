
//
// Created by Haoyu Huang on 5/19/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_LOOKUP_INDEX_H
#define LEVELDB_LOOKUP_INDEX_H

#include <atomic>
#include "leveldb/slice.h"

#define MAX_BUCKETS 1000000000

namespace leveldb {
    struct TableLocation {
        std::atomic<uint32_t> memtable_id;
    };

    class LookupIndex {
    public:
        uint64_t Lookup(const Slice &key, uint64_t hash);

        void Insert(const Slice &key, uint64_t hash, uint32_t memtableid);

        void CAS(const Slice &key, uint64_t hash, uint32_t current_memtableid,
                 uint32_t new_memtableid);

    private:
        TableLocation table_locator_[MAX_BUCKETS];
    };
}



#endif //LEVELDB_LOOKUP_INDEX_H
