
//
// Created by Haoyu Huang on 5/19/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "common/nova_console_logging.h"
#include "lookup_index.h"

namespace leveldb {
    uint64_t LookupIndex::Lookup(const leveldb::Slice &key, uint64_t hash) {
        NOVA_ASSERT(hash >= 0 && hash <= MAX_BUCKETS);
        TableLocation &loc = table_locator_[hash % MAX_BUCKETS];
        return loc.memtable_id.load();
    }

    void LookupIndex::Insert(const leveldb::Slice &key, uint64_t hash,
                             uint32_t memtableid) {
        NOVA_ASSERT(hash >= 0 && hash <= MAX_BUCKETS) << hash;
        TableLocation &loc = table_locator_[hash % MAX_BUCKETS];
        loc.memtable_id.store(memtableid);
    }

    void LookupIndex::CAS(const leveldb::Slice &key, uint64_t hash,
                          uint32_t current_memtableid,
                          uint32_t new_memtableid) {
        NOVA_ASSERT(hash >= 0 && hash <= MAX_BUCKETS) << hash;
        TableLocation &loc = table_locator_[hash % MAX_BUCKETS];
        loc.memtable_id.compare_exchange_strong(current_memtableid,
                                                new_memtableid);
    }
}