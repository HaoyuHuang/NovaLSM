
//
// Created by Haoyu Huang on 5/19/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "lookup_index.h"

#include <fmt/core.h>
#include "common/nova_console_logging.h"
#include "util/coding.h"

namespace leveldb {

    LookupIndex::LookupIndex(uint32_t size) : size_(size) {
        table_locator_ = new TableLocation[size_];
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Create lookup index of size {}", size);
    }

    uint64_t LookupIndex::Lookup(const leveldb::Slice &key, uint64_t hash) {
//        NOVA_ASSERT(hash >= 0 && hash <= size_);
        TableLocation &loc = table_locator_[hash % size_];
        return loc.memtable_id.load();
    }

    void LookupIndex::Insert(const leveldb::Slice &key, uint64_t hash,
                             uint32_t memtableid) {
//        NOVA_ASSERT(hash >= 0 && hash <= size_) << hash;
        TableLocation &loc = table_locator_[hash % size_];
        loc.memtable_id.store(memtableid);
    }

    void LookupIndex::CAS(const leveldb::Slice &key, uint64_t hash,
                          uint32_t current_memtableid,
                          uint32_t new_memtableid) {
//        NOVA_ASSERT(hash >= 0 && hash <= size_) << hash;
        TableLocation &loc = table_locator_[hash % size_];
        loc.memtable_id.compare_exchange_strong(current_memtableid,
                                                new_memtableid);
    }

    uint32_t LookupIndex::Encode(char *buf) {
        uint32_t msg_size = 0;
        msg_size += EncodeFixed32(buf + msg_size, size_);
        NOVA_LOG(rdmaio::INFO) << fmt::format("Lookup index size: {}", size_);
        for (int i = 0; i < size_; i++) {
            TableLocation &loc = table_locator_[i];
            msg_size += EncodeFixed32(buf + msg_size, loc.memtable_id);
        }
        return msg_size;
    }

    void LookupIndex::Decode(Slice *buf) {
        uint32_t size = 0;
        NOVA_ASSERT(DecodeFixed32(buf, &size));
        NOVA_LOG(rdmaio::INFO) << fmt::format("Lookup index size: {}", size);
        for (int i = 0; i < size; i++) {
            uint32_t id;
            NOVA_ASSERT(DecodeFixed32(buf, &id));
            table_locator_[i].memtable_id = id;
        }
    }
}