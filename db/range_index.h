
//
// Created by Haoyu Huang on 6/15/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_RANGE_INDEX_H
#define LEVELDB_RANGE_INDEX_H

#include <atomic>
#include <set>
#include <mutex>

#include "leveldb/subrange.h"
#include "leveldb/slice.h"
#include "version_set.h"

namespace leveldb {
    struct RangeTables {
        std::set<uint32_t> memtable_ids;
        std::set<uint64_t> l0_sstable_ids;
    };

    class RangeIndexManager;

    class RangeIndex {
    public:
        RangeIndex() = default;

        RangeIndex(RangeIndex *current, uint32_t version_id);

        void Ref();

        void UnRef();

        std::vector<Range> ranges;
        std::vector<RangeTables> range_tables;
        const uint32_t lsm_version_id = 0;
    private:
        friend class RangeIndexManager;

        std::mutex mutex_;
        bool is_deleted_ = false;
        RangeIndex *prev_ = nullptr;
        RangeIndex *next_ = nullptr;
        int refs_ = 0;
    };

    class RangeIndexManager {
    public:
        RangeIndexManager(VersionSet *versions,
                          const Comparator *user_comparator);

        void Initialize(RangeIndex *init);

        void AppendNewVersion(
                const SubRange &sr,
                uint32_t new_memtable_id,
                bool add_new_memtable,
                const std::vector<uint64_t> &removed_sstables,
                const std::vector<uint32_t> &removed_memtables,
                uint32_t lsm_version_id,
                const Comparator *user_comparator);

        void DeleteObsoleteVersions();

        RangeIndex *current();
    private:
        friend class RangeIndex;

        std::mutex mutex_;
        RangeIndex *dummy_ = nullptr;
        RangeIndex *current_ = nullptr;
        VersionSet *versions_ = nullptr;
        const Comparator *user_comparator_ = nullptr;
    };
}

#endif //LEVELDB_RANGE_INDEX_H
