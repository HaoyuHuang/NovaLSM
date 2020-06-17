
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

        std::string DebugString() const;
    };

    class RangeIndexManager;

    class RangeIndex {
    public:
        RangeIndex() = default;

        RangeIndex(uint32_t version_id, uint32_t lsm_version_id);

        RangeIndex(RangeIndex *current, uint32_t version_id,
                   uint32_t lsm_version_id);

        void Ref();

        void UnRef();

        std::vector<Range> ranges;
        std::vector<RangeTables> range_tables;
        const uint32_t lsm_version_id = 0;

        std::string DebugString() const;

    private:
        friend class RangeIndexManager;

        std::mutex mutex_;
        uint32_t version_id_ = 0;
        bool is_deleted_ = false;
        RangeIndex *prev_ = nullptr;
        RangeIndex *next_ = nullptr;
        int refs_ = 0;
    };

    Iterator *
    GetRangeIndexFragmentIterator(void *arg, BlockReadContext context,
                                  const ReadOptions &options,
                                  const Slice &file_value,
                                  std::string *next_key);

    class RangeIndexIterator : public Iterator {
    public:
        RangeIndexIterator(const InternalKeyComparator *icmp,
                           const RangeIndex *range_index)
                : icmp_(icmp), range_index_(range_index),
                  index_(range_index_->ranges.size()) {  // Marks as invalid
        }

        bool Valid() const override {
            return index_ < range_index_->ranges.size();
        }

        void Seek(const Slice &target) override {
            BinarySearch(range_index_->ranges, target, &index_,
                         icmp_->user_comparator());
        }

        void SeekToFirst() override { index_ = 0; }

        void SeekToLast() override {
            index_ = range_index_->ranges.empty() ? 0 :
                     range_index_->ranges.size() - 1;
        }

        void Next() override {
            assert(Valid());
            index_++;
        }

        void Prev() override {
            assert(Valid());
            if (index_ == 0) {
                index_ = range_index_->ranges.size();  // Marks as invalid
            } else {
                index_--;
            }
        }

        Slice key() const override {
            assert(Valid());
            return range_index_->ranges[index_].upper;
        }

        Slice value() const override {
            assert(Valid());
            const auto &range = range_index_->ranges[index_];
            const auto &range_table = range_index_->range_tables[index_];
            uint32_t msg_size = EncodeStr(value_buf_, range.lower);
            for (uint32_t memtableid : range_table.memtable_ids) {
                msg_size += EncodeFixed32(value_buf_ + msg_size,
                                          memtableid);
            }
            msg_size += EncodeFixed32(value_buf_ + msg_size, 0);
            for (uint64_t sstableid : range_table.l0_sstable_ids) {
                msg_size += EncodeFixed64(value_buf_ + msg_size, sstableid);
            }
            NOVA_ASSERT(msg_size <= 1024) << msg_size;
            NOVA_LOG(rdmaio::INFO)
                << fmt::format("RangeIndexScan Encode: {} {} {} {}", index_,
                               msg_size, range.DebugString(),
                               range_table.DebugString());
            return Slice(value_buf_, msg_size);
        }

        Status status() const override { return Status::OK(); }

    private:
        const InternalKeyComparator *icmp_;
        const RangeIndex *range_index_;
        int index_;
        // Backing store for value().  Holds the file number and size.
        mutable char value_buf_[1024];
    };

    struct RangeIndexVersionEdit {
        SubRange *sr = nullptr;
        uint32_t new_memtable_id = 0;
        bool add_new_memtable = false;
        std::unordered_map<uint32_t, uint64_t> replace_memtables;
        std::unordered_map<uint32_t, std::vector<uint64_t>> replace_l0_sstables;
        std::vector<uint64_t> removed_l0_sstables;
        std::set<uint32_t> removed_memtables;
        uint32_t lsm_version_id = 0;
    };

    class RangeIndexManager {
    public:
        RangeIndexManager(VersionSet *versions,
                          const Comparator *user_comparator);

        void Initialize(RangeIndex *init);

        void AppendNewVersion(const RangeIndexVersionEdit &edit);

        void DeleteObsoleteVersions();

        RangeIndex *current();

    private:
        friend class RangeIndex;

        std::mutex mutex_;

        uint32_t range_index_seq_id_ = 1;
        RangeIndex *first_ = nullptr;
        RangeIndex *last_ = nullptr;
        RangeIndex *current_ = nullptr;
        VersionSet *versions_ = nullptr;
        const Comparator *user_comparator_ = nullptr;
    };
}

#endif //LEVELDB_RANGE_INDEX_H
