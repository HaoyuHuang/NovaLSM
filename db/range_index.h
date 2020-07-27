
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

        uint32_t Encode(char *buf);

        void Decode(Slice* buf);
    };

    class RangeIndexManager;

    class RangeIndex {
    public:
        RangeIndex(ScanStats *scan_stats);

        RangeIndex(ScanStats *scan_stats, uint32_t version_id,
                   uint32_t lsm_version_id);

        RangeIndex(ScanStats *scan_stats, RangeIndex *current,
                   uint32_t version_id,
                   uint32_t lsm_version_id);

        uint32_t Encode(char *buf);

        void Decode(Slice* buf);

        bool Ref();

        void UnRef();

        std::vector<Range> ranges_;
        std::vector<RangeTables> range_tables_;
        uint32_t lsm_version_id_ = 0;
        ScanStats *scan_stats_ = nullptr;
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
    GetRangeIndexFragmentIterator(void *arg, void *arg2,
                                  BlockReadContext context,
                                  const ReadOptions &options,
                                  const Slice &file_value,
                                  std::string *next_key);

    class RangeIndexIterator : public Iterator {
    public:
        RangeIndexIterator(const InternalKeyComparator *icmp,
                           const RangeIndex *range_index, ScanStats *stats)
                : icmp_(icmp), range_index_(range_index),
                  index_(range_index_->ranges_.size()),
                  stats_(stats) {  // Marks as invalid
        }

        bool Valid() const override {
            return index_ < range_index_->ranges_.size();
        }

        void Seek(const Slice &target) override {
            BinarySearch(range_index_->ranges_, target, &index_,
                         icmp_->user_comparator());
        }

        void SkipToNextUserKey(const Slice &target) override;

        void SeekToFirst() override { index_ = 0; }

        void SeekToLast() override {
            index_ = range_index_->ranges_.empty() ? 0 :
                     range_index_->ranges_.size() - 1;
        }

        void Next() override {
            assert(Valid());
            index_++;
        }

        void Prev() override {
            assert(Valid());
            if (index_ == 0) {
                index_ = range_index_->ranges_.size();  // Marks as invalid
            } else {
                index_--;
            }
        }

        Slice key() const override {
            assert(Valid());
            return range_index_->ranges_[index_].upper;
        }

        Slice value() const override {
            assert(Valid());
            EncodeFixed32(value_buf_, index_);
            return Slice(value_buf_, 4);
        }

        Status status() const override { return Status::OK(); }

    private:
        const InternalKeyComparator *icmp_;
        const RangeIndex *range_index_;
        ScanStats *stats_ = nullptr;
        int index_;
        // Backing store for value().  Holds the file number and size.
        mutable char value_buf_[4];
    };

    struct RangeIndexVersionEdit {
        SubRange *sr = nullptr;
        uint32_t new_memtable_id = 0;
        bool add_new_memtable = false;
        std::unordered_map<uint32_t, uint64_t> replace_memtables;
        std::unordered_map<uint64_t, std::vector<uint64_t>> replace_l0_sstables;
        std::vector<uint64_t> removed_l0_sstables;
        std::set<uint32_t> removed_memtables;
        uint32_t lsm_version_id = 0;
    };

    class RangeIndexManager {
    public:
        RangeIndexManager(ScanStats *scan_stats, VersionSet *versions,
                          const Comparator *user_comparator);

        void Initialize(RangeIndex *init);

        void AppendNewVersion(ScanStats *scan_stats, const RangeIndexVersionEdit &edit);

        void AppendNewVersion(RangeIndex *new_range_idx);

        void DeleteObsoleteVersions();

        RangeIndex *current();

    private:
        friend class RangeIndex;

        std::mutex mutex_;
        uint32_t range_index_version_seq_id_ = 0;
        RangeIndex *first_ = nullptr;
        RangeIndex *last_ = nullptr;
        RangeIndex *current_;
        VersionSet *versions_ = nullptr;
        const Comparator *user_comparator_ = nullptr;
    };
}

#endif //LEVELDB_RANGE_INDEX_H
