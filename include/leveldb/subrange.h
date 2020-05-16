
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_SUBRANGE_H
#define LEVELDB_SUBRANGE_H

#include <vector>
#include "nova/logging.hpp"

#include "leveldb/slice.h"
#include "leveldb/comparator.h"

namespace leveldb {

    struct Range {
        std::string lower = {};
        std::string upper = {};
        bool lower_inclusive = true;
        bool upper_inclusive = false;
        bool is_duplicated = false;
        uint64_t ninserts = 0;
        double insertion_ratio = 0;

        uint32_t Encode(char *buf) const;

        bool Decode(Slice *input);

        std::string DebugString() const;

        bool Equals(const Range &other, const Comparator *comparator) const;

        bool
        IsSmallerThanLower(const Slice &key,
                           const Comparator *comparator) const;

        bool
        IsGreaterThanLower(const Slice &key,
                           const Comparator *comparator) const;

        bool
        IsGreaterThanUpper(const Slice &key,
                           const Comparator *comparator) const;

        bool IsAPoint(const Comparator *comparator) const;

        uint64_t lower_int() const;
        uint64_t upper_int() const;
    };


    struct SubRange {
        std::vector<Range> tiny_ranges;
        uint32_t num_duplicates = 0;
        uint64_t ninserts = 0;
        double insertion_ratio = 0;

        uint32_t decoded_subrange_id = 0;

        void UpdateStats();

        bool BinarySearch(const leveldb::Slice &key,
                          int *tinyrange_id,
                          const Comparator *user_comparator);

        const std::string &lower() const {
            RDMA_ASSERT(!tiny_ranges.empty());
            return tiny_ranges[0].lower;
        }

        bool lower_inclusive() const {
            RDMA_ASSERT(!tiny_ranges.empty());
            return tiny_ranges[0].lower_inclusive;
        }

        const std::string &upper() const {
            RDMA_ASSERT(!tiny_ranges.empty());
            return tiny_ranges[tiny_ranges.size() - 1].upper;
        }

        bool upper_inclusive() const {
            RDMA_ASSERT(!tiny_ranges.empty());
            return tiny_ranges[tiny_ranges.size() - 1].upper_inclusive;
        }

        uint32_t Encode(char *buf, uint32_t subrange_id) const;

        bool Decode(Slice *input);

        uint32_t EncodeForCompaction(char *buf, uint32_t subrange_id) const;

        bool DecodeForCompaction(Slice *input);

        std::string DebugString() const;

        bool Equals(const SubRange &other, const Comparator *comparator) const;

        bool
        IsSmallerThanLower(const Slice &key,
                           const Comparator *comparator) const;

        bool
        IsGreaterThanLower(const Slice &key,
                           const Comparator *comparator) const;

        bool
        IsGreaterThanUpper(const Slice &key,
                           const Comparator *comparator) const;

        bool IsAPoint(const Comparator *comparator);
    };

    class SubRanges {
    public:
        ~SubRanges();

        SubRanges() = default;

        SubRanges(const SubRanges &other);

        explicit SubRanges(const std::vector<SubRange> &other);

        std::vector<SubRange> subranges;

        bool BinarySearch(const leveldb::Slice &key,
                          int *subrange_id,
                          const Comparator *user_comparator);

        bool
        BinarySearchWithDuplicate(const leveldb::Slice &key,
                                  unsigned int *rand_seed, int *subrange_id,
                                  const Comparator *user_comparator);

        std::string DebugString();

        void AssertSubrangeBoundary(const Comparator *comparator);
    };
}


#endif //LEVELDB_SUBRANGE_H
