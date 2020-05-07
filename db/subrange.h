
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_SUBRANGE_H
#define LEVELDB_SUBRANGE_H

#include "leveldb/slice.h"
#include "nova/nova_common.h"
#include "leveldb/comparator.h"

namespace leveldb {

    struct SubRange {
        Slice lower;
        Slice upper;
        bool lower_inclusive = true;
        bool upper_inclusive = false;
        uint32_t num_duplicates = 0;

        uint64_t ninserts = 0;
        double insertion_ratio = 0;

        static Slice Copy(Slice from) {
            char *c = new char[from.size()];
            for (int i = 0; i < from.size(); i++) {
                c[i] = from[i];
            }
            return Slice(c, from.size());
        }

        std::string DebugString();

        bool Equals(const SubRange &other, const Comparator *comparator);

        bool
        IsSmallerThanLower(const Slice &key, const Comparator *comparator);

        bool
        IsGreaterThanLower(const Slice &key, const Comparator *comparator);

        bool
        IsGreaterThanUpper(const Slice &key, const Comparator *comparator);

        bool IsAPoint(const Comparator *comparator);
    };

    class SubRanges {
    public:
        ~SubRanges();

        SubRanges() {}

        SubRanges(const SubRanges &other);

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
