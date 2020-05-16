
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "leveldb/subrange.h"
#include "nova/nova_common.h"

namespace leveldb {

    uint64_t Range::lower_int() const {
        uint64_t low = 0;
        nova::str_to_int(lower.data(), &low, lower.size());
        return low;
    }

    uint64_t Range::upper_int() const {
        uint64_t up = 0;
        nova::str_to_int(upper.data(), &up, upper.size());
        return up;
    }

    uint32_t Range::Encode(char *dst) const {
        uint32_t msg_size = 0;
        msg_size += EncodeStr(dst + msg_size, lower);
        msg_size += EncodeStr(dst + msg_size, upper);
        msg_size += EncodeBool(dst + msg_size, lower_inclusive);
        msg_size += EncodeBool(dst + msg_size, upper_inclusive);
        return msg_size;
    }

    bool Range::Decode(leveldb::Slice *input) {
        return DecodeStr(input, &lower) &&
               DecodeStr(input, &upper) &&
               DecodeBool(input, &lower_inclusive) &&
               DecodeBool(input, &upper_inclusive);
    }

    std::string Range::DebugString() const {
        std::string output;
        uint64_t low;
        uint64_t up;
        nova::str_to_int(lower.data(), &low, lower.size());
        nova::str_to_int(upper.data(), &up, upper.size());

        if (lower_inclusive) {
            output += "[";
        } else {
            low++;
            output += "(";
        }
        output += lower;
        output += ",";
        output += upper;
        if (upper_inclusive) {
            up++;
            output += "]";
        } else {
            output += ")";
        }

        output += fmt::format(":{}, {}%, d={} keys={}", ninserts,
                              (uint32_t) insertion_ratio * 100.0,
                              is_duplicated,
                              up - low);
        return output;
    }

    bool Range::Equals(const Range &other, const Comparator *comparator) const {
        if (lower_inclusive != other.lower_inclusive) {
            return false;
        }
        if (upper_inclusive != other.upper_inclusive) {
            return false;
        }
        if (is_duplicated != other.is_duplicated) {
            return false;
        }
        if (comparator->Compare(lower, other.lower) != 0) {
            return false;
        }
        if (comparator->Compare(upper, other.upper) != 0) {
            return false;
        }
        return true;
    }

    bool
    Range::IsSmallerThanLower(const Slice &key,
                              const Comparator *comparator) const {
        int comp = comparator->Compare(key, lower);
        if (comp < 0) {
            return true;
        }
        if (comp == 0 && !lower_inclusive) {
            return true;
        }
        return false;
    }

    bool
    Range::IsGreaterThanLower(const Slice &key,
                              const Comparator *comparator) const {
        int comp = comparator->Compare(key, lower);
        if (comp > 0) {
            return true;
        }
        if (comp == 0 && !lower_inclusive) {
            return true;
        }
        return false;
    }

    bool
    Range::IsGreaterThanUpper(const Slice &key,
                              const Comparator *comparator) const {
        int comp = comparator->Compare(key, upper);
        if (comp > 0) {
            return true;
        }
        if (comp == 0 && !upper_inclusive) {
            return true;
        }
        return false;
    }

    bool Range::IsAPoint(const Comparator *comparator) const {
        if (upper_int() - lower_int() == 1) {
            return true;
        }
//        if (lower_inclusive && upper_inclusive &&
//            comparator->Compare(lower, upper) == 0) {
//            return true;
//        }
        return false;
    }

    uint32_t SubRange::Encode(char *buf, uint32_t subrange_id) const {
        uint32_t msg_size = 0;
        msg_size += EncodeFixed32(buf + msg_size, subrange_id);
        msg_size += EncodeFixed32(buf + msg_size, num_duplicates);
        msg_size += EncodeFixed32(buf + msg_size, tiny_ranges.size());
        for (auto &range : tiny_ranges) {
            msg_size += range.Encode(buf + msg_size);
        }
        return msg_size;
    }

    bool SubRange::Decode(Slice *input) {
        if (!DecodeFixed32(input, &decoded_subrange_id)) {
            return false;
        }
        if (!DecodeFixed32(input, &num_duplicates)) {
            return false;
        }
        uint32_t nranges = 0;
        if (!DecodeFixed32(input, &nranges)) {
            return false;
        }
        for (int i = 0; i < nranges; i++) {
            Range r = {};
            if (!r.Decode(input)) {
                return false;
            }
            tiny_ranges.push_back(std::move(r));
        }
    }

    uint32_t
    SubRange::EncodeForCompaction(char *buf, uint32_t subrange_id) const {
        uint32_t msg_size = 0;
        msg_size += EncodeFixed32(buf + msg_size, subrange_id);
        msg_size += EncodeFixed32(buf + msg_size, num_duplicates);
        Range r;
        r.lower = lower();
        r.lower_inclusive = lower_inclusive();
        r.upper = upper();
        r.upper_inclusive = upper_inclusive();
        msg_size += r.Encode(buf + msg_size);
        return msg_size;
    }

    bool SubRange::DecodeForCompaction(Slice *input) {
        if (!DecodeFixed32(input, &decoded_subrange_id)) {
            return false;
        }
        if (!DecodeFixed32(input, &num_duplicates)) {
            return false;
        }
        Range r = {};
        if (!r.Decode(input)) {
            return false;
        }
        tiny_ranges.push_back(std::move(r));
        return true;
    }

    std::string SubRange::DebugString() const {
        std::string output;
        output += std::to_string(num_duplicates);
        output += ",";
        for (auto &range : tiny_ranges) {
            output += range.DebugString();
            output += ",";
        }
        return output;
    }

    void SubRange::UpdateStats() {
        ninserts = 0;
        for (auto &r : tiny_ranges) {
            ninserts += r.ninserts;
        }
    }

    bool SubRange::BinarySearch(const leveldb::Slice &key, int *tinyrange_id,
                                const leveldb::Comparator *user_comparator) {
        int l = 0, r = tiny_ranges.size() - 1;
        while (l <= r) {
            int m = l + (r - l) / 2;
            Range &range = tiny_ranges[m];
            if (range.IsSmallerThanLower(key, user_comparator)) {
                r = m - 1;
            } else if (range.IsGreaterThanUpper(key, user_comparator)) {
                l = m + 1;
            } else {
                *tinyrange_id = m;
                return true;
            }
        }
        *tinyrange_id = -1;
        return false;
    }

    bool SubRange::Equals(const SubRange &other,
                          const Comparator *comparator) const {
        if (num_duplicates != other.num_duplicates) {
            return false;
        }
        if (tiny_ranges.size() != other.tiny_ranges.size()) {
            return false;
        }
        for (int i = 0; i < tiny_ranges.size(); i++) {
            if (!tiny_ranges[i].Equals(other.tiny_ranges[i], comparator)) {
                return false;
            }
        }
        return true;
    }

    bool
    SubRange::IsSmallerThanLower(const Slice &key,
                                 const Comparator *comparator) const {
        RDMA_ASSERT(!tiny_ranges.empty());
        return tiny_ranges[0].IsSmallerThanLower(key, comparator);
    }

    bool
    SubRange::IsGreaterThanLower(const Slice &key,
                                 const Comparator *comparator) const {
        RDMA_ASSERT(!tiny_ranges.empty());
        return tiny_ranges[0].IsGreaterThanLower(key,
                                                 comparator);
    }

    bool
    SubRange::IsGreaterThanUpper(const Slice &key,
                                 const Comparator *comparator) const {
        RDMA_ASSERT(!tiny_ranges.empty());
        return tiny_ranges[tiny_ranges.size() - 1].IsGreaterThanUpper(key,
                                                                      comparator);
    }

    bool SubRange::IsAPoint(const Comparator *comparator) {
        if (tiny_ranges.size() != 1) {
            return false;
        }
        return tiny_ranges[0].IsAPoint(comparator);
    }


    SubRanges::~SubRanges() {
    }

    SubRanges::SubRanges(const SubRanges &other) : SubRanges(other.subranges) {
    }

    SubRanges::SubRanges(const std::vector<SubRange> &other) {
        for (int i = 0; i < subranges.size(); i++) {
            SubRange sr = {};
            const SubRange &other_sr = other[i];
            sr.ninserts = other_sr.ninserts;
            sr.num_duplicates = other_sr.num_duplicates;
            for (int j = 0; j < sr.tiny_ranges.size(); j++) {
                Range r = {};
                const Range &o = other_sr.tiny_ranges[j];
                r.lower = o.lower;
                r.upper = o.upper;
                r.lower_inclusive = o.lower_inclusive;
                r.upper_inclusive = o.upper_inclusive;
                r.is_duplicated = o.is_duplicated;
                r.ninserts = o.ninserts;
                sr.tiny_ranges.push_back(std::move(r));
            }
            subranges.push_back(std::move(sr));
        }
    }

    std::string SubRanges::DebugString() {
        std::string output;
        output += "\n";
        for (int i = 0; i < subranges.size(); i++) {
            output += std::to_string(i) + " ";
            output += subranges[i].DebugString();
            output += "\n";
        }
        return output;
    }

    void SubRanges::AssertSubrangeBoundary(const Comparator *comparator) {
        if (subranges.empty()) {
            return;
        }
        Slice prior_upper = {};
        bool upper_inclusive = false;
        int i = 0;
        while (i < subranges.size()) {
            SubRange &sr = subranges[i];
            RDMA_ASSERT(!sr.IsSmallerThanLower(sr.upper(), comparator))
                << DebugString();
            if (!prior_upper.empty()) {
                if (upper_inclusive) {
                    if (sr.lower_inclusive()) {
                        RDMA_ASSERT(
                                sr.IsSmallerThanLower(prior_upper,
                                                      comparator))
                            << fmt::format("assert {} {}", i,
                                           DebugString());
                    } else {
                        RDMA_ASSERT(comparator->Compare(prior_upper,
                                                        sr.lower()) <= 0)
                            << fmt::format("assert {} {}", i,
                                           DebugString());
                    }
                } else {
                    // Does not include upper.
                    RDMA_ASSERT(comparator->Compare(prior_upper,
                                                    sr.lower()) <= 0)
                        << fmt::format("assert {} {}", i,
                                       DebugString());
                }
            }
            prior_upper = sr.upper();
            upper_inclusive = sr.upper_inclusive();
            i++;

            if (sr.num_duplicates > 0) {
                int ndup = 1;
                while (ndup < sr.num_duplicates) {
                    const SubRange &dup = subranges[i];
                    RDMA_ASSERT(sr.Equals(dup, comparator))
                        << fmt::format("assert {} {}", i,
                                       DebugString());;
                    i++;
                    ndup++;
                    RDMA_ASSERT(i < subranges.size());
                }
            }
        }
    }

    bool SubRanges::BinarySearch(
            const leveldb::Slice &key, int *subrange_id,
            const Comparator *user_comparator) {
        int l = 0, r = subranges.size() - 1;
        while (l <= r) {
            int m = l + (r - l) / 2;
            SubRange &subrange = subranges[m];

            if (subrange.IsSmallerThanLower(key, user_comparator)) {
                r = m - 1;
            } else if (subrange.IsGreaterThanUpper(key, user_comparator)) {
                l = m + 1;
            } else {
                *subrange_id = m;
                return true;
            }
        }
        // if we reach here, then element was
        // not present
        if (!subranges.empty()) {
            if (l == subranges.size()) {
                l--;
            }
            RDMA_ASSERT(l < subranges.size()) << "";
            if (subranges[l].IsGreaterThanUpper(key, user_comparator)) {
                RDMA_ASSERT(l == subranges.size() - 1);
            } else {
                RDMA_ASSERT(subranges[l].IsSmallerThanLower(key,
                                                            user_comparator));
            }
        }
        *subrange_id = l;
        return false;
    }

    bool
    SubRanges::BinarySearchWithDuplicate(const leveldb::Slice &key,
                                         unsigned int *rand_seed,
                                         int *subrange_id,
                                         const Comparator *user_comparator) {
        bool found = BinarySearch(key, subrange_id, user_comparator);
        if (!found) {
            return false;
        }

        RDMA_ASSERT(*subrange_id >= 0);
        SubRange &sr = subranges[*subrange_id];
        if (sr.num_duplicates == 0) {
            return true;
        }

        int i = (*subrange_id) - 1;
        while (i >= 0) {
            if (subranges[i].Equals(sr, user_comparator)) {
                i--;
            } else {
                break;
            }
        }

        if (rand_seed) {
            *subrange_id = i + 1 +
                           rand_r(rand_seed) % sr.num_duplicates;
        } else {
            // Return the first subrange.
            *subrange_id = i + 1;
        }

        sr = subranges[*subrange_id];
        RDMA_ASSERT(!sr.IsSmallerThanLower(key, user_comparator) &&
                    !sr.IsGreaterThanUpper(key, user_comparator))
            << fmt::format("key:{} id:{} ranges:{}", key.ToString(),
                           *subrange_id, DebugString());
        return true;
    }

}