
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
// It assumes keys are integers.
//

#include "leveldb/subrange.h"
#include "common/nova_common.h"

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
        msg_size += EncodeFixed32(dst + msg_size, num_duplicates);
        return msg_size;
    }

    bool Range::Decode(leveldb::Slice *input) {
        return DecodeStr(input, &lower) &&
               DecodeStr(input, &upper) &&
               DecodeBool(input, &lower_inclusive) &&
               DecodeBool(input, &upper_inclusive) &&
               DecodeFixed32(input, &num_duplicates);
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
                              num_duplicates, up - low);
        return output;
    }

    bool Range::Equals(const Range &other, const Comparator *comparator) const {
        if (lower_inclusive != other.lower_inclusive) {
            return false;
        }
        if (upper_inclusive != other.upper_inclusive) {
            return false;
        }
        if (num_duplicates != other.num_duplicates) {
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
        uint32_t nranges = 0;
        NOVA_ASSERT(DecodeFixed32(input, &decoded_subrange_id));
        NOVA_ASSERT(DecodeFixed32(input, &num_duplicates));
        NOVA_ASSERT(DecodeFixed32(input, &nranges));
        for (int i = 0; i < nranges; i++) {
            Range r = {};
            NOVA_ASSERT(r.Decode(input));
            tiny_ranges.push_back(std::move(r));
        }
        return true;
    }

    uint32_t
    SubRange::EncodeForCompaction(char *buf, uint32_t subrange_id) const {
        uint32_t msg_size = 0;
        msg_size += EncodeFixed32(buf + msg_size, subrange_id);
        msg_size += EncodeFixed32(buf + msg_size, num_duplicates);
        Range r;
        r.lower = tiny_ranges[0].lower;
        r.lower_inclusive = tiny_ranges[0].lower_inclusive;
        r.upper = tiny_ranges[tiny_ranges.size() - 1].upper;
        r.upper_inclusive = tiny_ranges[tiny_ranges.size() - 1].upper_inclusive;
        r.num_duplicates = num_duplicates;
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
        output += "[";
        output += tiny_ranges[0].lower;
        output += ",";
        output += tiny_ranges[tiny_ranges.size() - 1].upper;
        output += ") ";
        output += fmt::format("c:{} {} {} ", start_tid, end_tid,
                              merge_memtables_without_flushing);
        output += "keys:";
        output += std::to_string(
                tiny_ranges[tiny_ranges.size() - 1].upper_int() -
                tiny_ranges[0].lower_int());
        output += " tiny:";
        for (auto &range : tiny_ranges) {
            output += range.DebugString();
            output += ",";
        }
        return output;
    }

    void SubRange::UpdateStats(double num_inserts_since_last_major) {
        ninserts = 0;
        for (auto &r : tiny_ranges) {
            ninserts += r.ninserts;
            if (num_inserts_since_last_major > 0) {
                r.insertion_ratio = r.ninserts / num_inserts_since_last_major;
            }
        }
        insertion_ratio = ninserts / num_inserts_since_last_major;
    }

    int SubRange::GetCompactionThreadId(std::atomic_int_fast32_t *rr_id,
                                        bool *_merge_memtables_without_flushing) const {
        *_merge_memtables_without_flushing = merge_memtables_without_flushing;
        if (start_tid == end_tid) {
            return start_tid;
        }
        int range = end_tid - start_tid + 1;
        int index = rr_id->fetch_add(1, std::memory_order_relaxed) % range;
        NOVA_ASSERT(start_tid + index <= end_tid);
        return start_tid + index;
    }

    int SubRange::keys() const {
        return tiny_ranges[tiny_ranges.size() - 1].upper_int() -
               tiny_ranges[0].lower_int();
    }

    bool SubRange::RangeEquals(const SubRange &other, const Comparator *comparator) const {
        if (tiny_ranges[0].lower_int() != other.tiny_ranges[0].lower_int()) {
            return false;
        }
        if (tiny_ranges[tiny_ranges.size() - 1].upper_int() !=
            other.tiny_ranges[other.tiny_ranges.size() - 1].upper_int()) {
            return false;
        }
        return true;
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
        NOVA_ASSERT(!tiny_ranges.empty());
        return tiny_ranges[0].IsSmallerThanLower(key, comparator);
    }

    bool
    SubRange::IsGreaterThanLower(const Slice &key,
                                 const Comparator *comparator) const {
        NOVA_ASSERT(!tiny_ranges.empty());
        return tiny_ranges[0].IsGreaterThanLower(key,
                                                 comparator);
    }

    bool
    SubRange::IsGreaterThanUpper(const Slice &key,
                                 const Comparator *comparator) const {
        NOVA_ASSERT(!tiny_ranges.empty());
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
        for (int i = 0; i < other.size(); i++) {
            SubRange sr = {};
            const SubRange &other_sr = other[i];
            sr.ninserts = other_sr.ninserts;
            sr.num_duplicates = other_sr.num_duplicates;
            for (int j = 0; j < other_sr.tiny_ranges.size(); j++) {
                Range r = {};
                const Range &o = other_sr.tiny_ranges[j];
                r.lower = o.lower;
                r.upper = o.upper;
                r.lower_inclusive = o.lower_inclusive;
                r.upper_inclusive = o.upper_inclusive;
                r.num_duplicates = o.num_duplicates;
                r.ninserts = o.ninserts;
                r.prior_subrange_id = o.prior_subrange_id;
                sr.tiny_ranges.push_back(std::move(r));
            }
            subranges.push_back(std::move(sr));
        }
    }

    std::string SubRanges::DebugString() const {
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
        // Assert duplicates.
        auto it = subranges.begin();
        while (it != subranges.end()) {
            NOVA_ASSERT(!it->tiny_ranges.empty()) << DebugString();
            if (it->num_duplicates > 0) {
                NOVA_ASSERT(it->tiny_ranges.size() == 1) << DebugString();
                NOVA_ASSERT(it->IsAPoint(comparator)) << DebugString();
                uint64_t lk = it->tiny_ranges[0].lower_int();
                for (int i = 0; i < it->num_duplicates - 1; i++) {
                    NOVA_ASSERT(it->tiny_ranges.size() == 1) << DebugString();
                    NOVA_ASSERT(it->IsAPoint(comparator)) << DebugString();
                    uint64_t other = it->tiny_ranges[0].lower_int();
                    NOVA_ASSERT(lk == other)
                        << fmt::format("{} {} {}", lk, other, DebugString());
                    NOVA_ASSERT(it->tiny_ranges[0].num_duplicates ==
                                it->num_duplicates) << DebugString();
                    it++;
                }
                it++;
                continue;
            }
            it++;
        }
        // Assert boundaries.
        int prior_lower = -1;
        int prior_upper = -1;
        bool isPriorDup = false;
        for (int i = 0; i < subranges.size(); i++) {
            for (int j = 0; j < subranges[i].tiny_ranges.size(); j++) {
                NOVA_ASSERT(subranges[i].num_duplicates ==
                            subranges[i].tiny_ranges[j].num_duplicates)
                    << DebugString();
                Range &range = subranges[i].tiny_ranges[j];
                NOVA_ASSERT(range.lower_inclusive) << DebugString();
                NOVA_ASSERT(!range.upper_inclusive) << DebugString();
                if (prior_lower == -1) {
                    prior_lower = range.lower_int();
                    prior_upper = range.upper_int();
                    isPriorDup = range.num_duplicates > 0;
                    continue;
                }
                if (range.lower_int() == prior_lower) {
                    NOVA_ASSERT(range.upper_int() == prior_upper)
                        << DebugString();
                    NOVA_ASSERT(range.num_duplicates > 0) << DebugString();
                    NOVA_ASSERT(isPriorDup) << DebugString();
                } else {
                    NOVA_ASSERT(range.lower_int() >= prior_upper)
                        << DebugString();
                    NOVA_ASSERT(range.upper_int() > range.lower_int())
                        << fmt::format("{} {}", range.DebugString(),
                                       DebugString());
                    prior_lower = range.lower_int();
                    prior_upper = range.upper_int();
                    isPriorDup = range.num_duplicates > 0;
                }
            }
        }
    }

    bool SubRanges::BinarySearch(
            const leveldb::Slice &key, int *subrange_id,
            const Comparator *user_comparator) const {
        int l = 0, r = subranges.size() - 1;
        while (l <= r) {
            int m = l + (r - l) / 2;
            const SubRange &subrange = subranges[m];
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
//        if (!subranges.empty()) {
//            if (l == subranges.size()) {
//                l--;
//            }
//            RDMA_ASSERT(l < subranges.size()) << "";
//            if (subranges[l].IsGreaterThanUpper(key, user_comparator)) {
//                RDMA_ASSERT(l == subranges.size() - 1);
//            } else {
//                RDMA_ASSERT(subranges[l].IsSmallerThanLower(key,
//                                                            user_comparator));
//            }
//        }
//        *subrange_id = l;
        return false;
    }

    bool
    SubRanges::BinarySearchWithDuplicate(const leveldb::Slice &key,
                                         unsigned int *rand_seed,
                                         int *subrange_id,
                                         const Comparator *user_comparator) const {
        bool found = BinarySearch(key, subrange_id, user_comparator);
        if (!found) {
            return false;
        }

        NOVA_ASSERT(*subrange_id >= 0);
        const SubRange &sr = subranges[*subrange_id];
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
        {
            const SubRange &sr = subranges[*subrange_id];
            NOVA_ASSERT(!sr.IsSmallerThanLower(key, user_comparator) &&
                        !sr.IsGreaterThanUpper(key, user_comparator))
                << fmt::format("key:{} id:{} ranges:{}", key.ToString(),
                               *subrange_id, DebugString());
        }
        return true;
    }

    uint32_t SubRanges::Encode(char *buf) {
        uint32_t msg_size = 0;
        msg_size += EncodeFixed32(buf + msg_size, subranges.size());
        for (int i = 0; i < subranges.size(); i++) {
            msg_size += subranges[i].Encode(buf + msg_size, i);
        }
        return msg_size;
    }

    bool SubRanges::Decode(Slice *buf) {
        uint32_t num_subranges = 0;
        NOVA_ASSERT(DecodeFixed32(buf, &num_subranges));
        NOVA_LOG(rdmaio::INFO) << fmt::format("Decoded number of subranges: {}", num_subranges);
        for (int i = 0; i < num_subranges; i++) {
            SubRange sr = {};
            NOVA_ASSERT(sr.Decode(buf));
            subranges.push_back(sr);
        }
        return true;
    }

}