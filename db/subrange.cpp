
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "subrange.h"

namespace leveldb {
    std::string SubRange::DebugString() {
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
        output += lower.ToString();
        output += ",";
        output += upper.ToString();
        if (upper_inclusive) {
            up++;
            output += "]";
        } else {
            output += ")";
        }

        output += fmt::format(":{}, {}%, d={} keys={}", ninserts,
                              (uint32_t) insertion_ratio * 100.0,
                              num_duplicates,
                              up - low);
        return output;
    }

    bool SubRange::Equals(const SubRange &other, const Comparator *comparator) {
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
    SubRange::IsSmallerThanLower(const Slice &key, const Comparator *comparator) {
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
    SubRange::IsGreaterThanLower(const Slice &key, const Comparator *comparator) {
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
    SubRange::IsGreaterThanUpper(const Slice &key, const Comparator *comparator) {
        int comp = comparator->Compare(key, upper);
        if (comp > 0) {
            return true;
        }
        if (comp == 0 && !upper_inclusive) {
            return true;
        }
        return false;
    }

    bool SubRange::IsAPoint(const Comparator *comparator) {
        if (lower_inclusive && upper_inclusive &&
            comparator->Compare(lower, upper) == 0) {
            return true;
        }
        return false;
    }

    SubRanges::~SubRanges() {
        for (int i = 0; i < subranges.size(); i++) {
            delete subranges[i].lower.data();
            delete subranges[i].upper.data();
        }
    }

    SubRanges::SubRanges(const SubRanges &other) {
        subranges.resize(other.subranges.size());
        for (int i = 0; i < subranges.size(); i++) {
            subranges[i].lower = SubRange::Copy(
                    other.subranges[i].lower);
            subranges[i].upper = SubRange::Copy(
                    other.subranges[i].upper);
            subranges[i].lower_inclusive = other.subranges[i].lower_inclusive;
            subranges[i].upper_inclusive = other.subranges[i].upper_inclusive;
            subranges[i].ninserts = other.subranges[i].ninserts;
            subranges[i].num_duplicates = other.subranges[i].num_duplicates;
        }
    }

    std::string SubRanges::DebugString() {
        std::string output;
//                output += std::to_string(subranges.size());
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
            RDMA_ASSERT(!sr.IsSmallerThanLower(sr.upper, comparator))
                << DebugString();
            if (!prior_upper.empty()) {
                if (upper_inclusive) {
                    if (sr.lower_inclusive) {
                        RDMA_ASSERT(
                                sr.IsSmallerThanLower(prior_upper,
                                                      comparator))
                            << fmt::format("assert {} {}", i,
                                           DebugString());
                    } else {
                        RDMA_ASSERT(comparator->Compare(prior_upper,
                                                        sr.lower) <= 0)
                            << fmt::format("assert {} {}", i,
                                           DebugString());
                    }
                } else {
                    // Does not include upper.
                    RDMA_ASSERT(comparator->Compare(prior_upper,
                                                    sr.lower) <= 0)
                        << fmt::format("assert {} {}", i,
                                       DebugString());
                }
            }
            prior_upper = sr.upper;
            upper_inclusive = sr.upper_inclusive;
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
        if (subranges.size() > 0) {
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