
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "subrange_manager.h"
#include "memtable.h"

namespace leveldb {
    SubRangeManager::SubRangeManager(leveldb::NovaCCMemFile *manifest_file,
                                     const std::string &dbname,
                                     leveldb::VersionSet *versions,
                                     const leveldb::Options &options,
                                     const leveldb::Comparator *user_comparator,
                                     std::vector<leveldb::MemTablePartition *> *partitioned_active_memtables,
                                     std::vector<uint32_t> *partitioned_imms)
            : manifest_file_(manifest_file), dbname_(dbname),
              versions_(versions), options_(options),
              user_comparator_(user_comparator),
              partitioned_active_memtables_(partitioned_active_memtables),
              partitioned_imms_(partitioned_imms) {
        latest_subranges_.store(new SubRanges);
    }


    int SubRangeManager::SearchSubranges(const leveldb::WriteOptions &options,
                                         const leveldb::Slice &key,
                                         const leveldb::Slice &val,
                                         leveldb::SubRange **subrange) {
        SubRanges *ref = latest_subranges_;
        ref->AssertSubrangeBoundary(user_comparator_);
        int subrange_id = -1;
        if (ref->subranges.size() == options_.num_memtable_partitions) {
            // steady state.
            bool found = ref->BinarySearchWithDuplicate(key,
                                                        options.rand_seed,
                                                        &subrange_id,
                                                        user_comparator_);
            if (found) {
                *subrange = &ref->subranges[subrange_id];
                return subrange_id;
            }
        }

        // Require updating boundary of subranges.
        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format("Put key {} {}", key.ToString(), ref->DebugString());

        range_lock_.Lock();
        SubRanges *new_subranges = nullptr;
        while (true) {
            ref = latest_subranges_;
            bool found = ref->BinarySearchWithDuplicate(key,
                                                        options.rand_seed,
                                                        &subrange_id,
                                                        user_comparator_);
            if (found &&
                ref->subranges.size() == options_.num_memtable_partitions) {
                break;
            }
            new_subranges = new SubRanges(*ref);
            if (found &&
                new_subranges->subranges.size() <
                options_.num_memtable_partitions) {
                SubRange &sr = new_subranges->subranges[subrange_id];
                if (sr.lower_inclusive() &&
                    user_comparator_->Compare(sr.lower(), key) == 0) {
                    // Cannot split this subrange since it will create a new subrange with no keys.
                    break;
                }

                // find the key but not enough subranges. Split this subrange.
                SubRange new_sr = {}; // [k, sr.upper)
                Range new_range = {};
                new_range.lower.assign(key.ToString());
                new_range.upper.assign(sr.upper());
                // Update the last tiny range of sr to [tr.lower, k)
                Range &last = sr.tiny_ranges[sr.tiny_ranges.size() - 1];
                last.upper.assign(key.ToString());

                // update stats.
                last.ninserts /= 2;
                new_range.ninserts = last.ninserts;
                new_sr.UpdateStats();
                sr.UpdateStats();

                // insert new subrange.
                new_sr.tiny_ranges.push_back(std::move(new_range));
                subrange_id += 1;
                new_subranges->subranges.insert(
                        new_subranges->subranges.begin() + subrange_id,
                        std::move(new_sr));
                break;
            }

            if (!found &&
                new_subranges->subranges.size() ==
                options_.num_memtable_partitions) {
                // didn't find the key but we have enough subranges.
                // Extend the lower boundary of the next subrange to include this key.
                if (new_subranges->subranges[subrange_id].IsSmallerThanLower(
                        key,
                        user_comparator_)) {
                    SubRange &sr = new_subranges->subranges[subrange_id];
                    Range &first = sr.tiny_ranges[0];
                    first.lower.assign(key.ToString());
                    if (sr.num_duplicates > 0) {
                        int i = subrange_id - 1;
                        while (i >= 0) {
                            if (new_subranges->subranges[i].Equals(sr,
                                                                   user_comparator_)) {
                                i--;
                            } else {
                                break;
                            }
                        }

                        int start = i + 1;
                        for (int i = 0; i < sr.num_duplicates; i++) {
                            SubRange &dup = new_subranges->subranges[start + i];
                            Range &dup_first = dup.tiny_ranges[0];
                            dup_first.lower.assign(key.ToString());
                        }
                    }
                } else {
                    RDMA_ASSERT(
                            new_subranges->subranges[subrange_id].IsGreaterThanUpper(
                                    key,
                                    user_comparator_));
                    SubRange &sr = new_subranges->subranges[subrange_id];
                    Range &last = sr.tiny_ranges[sr.tiny_ranges.size() - 1];
                    last.upper.assign(key.ToString());
                    last.upper_inclusive = true;
                    if (sr.num_duplicates > 0) {
                        int i = subrange_id - 1;
                        while (i >= 0) {
                            if (new_subranges->subranges[i].Equals(sr,
                                                                   user_comparator_)) {
                                i--;
                            } else {
                                break;
                            }
                        }

                        int start = i + 1;
                        for (int i = 0; i < sr.num_duplicates; i++) {
                            SubRange &dup = new_subranges->subranges[start + i];
                            Range &dup_last = dup.tiny_ranges[
                                    sr.tiny_ranges.size() - 1];
                            dup_last.upper = key.ToString();
                            dup_last.upper_inclusive = true;
                        }
                    }
                }
                break;
            }

            // not found and not enough subranges.
            // no subranges. construct a point.
            if (new_subranges->subranges.empty()) {
                SubRange sr = {};
                Range new_range = {};
                new_range.lower.assign(key.ToString());
                new_range.upper.assign(
                        std::to_string(new_range.lower_int() + 1));
                sr.tiny_ranges.push_back(std::move(new_range));
                new_subranges->subranges.push_back(std::move(sr));
                subrange_id = 0;
                break;
            }

            if (new_subranges->subranges.size() == 1 &&
                new_subranges->subranges[0].IsAPoint(user_comparator_)) {
                if (new_subranges->subranges[0].IsSmallerThanLower(key,
                                                                   user_comparator_)) {
                    Range &first = new_subranges->subranges[0].tiny_ranges[0];
                    first.lower.assign(key.ToString());
                } else {
                    Range &last = new_subranges->subranges[0].tiny_ranges[
                            new_subranges->subranges[0].tiny_ranges.size() -
                            1];
                    uint64_t u = 0;
                    nova::str_to_int(key.data(), &u, key.size());
                    last.upper.assign(std::to_string(u + 1));
                }
                subrange_id = 0;
                break;
            }

            // key is less than the smallest user key.
            if (new_subranges->subranges[subrange_id].IsSmallerThanLower(key,
                                                                         user_comparator_)) {
                SubRange sr = {};
                Range new_range = {};
                new_range.lower.assign(key.ToString());
                new_range.upper.assign(
                        new_subranges->subranges[subrange_id].lower());
                sr.tiny_ranges.push_back(std::move(new_range));
                new_subranges->subranges.insert(
                        new_subranges->subranges.begin(), std::move(sr));
                break;
            }

            // key is greater than the largest user key.
            if (new_subranges->subranges[subrange_id].IsGreaterThanUpper(key,
                                                                         user_comparator_)) {
                SubRange sr = {};
                Range new_range = {};
                new_range.lower.assign(
                        new_subranges->subranges[subrange_id].upper());
                uint64_t u = 0;
                nova::str_to_int(key.data(), &u, key.size());
                new_range.upper.assign(std::to_string(u + 1));
                sr.tiny_ranges.push_back(std::move(new_range));
                new_subranges->subranges.push_back(std::move(sr));
                subrange_id = new_subranges->subranges.size() - 1;
                break;
            }
            RDMA_ASSERT(false);
        }

        if (new_subranges) {
            latest_subranges_.store(new_subranges);
            ref = new_subranges;
        }
        range_lock_.Unlock();

        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format("Expand subranges at {} for key {} ",
                           subrange_id, key.ToString());

        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format("Expand subranges at {} for key {} subranges:{}",
                           subrange_id, key.ToString(), ref->DebugString());
        *subrange = &ref->subranges[subrange_id];
        return subrange_id;
    }

    void SubRangeManager::PerformSubrangeMajorReorg(SubRanges *latest,
                                                    double processed_writes) {
        std::vector<double> insertion_rates;
        std::vector<SubRange> &subranges = latest->subranges;
        // Perform major reorg.
        std::vector<std::vector<AtomicMemTable *>> subrange_imms;
        uint32_t nslots = (options_.num_memtables -
                           options_.num_memtable_partitions) /
                          options_.num_memtable_partitions;
        uint32_t remainder = (options_.num_memtables -
                              options_.num_memtable_partitions) %
                             options_.num_memtable_partitions;
        uint32_t slot_id = 0;
        for (int i = 0; i < options_.num_memtable_partitions; i++) {
            std::vector<AtomicMemTable *> memtables;

            (*partitioned_active_memtables_)[i]->mutex.Lock();
            MemTable *m = (*partitioned_active_memtables_)[i]->memtable;
            if (m) {
                RDMA_ASSERT(
                        versions_->mid_table_mapping_[m->memtableid()]->RefMemTable());
                memtables.push_back(
                        versions_->mid_table_mapping_[m->memtableid()]);
            }
            uint32_t slots = nslots;
            if (remainder > 0) {
                slots += 1;
                remainder--;
            }
            for (int j = 0; j < slots; j++) {
                uint32_t imm_id = (*partitioned_imms_)[slot_id + j];
                if (imm_id == 0) {
                    continue;
                }
                auto *imm = versions_->mid_table_mapping_[imm_id]->RefMemTable();
                if (imm) {
                    memtables.push_back(versions_->mid_table_mapping_[imm_id]);
                }
            }
            slot_id += slots;
            (*partitioned_active_memtables_)[i]->mutex.Unlock();

            subrange_imms.push_back(memtables);
        }

        // We have all memtables now.
        // Determine the number of samples we retrieve from each subrange.
        uint32_t sample_size_per_subrange = UINT32_MAX;
        std::vector<uint32_t> subrange_nputs;
        std::vector<std::vector<uint32_t>> subrange_mem_nputs;
        for (int i = 0; i < subrange_imms.size(); i++) {
            uint32_t nputs = 0;
            std::vector<uint32_t> ns;
            for (int j = 0; j < subrange_imms[i].size(); j++) {
                uint32_t n = subrange_imms[i][j]->nentries_.load(
                        std::memory_order_relaxed);
                ns.push_back(n);

                nputs += n;
            }
            subrange_mem_nputs.push_back(ns);
            subrange_nputs.push_back(nputs);
            if (nputs > 100) {
                sample_size_per_subrange = std::min(sample_size_per_subrange,
                                                    nputs);
            }
        }

        // Sample from each memtable.
        sample_size_per_subrange = (double) (sample_size_per_subrange) *
                                   options_.subrange_reorg_sampling_ratio;
        auto comp = [&](const std::string &a, const std::string &b) {
            return user_comparator_->Compare(a, b);
        };
        std::map<uint64_t, double> userkey_rate;
        double total_rate = 0;

        for (int i = 0; i < subrange_imms.size(); i++) {
            SubRange &sr = subranges[i];
            uint32_t total_puts = subrange_nputs[i];
            double insertion_ratio = subranges[i].insertion_ratio;

            for (int j = 0; j < subrange_imms[i].size(); j++) {
                AtomicMemTable *mem = subrange_imms[i][j];
                uint32_t sample_size = 0;
                if (total_puts <= sample_size_per_subrange) {
                    // sample everything.
                    sample_size = sample_size_per_subrange;
                } else {
                    sample_size = ((double) subrange_mem_nputs[i][j] /
                                   (double) total_puts) *
                                  sample_size_per_subrange;
                }

                uint32_t samples = 0;
                leveldb::Iterator *it = mem->memtable_->NewIterator(
                        TraceType::MEMTABLE, AccessCaller::kUncategorized,
                        sample_size);
                it->SeekToFirst();
                ParsedInternalKey ik;
                while (it->Valid() && samples < sample_size) {
                    RDMA_ASSERT(ParseInternalKey(it->key(), &ik));
                    uint64_t k = 0;
                    nova::str_to_int(ik.user_key.data(), &k,
                                     ik.user_key.size());
                    userkey_rate[k] += insertion_ratio;
                    total_rate += insertion_ratio;
                    samples += 1;
                    it->Next();
                }

                RDMA_LOG(rdmaio::INFO)
                    << fmt::format("Sample {} {} {} {} from mid-{} subrange-{}",
                                   samples, sample_size,
                                   subrange_mem_nputs[i][j], total_puts,
                                   mem->memtable_->memtableid(), i);
                delete it;
            }
        }

        if (userkey_rate.size() <= options_.num_memtable_partitions * 2) {
            num_skipped_major_reorgs++;
            // Unref all immutable memtables.
            for (int i = 0; i < subrange_imms.size(); i++) {
                for (int j = 0; j < subrange_imms[i].size(); j++) {
                    subrange_imms[i][j]->Unref(dbname_);
                }
            }
            delete latest;
            return;
        }

        last_major_reorg_seq_ = versions_->last_sequence_;
        last_minor_reorg_seq_ = versions_->last_sequence_;
        num_major_reorgs++;
        subranges.clear();

        // First, construct subranges with each subrange containing one tiny
        // range.
        std::vector<Range> tmp_subranges;
        ConstructRanges(userkey_rate, total_rate, 0, 10000000,
                        options_.num_memtable_partitions, true, &tmp_subranges);
        // Second, break each subrange that contains more than one value into
        // alpha tiny ranges.
        for (int i = 0; i < tmp_subranges.size(); i++) {
            std::map<uint64_t, double> sub_userkey_rate;
            uint64_t lower = tmp_subranges[i].lower_int();
            uint64_t upper = tmp_subranges[i].upper_int();
            SubRange sr = {};
            if (upper - lower > 1) {
                double subTotalShare = 0;
                for (auto it : userkey_rate) {
                    if (it.first < lower) {
                        continue;
                    }
                    if (it.first >= upper) {
                        continue;
                    }
                    subTotalShare += it.second;
                    sub_userkey_rate[it.first] = it.second;
                }
                ConstructRanges(sub_userkey_rate, subTotalShare,
                                lower, upper,
                                0, //alpha,
                                false, &sr.tiny_ranges);
            } else {
                sr.tiny_ranges.push_back(std::move(tmp_subranges[i]));
            }
            subranges.push_back(std::move(sr));
        }
        latest->AssertSubrangeBoundary(user_comparator_);

        // Unref all immutable memtables.
        for (int i = 0; i < subrange_imms.size(); i++) {
            for (int j = 0; j < subrange_imms[i].size(); j++) {
                subrange_imms[i][j]->Unref(dbname_);
            }
        }
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("major at {} puts with {} keys: {}",
                           processed_writes,
                           userkey_rate.size(), latest->DebugString());
        VersionEdit edit;
        for (int i = 0; i < subranges.size(); i++) {
            edit.UpdateSubRange(i, subranges[i].tiny_ranges,
                                subranges[i].num_duplicates);
        }
        latest_subranges_.store(latest);
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
    }

    void
    SubRangeManager::MoveShareDuplicateSubranges(SubRanges *latest, int index) {
//        SubRange &sr = latest->subranges[index];
//        int nDuplicates = 0;
//        int start = -1;
//        int end = -1;
//        for (int i = 0; i < latest->subranges.size(); i++) {
//            if (!latest->subranges[i].Equals(sr, user_comparator_)) {
//                continue;
//            }
//            end = i;
//            if (start == -1) {
//                start = i;
//            }
//        }
//        RDMA_ASSERT(end - start + 1 == sr.num_duplicates);
//        nDuplicates = sr.num_duplicates - 1;
//        double share = sr.ninserts / nDuplicates;
//        for (int i = start; i <= end; i++) {
//            SubRange &dup = latest->subranges[i];
//            dup.ninserts += share;
//            dup.num_duplicates -= 1;
//            if (nDuplicates == 1) {
//                dup.num_duplicates = 0;
//            }
//        }
    }

    void SubRangeManager::moveShare(SubRanges *latest, int index) {
        SubRange &sr = latest->subranges[index];
        int lower = sr.tiny_ranges[0].lower_int();
        int nDuplicates = 0;
        int start = -1;
        int end = -1;
        for (int i = 0; i < latest->subranges.size(); i++) {
            SubRange &r = latest->subranges[i];
            if (r.num_duplicates == 0) {
                continue;
            }
            RDMA_ASSERT(r.tiny_ranges.size() == 1);
            if (r.tiny_ranges[0].lower_int() != lower) {
                continue;
            }
            end = i;
            if (start == -1) {
                start = i;
            }
        }

        nDuplicates = end - start;
        double share = sr.ninserts / nDuplicates;
        for (int i = start; i <= end; i++) {
            SubRange &r = latest->subranges[i];
            if (nDuplicates == 1) {
                r.tiny_ranges[0].is_duplicated = false;
            }
            r.tiny_ranges[0].ninserts += share;
        }
    }

    void SubRangeManager::ConstructRanges(
            const std::map<uint64_t, double> &userkey_rate, double total_rate,
            uint64_t lower, uint64_t upper, uint32_t num_ranges_to_construct,
            bool is_constructing_subranges,
            std::vector<leveldb::Range> *ranges) {
        RDMA_ASSERT(upper - lower > 1);
        RDMA_ASSERT(num_ranges_to_construct > 1);
        double share_per_range = total_rate / (double) num_ranges_to_construct;
        double fair_rate = total_rate / (double) num_ranges_to_construct;
        double total = total_rate;
        double sum = 0;

        uint32_t current_lower = lower;
        uint32_t current_upper = 0;
        for (auto it : userkey_rate) {
            RDMA_ASSERT(it.first >= lower);
            RDMA_ASSERT(it.first < upper);
            double rate = it.second;
            if (rate >= fair_rate && is_constructing_subranges) {
                RDMA_LOG(rdmaio::INFO)
                    << fmt::format("hot key {}:{}:{}", it.first, rate / total,
                                   fair_rate / total);
                // close the current subrange.
                if (current_lower < it.first) {
                    current_upper = it.first;
                    Range r = {};
                    r.lower = std::to_string(current_lower);
                    r.upper = std::to_string((current_upper));
                    (*ranges).push_back(std::move(r));
                }

                int nDuplicates = (int) std::ceil(rate / fair_rate);
                for (int i = 0; i < nDuplicates; i++) {
                    Range r = {};
                    r.lower = std::to_string(it.first);
                    r.upper = std::to_string(it.first + 1);
                    r.is_duplicated = true;
                    (*ranges).push_back(std::move(r));
                }
                current_lower = it.first + 1;
                total_rate -= it.second;
                sum = 0;
                share_per_range =
                        total_rate / (num_ranges_to_construct - ranges->size());
                continue;
            }

            if (sum + rate > share_per_range) {
                if (current_lower == it.first) {
                    current_upper = it.first + 1;
                    Range r = {};
                    r.lower = std::to_string(current_lower);
                    r.upper = std::to_string(current_upper);
                    (*ranges).push_back(std::move(r));

                    current_lower = it.first + 1;
                    if (ranges->size() + 1 == num_ranges_to_construct) {
                        break;
                    }
                    sum = 0;
                    total_rate -= rate;
                    share_per_range = total_rate / (num_ranges_to_construct -
                                                    ranges->size());
                    continue;
                } else {
                    current_upper = it.first;
                    Range r = {};
                    r.lower = std::to_string(current_lower);
                    r.upper = std::to_string(current_upper);
                    (*ranges).push_back(std::move(r));

                    current_lower = it.first;
                    if (ranges->size() + 1 == num_ranges_to_construct) {
                        break;
                    }
                    sum = 0;
                    share_per_range = total_rate / (num_ranges_to_construct -
                                                    ranges->size());
                }
            }
            sum += rate;
            total_rate -= rate;
        }

        if (is_constructing_subranges) {
            Range r = {};
            r.lower = std::to_string(current_lower);
            ranges->push_back(std::move(r));
            RDMA_ASSERT(ranges->size() == num_ranges_to_construct);
        } else {
            if (current_lower < upper) {
                Range r = {};
                r.lower = std::to_string(current_lower);
                ranges->push_back(std::move(r));
            }
            RDMA_ASSERT(ranges->size() <= num_ranges_to_construct);
        }

        (*ranges)[0].lower = std::to_string(lower);
        (*ranges->rbegin()).upper = std::to_string(upper);

        int prior_upper = -1;
        for (int i = 0; i < ranges->size(); i++) {
            Range &sr = (*ranges)[i];
            uint64_t lower = sr.lower_int();
            uint64_t upper = sr.upper_int();
            if (prior_upper != -1 && prior_upper - lower > 1) {
                RDMA_ASSERT(lower > prior_upper);
            }
            prior_upper = lower;
        }
    }

    bool
    SubRangeManager::MinorReorgDestroyDuplicates(SubRanges *latest,
                                                 int subrange_id, bool force) {
        SubRange &sr = latest->subranges[subrange_id];
        double fair_ratio = 1.0 / (double) (options_.num_memtable_partitions);
        if (sr.num_duplicates == 0) {
            return false;
        }

        if (force) {
            moveShare(latest, subrange_id);
            latest->subranges.erase(latest->subranges.begin() + subrange_id);
            return true;
        }

        double percent = sr.insertion_ratio / fair_ratio;
        if (percent >= 0.5) {
            return false;
        }
        num_minor_reorgs_for_dup++;
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Destroy subrange {}", subrange_id);
        // destroy this duplicate.
        moveShare(latest, subrange_id);
        latest->subranges.erase(latest->subranges.begin() + subrange_id);
        return true;
    }

    bool SubRangeManager::PerformSubrangeMinorReorgDuplicate(
            leveldb::SubRanges *latest, int subrange_id,
            double total_inserts) {
        double fair_ratio = 1.0 / (double) (options_.num_memtable_partitions);
        SubRange &sr = latest->subranges[subrange_id];
        if (sr.tiny_ranges.size() != 1) {
            return false;
        }
        int lower = sr.tiny_ranges[0].lower_int();
        int upper = sr.tiny_ranges[0].upper_int();

        if (upper - lower != 1) {
            return false;
        }
        if (sr.insertion_ratio <= 1.5 * fair_ratio) {
            return false;
        }
        int nDuplicates = (int) std::floor(sr.insertion_ratio / fair_ratio);
        if (nDuplicates == 0) {
            return false;
        }

        last_minor_reorg_seq_ = total_inserts;
        num_minor_reorgs_for_dup++;

        // Create new duplicate subranges.
        sr.tiny_ranges[0].is_duplicated = true;
        double remainingSum = sr.ninserts;
        for (int i = 0; i < nDuplicates; i++) {
            SubRange newSR = {};
            Range tinyRange = {};

            tinyRange.lower = std::to_string(lower);
            tinyRange.upper = std::to_string(upper);
            tinyRange.ninserts = sr.ninserts / (nDuplicates + 1);
            remainingSum -= tinyRange.ninserts;
            tinyRange.insertion_ratio = tinyRange.ninserts / total_inserts;
            tinyRange.is_duplicated = true;
            newSR.tiny_ranges.push_back(std::move(tinyRange));
            latest->subranges.insert(latest->subranges.begin() + subrange_id,
                                     newSR);
        }
        sr.tiny_ranges[0].ninserts = remainingSum;

        // Remove subranges if the number of subranges exceeds max.
        // For each removed subrange, move its tiny ranges to its neighboring
        // subranges.
        while (latest->subranges.size() > options_.num_memtable_partitions) {
            // remove the subrange that has the lowest insertion rate.
            double minShare = UINT64_MAX;
            int minRangeId = 0;
            for (int i = 0; i < latest->subranges.size(); i++) {
                SubRange &minSR = latest->subranges[i];
                // Skip the new subranges.
                if (minSR.tiny_ranges.size() == 1) {
                    if (minSR.tiny_ranges[0].lower_int() == lower) {
                        continue;
                    }
                }
                if (minSR.insertion_ratio < minShare) {
                    minShare = minSR.insertion_ratio;
                    minRangeId = i;
                }
            }

            if (latest->subranges[minRangeId].num_duplicates > 0) {
                MinorReorgDestroyDuplicates(latest, minRangeId, true);
                continue;
            }

            int left = minRangeId - 1;
            int right = minRangeId + 1;
            bool mergeLeft = true;
            if (left >= 0 && right < latest->subranges.size()) {
                if (latest->subranges[left].insertion_ratio <
                    latest->subranges[right].insertion_ratio) {
                    mergeLeft = true;
                } else {
                    mergeLeft = false;
                }
            } else if (left >= 0) {
                mergeLeft = true;
            } else {
                mergeLeft = false;
            }

            if (mergeLeft && latest->subranges[left].num_duplicates > 0) {
                MinorReorgDestroyDuplicates(latest, left, true);
            } else if (!mergeLeft &&
                       latest->subranges[right].num_duplicates > 0) {
                MinorReorgDestroyDuplicates(latest, right, true);
            } else {
                int nranges = latest->subranges[minRangeId].tiny_ranges.size();
                int pushedRanges = PushTinyRanges(latest, minRangeId, false);
                RDMA_ASSERT(pushedRanges == nranges);
            }
        }
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("minor duplicate subrange {}", subrange_id);
        return true;
    }

    int
    SubRangeManager::PushTinyRanges(leveldb::SubRanges *latest, int subrangeId,
                                    bool stopWhenBelowFair) {
        // Push its tiny ranges to its neighbors.
        int left = subrangeId - 1;
        int right = subrangeId + 1;
        double fair_ratio = 1.0 / (double) (options_.num_memtable_partitions);

        SubRange *leftSR = nullptr;
        SubRange *rightSR = nullptr;
        if (left >= 0) {
            leftSR = &latest->subranges[left];
        }
        if (right < latest->subranges.size()) {
            rightSR = &latest->subranges[right];
        }
        int moved = 0;
        SubRange *minSR = &latest->subranges[subrangeId];
        // move to left
        // move the remaining
        double leftShare = UINT64_MAX;
        double rightShare = UINT64_MAX;
        if (leftSR != nullptr && leftSR->num_duplicates == 0) {
            leftShare = leftSR->insertion_ratio;
        }
        if (rightSR != nullptr && rightSR->num_duplicates == 0) {
            rightShare = rightSR->insertion_ratio;
        }
        if (leftShare != UINT64_MAX || rightShare != UINT64_MAX) {
            while (!minSR->tiny_ranges.empty()) {
                Range &first = minSR->tiny_ranges[0];
                Range &last = minSR->tiny_ranges[minSR->tiny_ranges.size() - 1];
                bool pushLeft = false;

                if (leftShare + first.insertion_ratio < rightShare
                                                        +
                                                        last.insertion_ratio) {
                    pushLeft = true;
                }

                if (stopWhenBelowFair) {
                    if (pushLeft) {
                        if (minSR->insertion_ratio
                            - first.insertion_ratio < fair_ratio && moved > 0) {
                            return moved;
                        }
                    } else {
                        if (minSR->insertion_ratio - last.insertion_ratio <
                            fair_ratio
                            && moved > 0) {
                            return moved;
                        }
                    }
                }

                if (pushLeft) {
                    // move to left.
                    leftShare += first.insertion_ratio;
                    leftSR->tiny_ranges.push_back(std::move(first));
                    minSR->tiny_ranges.erase(minSR->tiny_ranges.begin());
                } else {
                    // move to right.
                    rightShare += last.insertion_ratio;
                    rightSR->tiny_ranges.insert(rightSR->tiny_ranges.begin(),
                                                std::move(last));
                    minSR->tiny_ranges.erase(minSR->tiny_ranges.end() - 1);
                }
                moved++;
            }
        }
        if (minSR->tiny_ranges.empty()) {
            latest->subranges.erase(latest->subranges.begin() + subrangeId);
        }
        return moved;
    }

    bool
    SubRangeManager::MinorRebalancePush(leveldb::SubRanges *latest, int index,
                                        double total_inserts) {
        SubRange &sr = latest->subranges[index];
        double fair_ratio = 1.0 / (double) (options_.num_memtable_partitions);
        double totalRemoveInserts = (sr.insertion_ratio - fair_ratio)
                                    * total_inserts;

        if (totalRemoveInserts >= sr.ninserts) {
            return false;
        }

        if (sr.tiny_ranges.size() == 1) {
            return false;
        }

        RDMA_ASSERT(sr.insertion_ratio > fair_ratio);
        // Distribute the load across adjacent subranges.
        last_minor_reorg_seq_ = total_inserts;

        RDMA_LOG(rdmaio::INFO)
            << fmt::format("{} push minor before {}", index, sr.DebugString());
        // push tiny ranges.
        if (PushTinyRanges(latest, index, true) == 0) {
            return false;
        }
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("{} push minor after {}", index, sr.DebugString());

        return true;
    }

    void
    SubRangeManager::PerformSubRangeReorganization(double processed_writes) {
        RDMA_ASSERT(versions_->last_sequence_ > SUBRANGE_WARMUP_NPUTS);
        SubRanges *ref = latest_subranges_;
        double fair_ratio = 1.0 / (double) options_.num_memtable_partitions;

        // Make a copy.
        range_lock_.Lock();
        auto latest = new SubRanges(*ref);
        range_lock_.Unlock();
        std::vector<SubRange> &subranges = latest->subranges;
        double total_inserts = 0;
        double unfair_subranges = 0;

        for (int i = 0; i < subranges.size(); i++) {
            SubRange &sr = subranges[i];
            total_inserts += sr.ninserts;
        }

        uint64_t now = versions_->last_sequence_.load(
                std::memory_order_relaxed);
        RDMA_ASSERT(total_inserts <= now)
            << fmt::format("{},{}", total_inserts, now);

        if (now - last_minor_reorg_seq_ > SUBRANGE_MINOR_REORG_INTERVAL) {
            int pivot = 0;
            bool success = false;
            while (pivot < subranges.size()) {
                SubRange &sr = subranges[pivot];
                if (MinorReorgDestroyDuplicates(latest, pivot, false)) {
                    // no need to update pivot since it already points to the
                    // next range.
                    success = true;
                } else if (PerformSubrangeMinorReorgDuplicate(latest,
                                                              pivot,
                                                              total_inserts)) {
                    // go to the next subrange.
                    for (int i = 0; i < subranges.size(); i++) {
                        SubRange &other = subranges[i];
                        if (other.Equals(sr, user_comparator_)) {
                            pivot = i;
                        }
                    }
                    pivot++;
                    success = true;
                } else {
                    pivot++;
                }
            }
            if (success) {
                last_minor_reorg_seq_ = now;
            }
        }

        double most_unfair = 0;
        uint32_t most_unfair_subrange = 0;
        for (int i = 0; i < subranges.size(); i++) {
            SubRange &sr = subranges[i];
            sr.insertion_ratio = (double) sr.ninserts / total_inserts;
            double diff =
                    (sr.insertion_ratio - fair_ratio) * 100.0 / fair_ratio;
            if (std::abs(diff) > SUBRANGE_REORG_DIFF_FROM_FAIR_THRESHOLD &&
                !sr.IsAPoint(user_comparator_)) {
                unfair_subranges += 1;
            }

            bool eligble_for_minor = sr.ninserts > 100 &&
                                     now - last_minor_reorg_seq_ >
                                     SUBRANGE_MINOR_REORG_INTERVAL;
            if (eligble_for_minor) {
                if (diff > SUBRANGE_REORG_DIFF_FROM_FAIR_THRESHOLD &&
                    diff > most_unfair) {
                    most_unfair = diff;
                    most_unfair_subrange = i;
                }
            }
        }
        if (unfair_subranges / (double) subranges.size() >
            SUBRANGE_MAJOR_REORG_THRESHOLD &&
            now - last_major_reorg_seq_ >
            SUBRANGE_MAJOR_REORG_INTERVAL) {
            PerformSubrangeMajorReorg(latest, processed_writes);
            return;
        } else if (most_unfair != 0) {
            // Perform minor.
            MinorRebalancePush(latest, most_unfair_subrange, total_inserts);
            return;
        }
        delete latest;
    }


}