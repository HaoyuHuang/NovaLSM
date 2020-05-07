
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
                if (sr.lower_inclusive &&
                    user_comparator_->Compare(sr.lower, key) == 0) {
                    // Cannot split this subrange since it will create a new subrange with no keys.
                    break;
                }

                // find the key but not enough subranges. Split this subrange.
                SubRange new_sr = {}; // [k, sr.upper]
                new_sr.lower = SubRange::Copy(key);
                new_sr.upper = SubRange::Copy(sr.upper);
                new_sr.lower_inclusive = true;
                new_sr.upper_inclusive = sr.upper_inclusive;
                // [lower, k)
                delete sr.upper.data();
                sr.upper = SubRange::Copy(new_sr.lower);
                sr.upper_inclusive = false;

                // update stats.
                sr.ninserts /= 2;
                new_sr.ninserts = sr.ninserts;

                // insert new subrange.
                subrange_id += 1;
                new_subranges->subranges.insert(
                        new_subranges->subranges.begin() + subrange_id,
                        new_sr);
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
                    sr.lower = SubRange::Copy(key);
                    sr.lower_inclusive = true;
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
                            dup.lower = SubRange::Copy(key);
                            dup.lower_inclusive = true;
                        }
                    }
                } else {
                    RDMA_ASSERT(
                            new_subranges->subranges[subrange_id].IsGreaterThanUpper(
                                    key,
                                    user_comparator_));
                    SubRange &sr = new_subranges->subranges[subrange_id];
                    sr.upper = SubRange::Copy(key);
                    sr.upper_inclusive = true;
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
                            dup.upper = SubRange::Copy(key);
                            dup.upper_inclusive = true;
                        }
                    }
                }
                break;
            }

            // not found and not enough subranges.
            // no subranges. construct a point.
            if (new_subranges->subranges.empty()) {
                SubRange sr = {};
                sr.lower = SubRange::Copy(key);
                sr.upper = SubRange::Copy(key);

                sr.lower_inclusive = true;
                sr.upper_inclusive = true;
                new_subranges->subranges.push_back(sr);
                subrange_id = 0;
                break;
            }

            if (new_subranges->subranges.size() == 1 &&
                new_subranges->subranges[0].IsAPoint(user_comparator_)) {
                if (new_subranges->subranges[0].IsSmallerThanLower(key,
                                                                   user_comparator_)) {
                    new_subranges->subranges[0].lower_inclusive = true;
                    new_subranges->subranges[0].lower = SubRange::Copy(key);
                } else {
                    new_subranges->subranges[0].upper_inclusive = true;
                    new_subranges->subranges[0].upper = SubRange::Copy(key);
                }
                subrange_id = 0;
                break;
            }

            // key is less than the smallest user key.
            if (new_subranges->subranges[subrange_id].IsSmallerThanLower(key,
                                                                         user_comparator_)) {
                SubRange sr = {};
                sr.lower = SubRange::Copy(key);
                sr.upper = SubRange::Copy(
                        new_subranges->subranges[subrange_id].lower);
                sr.lower_inclusive = true;
                sr.upper_inclusive = false;

                new_subranges->subranges.insert(
                        new_subranges->subranges.begin(), sr);
                break;
            }

            // key is greater than the largest user key.
            if (new_subranges->subranges[subrange_id].IsGreaterThanUpper(key,
                                                                         user_comparator_)) {
                SubRange sr = {};
                sr.lower = SubRange::Copy(
                        new_subranges->subranges[subrange_id].upper);
                sr.upper = SubRange::Copy(key);
                sr.lower_inclusive = false;
                sr.upper_inclusive = true;
                new_subranges->subranges.push_back(sr);
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
                        versions_->mid_table_mapping_[m->memtableid()].Ref());
                memtables.push_back(
                        &versions_->mid_table_mapping_[m->memtableid()]);
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
                auto *imm = versions_->mid_table_mapping_[imm_id].Ref();
                if (imm) {
                    memtables.push_back(&versions_->mid_table_mapping_[imm_id]);
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
        double share_per_subrange =
                total_rate / options_.num_memtable_partitions;
        double fair_share = 1.0 /
                            (double) options_.num_memtable_partitions;
        double fair_rate = fair_share * total_rate;
        double remaining_rate = total_rate;

        int index = 0;
        double sum = 0;

        std::string lower = subranges[0].lower.ToString();
        bool li = subranges[0].lower_inclusive;
        std::string upper = subranges[subranges.size() - 1].upper.ToString();
        bool ui = subranges[subranges.size() - 1].upper_inclusive;

        for (int i = 0; i < subranges.size(); i++) {
            delete subranges[i].lower.data();
            delete subranges[i].upper.data();
            subranges[i].lower = {};
            subranges[i].upper = {};
            subranges[i].num_duplicates = 0;
        }
        subranges.clear();
        subranges.resize(options_.num_memtable_partitions);
        subranges[0].lower_inclusive = li;
        subranges[0].lower = SubRange::Copy(lower);
        subranges[subranges.size() - 1].upper_inclusive = ui;
        subranges[subranges.size() - 1].upper = SubRange::Copy(upper);

        for (auto entry = userkey_rate.begin();
             entry != userkey_rate.end(); entry++) {
            double keyrate = entry->second;
            if (keyrate >= 1.5 * fair_rate) {
                // a hot key.
                RDMA_LOG(rdmaio::INFO)
                    << fmt::format("hot key {} rate:{} fair:{}", entry->first,
                                   keyrate, fair_rate);
                std::string userkey = std::to_string(entry->first);
                if (subranges[index].IsGreaterThanLower(userkey,
                                                        user_comparator_)) {
                    // close the current subrange.
                    subranges[index].upper = SubRange::Copy(userkey);
                    subranges[index].upper_inclusive = false;
                    index++;
                }

                int num_duplicates = std::ceil(keyrate / fair_rate);
                for (int i = 0; i < num_duplicates; i++) {
                    subranges[index].num_duplicates = num_duplicates;
                    subranges[index].lower = SubRange::Copy(userkey);
                    subranges[index].lower_inclusive = true;
                    subranges[index].upper = SubRange::Copy(userkey);
                    subranges[index].upper_inclusive = true;
                    index++;
                }
                entry++;
                auto next = entry;
                subranges[index].lower = SubRange::Copy(
                        std::to_string(next->first));
                subranges[index].lower_inclusive = true;
                remaining_rate -= keyrate;
                sum = 0;
                share_per_subrange =
                        remaining_rate / (subranges.size() - index);
                entry--;
                continue;
            }

            if (sum + entry->second > share_per_subrange) {
                // Close the current subrange.
                Slice userkey = std::to_string(entry->first);
                if (user_comparator_->Compare(userkey,
                                              subranges[index].lower) == 0 &&
                    subranges[index].lower_inclusive) {
                    // A single point.
                    subranges[index].upper = SubRange::Copy(userkey);
                    subranges[index].upper_inclusive = true;

                    remaining_rate -= entry->second;

                    entry++;
                    index++;
                    subranges[index].lower = SubRange::Copy(
                            std::to_string(entry->first));
                    subranges[index].lower_inclusive = true;

                    sum = entry->second;
                    share_per_subrange =
                            remaining_rate / (subranges.size() - index);
                    remaining_rate -= entry->second;
                    continue;
                } else {
                    subranges[index].upper = SubRange::Copy(userkey);
                    subranges[index].upper_inclusive = false;

                    index++;
                    subranges[index].lower = SubRange::Copy(userkey);
                    subranges[index].lower_inclusive = true;
                    if (index == subranges.size() - 1) {
                        break;
                    }
                    share_per_subrange =
                            remaining_rate / (subranges.size() - index);
                }
                sum = 0;
            }
            sum += entry->second;
            remaining_rate -= entry->second;
        }

        RDMA_ASSERT(index == subranges.size() - 1);
        latest->AssertSubrangeBoundary(user_comparator_);

        bool bad_reorg = false;
        for (int i = 0; i < subranges.size(); i++) {
            SubRange &sr = subranges[i];
            if (user_comparator_->Compare(sr.upper, sr.lower) < 0) {
                // This is a bad reorg due to too few samples. give up.
                bad_reorg = true;
                break;
            }
            sr.ninserts = 0; //total_inserts / subranges.size();
        }

        // Unref all immutable memtables.
        for (int i = 0; i < subrange_imms.size(); i++) {
            for (int j = 0; j < subrange_imms[i].size(); j++) {
                subrange_imms[i][j]->Unref(dbname_);
            }
        }

        if (bad_reorg) {
            RDMA_LOG(rdmaio::INFO)
                << fmt::format("bad reorg: {}", latest->DebugString());
            delete latest;
            return;
        }
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("major at {} puts with {} keys: {}",
                           processed_writes,
                           userkey_rate.size(), latest->DebugString());
        VersionEdit edit;
        for (int i = 0; i < subranges.size(); i++) {
            edit.UpdateSubRange(i, subranges[i].lower, subranges[i].upper,
                                subranges[i].lower_inclusive,
                                subranges[i].upper_inclusive,
                                subranges[i].num_duplicates);
        }
        latest_subranges_.store(latest);
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
    }

    void
    SubRangeManager::MoveShareDuplicateSubranges(SubRanges *latest, int index) {
        SubRange &sr = latest->subranges[index];
        int nDuplicates = 0;
        int start = -1;
        int end = -1;
        for (int i = 0; i < latest->subranges.size(); i++) {
            if (!latest->subranges[i].Equals(sr, user_comparator_)) {
                continue;
            }
            end = i;
            if (start == -1) {
                start = i;
            }
        }
        RDMA_ASSERT(end - start + 1 == sr.num_duplicates);
        nDuplicates = sr.num_duplicates - 1;
        double share = sr.ninserts / nDuplicates;
        for (int i = start; i <= end; i++) {
            SubRange &dup = latest->subranges[i];
            dup.ninserts += share;
            dup.num_duplicates -= 1;
            if (nDuplicates == 1) {
                dup.num_duplicates = 0;
            }
        }
    }

    bool
    SubRangeManager::MinorReorgDestroyDuplicates(SubRanges *latest,
                                               int subrange_id) {
        SubRange &sr = latest->subranges[subrange_id];
        if (sr.num_duplicates == 0) {
            return false;
        }

        double fair_ratio = 1.0 / options_.num_memtable_partitions;
        double percent = sr.insertion_ratio / fair_ratio;
        if (percent >= 0.5) {
            return false;
        }

        // destroy this duplicate.
        MoveShareDuplicateSubranges(latest, subrange_id);
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Destroy subrange {} {}", subrange_id,
                           sr.DebugString());
        latest->subranges.erase(latest->subranges.begin() + subrange_id);
        VersionEdit edit;
        std::vector<SubRange> &subranges = latest->subranges;
        for (int i = 0; i < subranges.size(); i++) {
            edit.UpdateSubRange(i, subranges[i].lower, subranges[i].upper,
                                subranges[i].lower_inclusive,
                                subranges[i].upper_inclusive,
                                subranges[i].num_duplicates);
        }
        latest_subranges_.store(latest);
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
        num_minor_reorgs_for_dup += 1;
        last_minor_reorg_seq_ = versions_->last_sequence_;
        return true;
    }

    bool SubRangeManager::PerformSubrangeMinorReorgDuplicate(int subrange_id,
                                                           leveldb::SubRanges *latest,
                                                           double total_inserts) {
        double fair_ratio = 1.0 / (double) options_.num_memtable_partitions;
        std::vector<SubRange> &subranges = latest->subranges;
        SubRange &sr = subranges[subrange_id];
        RDMA_ASSERT(sr.insertion_ratio > fair_ratio);

        if (!sr.IsAPoint(user_comparator_)) {
            return false;
        }

        int num_duplicates = (int) std::floor(sr.insertion_ratio / fair_ratio);
        if (num_duplicates == 0) {
            return false;
        }

        num_minor_reorgs_for_dup++;
        last_minor_reorg_seq_ = versions_->last_sequence_;

        uint64_t total_sr_inserts = sr.ninserts;
        uint64_t remaining_sum = sr.ninserts;
        int new_num_duplicates = sr.num_duplicates + num_duplicates + 1;
        Slice lower = sr.lower;
        for (int i = 0; i < num_duplicates; i++) {
            SubRange new_sr = {};
            new_sr.lower = SubRange::Copy(lower);
            new_sr.upper = SubRange::Copy(lower);
            new_sr.lower_inclusive = true;
            new_sr.upper_inclusive = true;
            new_sr.num_duplicates = new_num_duplicates;
            new_sr.ninserts = total_sr_inserts / (num_duplicates + 1);

            remaining_sum -= new_sr.ninserts;
            subranges.insert(subranges.begin() + subrange_id + 1, new_sr);
        }

        subranges[subrange_id].ninserts = remaining_sum;
        subranges[subrange_id].num_duplicates = new_num_duplicates;

        int start = -1;
        int end = -1;
        for (int i = 0; i < subranges.size(); i++) {
            if (!subranges[i].Equals(subranges[subrange_id],
                                     user_comparator_)) {
                continue;
            }
            end = i;
            subranges[i].num_duplicates = new_num_duplicates;
            if (start == -1) {
                start = i;
            }
        }
        RDMA_ASSERT(end - start + 1 == subranges[subrange_id].num_duplicates);

        while (subranges.size() > options_.num_memtable_partitions) {
            // remove min.
            double min_ratio = 9999999;
            int min_range_id = -1;
            for (int i = 0; i < subranges.size(); i++) {
                // Skip the new subranges.
                if (i >= start && i <= end) {
                    continue;
                }

                SubRange &min_sr = subranges[i];
                if (user_comparator_->Compare(min_sr.lower, lower) == 0) {
                    continue;
                }
                if (min_sr.insertion_ratio < min_ratio) {
                    min_ratio = min_sr.insertion_ratio;
                    min_range_id = i;
                }
            }

            // merge min with neighboring subrange.
            int left = min_range_id - 1;
            int right = min_range_id + 1;
            bool merge_left = true;
            if (left >= 0 && right < subranges.size()) {
                if (subranges[left].insertion_ratio <
                    subranges[right].insertion_ratio) {
                    merge_left = true;
                } else {
                    merge_left = false;
                }
            } else if (left >= 0) {
                merge_left = true;
            } else {
                merge_left = false;
            }
            SubRange &min_sr = subranges[min_range_id];
            if (merge_left) {
                SubRange &l = subranges[left];
                if (l.num_duplicates > 0) {
                    // move its shares to other duplicates.
                    MoveShareDuplicateSubranges(latest, left);
                    // destroy this subrange.
                    subranges.erase(subranges.begin() + left);
                } else {
                    l.ninserts += min_sr.ninserts;
                    l.upper = min_sr.upper;
                    l.upper_inclusive = min_sr.upper_inclusive;
                    subranges.erase(subranges.begin() + min_range_id);
                }
            } else {
                SubRange &r = subranges[right];
                if (r.num_duplicates > 0) {
                    // move its shares to other duplicates.
                    MoveShareDuplicateSubranges(latest, right);
                    // destroy this subrange.
                    subranges.erase(subranges.begin() + right);
                } else {
                    r.ninserts += min_sr.ninserts;
                    r.lower = min_sr.lower;
                    r.lower_inclusive = min_sr.lower_inclusive;
                    subranges.erase(subranges.begin() + min_range_id);
                }
            }
        }
        VersionEdit edit;
        for (int i = 0; i < subranges.size(); i++) {
            edit.UpdateSubRange(i, subranges[i].lower, subranges[i].upper,
                                subranges[i].lower_inclusive,
                                subranges[i].upper_inclusive,
                                subranges[i].num_duplicates);
        }
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Duplicate {} subrange-{} {}", num_duplicates,
                           subrange_id, latest->DebugString());

        latest_subranges_.store(latest);
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
        return true;
    }

    void SubRangeManager::PerformSubrangeMinorReorg(int subrange_id,
                                                  leveldb::SubRanges *latest,
                                                  double total_inserts) {
        last_minor_reorg_seq_ = versions_->last_sequence_;
        std::vector<SubRange> &subranges = latest->subranges;
        double fair_ratio = 1.0 / (double) options_.num_memtable_partitions;

        SubRange &sr = subranges[subrange_id];
        RDMA_ASSERT(sr.insertion_ratio > fair_ratio);

        if (PerformSubrangeMinorReorgDuplicate(subrange_id, latest,
                                               total_inserts)) {
            return;
        }

        // higher share.
        // Perform major reorg.
        std::vector<AtomicMemTable *> subrange_imms;
        uint32_t nslots = (options_.num_memtables -
                           options_.num_memtable_partitions) /
                          options_.num_memtable_partitions;
        uint32_t remainder = (options_.num_memtables -
                              options_.num_memtable_partitions) %
                             options_.num_memtable_partitions;
        uint32_t slot_id = 0;
        uint32_t slots = 0;
        for (int i = 0; i < subrange_id; i++) {
            slots = nslots;
            if (remainder > 0) {
                slots += 1;
                remainder--;
            }
            slot_id += slots;
        }

        slots = nslots;
        if (remainder > 0) {
            slots += 1;
        }

        (*partitioned_active_memtables_)[subrange_id]->mutex.Lock();
        MemTable *m = (*partitioned_active_memtables_)[subrange_id]->memtable;
        if (m) {
            RDMA_ASSERT(versions_->mid_table_mapping_[m->memtableid()].Ref());
            subrange_imms.push_back(
                    &versions_->mid_table_mapping_[m->memtableid()]);
        }

        for (int j = 0; j < slots; j++) {
            RDMA_ASSERT(slot_id + j < (*partitioned_imms_).size());
            uint32_t imm_id = (*partitioned_imms_)[slot_id + j];
            if (imm_id == 0) {
                continue;
            }
            auto *imm = versions_->mid_table_mapping_[imm_id].Ref();
            if (imm) {
                subrange_imms.push_back(&versions_->mid_table_mapping_[imm_id]);
            }
        }
        (*partitioned_active_memtables_)[subrange_id]->mutex.Unlock();

        // We have all memtables now.
        std::map<uint64_t, uint32_t> userkey_freq;
        uint32_t total_accesses = 0;
        for (int i = 0; i < subrange_imms.size(); i++) {
            AtomicMemTable *mem = subrange_imms[i];
            uint32_t samples = 0;
            leveldb::Iterator *it = mem->memtable_->NewIterator(
                    TraceType::MEMTABLE, AccessCaller::kUncategorized,
                    0);
            it->SeekToFirst();
            ParsedInternalKey ik;
            while (it->Valid()) {
                RDMA_ASSERT(ParseInternalKey(it->key(), &ik));
                if (sr.IsSmallerThanLower(ik.user_key, user_comparator_)) {
                    it->Next();
                    continue;
                }
                if (sr.IsGreaterThanUpper(ik.user_key, user_comparator_)) {
                    it->Next();
                    continue;
                }

                uint64_t k = 0;
                nova::str_to_int(ik.user_key.data(), &k,
                                 ik.user_key.size());
                userkey_freq[k] += 1;
                samples += 1;
                total_accesses += 1;
                it->Next();
            }
//            RDMA_LOG(rdmaio::INFO)
//                << fmt::format("minor sample {} from mid-{} subrange-{}",
//                               samples, mem->memtable_->memtableid(),
//                               subrange_id);
            delete it;
        }

        if (userkey_freq.size() <= 1 || total_accesses <= 100) {
            num_skipped_minor_reorgs++;
            // Unref all immutable memtables.
            for (int j = 0; j < subrange_imms.size(); j++) {
                subrange_imms[j]->Unref(dbname_);
            }
            delete latest;
            return;
        }

        RDMA_LOG(rdmaio::INFO)
            << fmt::format(
                    "minor at {} puts with {} keys for subrange-{}: before {}",
                    total_inserts,
                    userkey_freq.size(), subrange_id,
                    sr.DebugString());

        double inserts = (sr.insertion_ratio - fair_ratio) * total_inserts;
        RDMA_ASSERT(inserts < sr.ninserts)
            << fmt::format("{} inserts:{} fair:{}", sr.DebugString(), inserts,
                           fair_ratio);
        double remove_share = (double) (
                ((sr.insertion_ratio - fair_ratio) / sr.insertion_ratio)
                * total_accesses);
        double left_share = remove_share;
        double right_share = remove_share;
        double left_inserts = inserts;
        double right_inserts = inserts;

        double total_rate_to_distribute = sr.insertion_ratio - fair_ratio;
        double remaining_rate = total_rate_to_distribute;
        double left_rate = 0;
        double right_rate = 0;

        if (subrange_id != 0 && subrange_id != subranges.size() - 1) {
            SubRange &left = subranges[subrange_id - 1];
            SubRange &right = subranges[subrange_id + 1];
            // they are not duplicates.
            if (left.num_duplicates == 0 && right.num_duplicates == 0) {
                if (left.insertion_ratio > fair_ratio
                    && right.insertion_ratio < fair_ratio) {
                    double need_rate = fair_ratio - right.insertion_ratio;
                    if (total_rate_to_distribute < need_rate) {
                        right_rate = total_rate_to_distribute;
                        remaining_rate = 0;
                    } else {
                        right_rate = need_rate;
                        remaining_rate = total_rate_to_distribute - need_rate;
                    }
                } else if (left.insertion_ratio < fair_ratio
                           && right.insertion_ratio > fair_ratio) {
                    double need_rate = fair_ratio - left.insertion_ratio;
                    if (total_rate_to_distribute < need_rate) {
                        left_rate = total_rate_to_distribute;
                        remaining_rate = 0;
                    } else {
                        left_rate = need_rate;
                        remaining_rate = total_rate_to_distribute - need_rate;
                    }
                }

                if (remaining_rate > 0) {
                    double new_right_rate = right.insertion_ratio + right_rate;
                    double new_left_rate = left.insertion_ratio + left_rate;

                    double total = new_left_rate + new_right_rate;
                    double leftp = new_right_rate / total;
                    double rightp = new_left_rate / total;

                    left_rate += leftp * remaining_rate;
                    right_rate += rightp * remaining_rate;
                }

                RDMA_ASSERT(std::abs(
                        total_rate_to_distribute - left_rate - right_rate) <=
                            0.001);
                double leftp = left_rate / total_rate_to_distribute;
                double rightp = right_rate / total_rate_to_distribute;
                left_share = remove_share * leftp;
                left_inserts = inserts * leftp;
                right_share = remove_share * rightp;
                right_inserts = inserts * rightp;

                if (left_share < userkey_freq.begin()->second
                    && right_share < userkey_freq.rbegin()->second) {
                    // cannot move either to left or right.
                    // move to right.
                    right_share = userkey_freq.rbegin()->second;
                    right_inserts += left_inserts;
                    left_share = 0;
                    left_inserts = 0;
                }
            }
        }

        bool success = false;
        if (subrange_id > 0 && left_share > 0 &&
            subranges[subrange_id].num_duplicates == 0) {
            uint64_t sr_lower;
            uint64_t new_lower;
            nova::str_to_int(sr.lower.data(), &new_lower, sr.lower.size());
            sr_lower = new_lower;
            double removes = left_share;
            auto it = userkey_freq.begin();
            while (it != userkey_freq.end()) {
                if (removes - it->second < 0) {
                    break;
                }
                removes -= it->second;
                new_lower = it->first;
                it = userkey_freq.erase(it);
            }
            if (new_lower > sr_lower) {
                sr.lower = SubRange::Copy(std::to_string(new_lower));
                sr.lower_inclusive = true;
                sr.ninserts -= left_inserts;

                SubRange &other = subranges[subrange_id - 1];
                other.ninserts += left_inserts;
                other.upper = SubRange::Copy(sr.lower);
                other.upper_inclusive = false;
                success = true;
            }
        }
        if (subrange_id < subranges.size() - 1 && right_share > 0 &&
            subranges[subrange_id + 1].num_duplicates == 0) {
            uint64_t sr_upper;
            uint64_t new_upper;
            uint64_t sr_lower;
            nova::str_to_int(sr.upper.data(), &new_upper, sr.upper.size());
            nova::str_to_int(sr.lower.data(), &sr_lower, sr.lower.size());
            sr_upper = new_upper;
            if (!sr.lower_inclusive) {
                sr_lower += 1;
            }
            double removes = right_share;
            for (auto it = userkey_freq.rbegin();
                 it != userkey_freq.rend(); it++) {
                if (removes - it->second < 0) {
                    break;
                }
                removes -= it->second;
                new_upper = it->first;
            }
            if (new_upper < sr_upper && new_upper - sr_lower >= 1) {
                sr.upper = SubRange::Copy(std::to_string(new_upper));
                sr.upper_inclusive = false;
                sr.ninserts -= right_inserts;

                SubRange &other = subranges[subrange_id + 1];
                other.ninserts += right_inserts;
                other.lower = SubRange::Copy(sr.upper);
                other.lower_inclusive = true;
                success = true;
            }
        }

        // Unref all immutable memtables.
        for (int j = 0; j < subrange_imms.size(); j++) {
            subrange_imms[j]->Unref(dbname_);
        }

        if (success) {
            RDMA_LOG(rdmaio::INFO)
                << fmt::format(
                        "minor at {} puts with {} keys for subrange-{}: after {}",
                        total_inserts,
                        userkey_freq.size(), subrange_id,
                        sr.DebugString());
            num_minor_reorgs += 1;
            VersionEdit edit;
            for (int i = 0; i < subranges.size(); i++) {
                edit.UpdateSubRange(i, subranges[i].lower, subranges[i].upper,
                                    subranges[i].lower_inclusive,
                                    subranges[i].upper_inclusive,
                                    subranges[i].num_duplicates);
            }
            latest_subranges_.store(latest);
            versions_->AppendChangesToManifest(&edit, manifest_file_,
                                               options_.manifest_stoc_id);
            return;
        }

        delete latest;
        num_skipped_minor_reorgs++;
    }

    void SubRangeManager::PerformSubRangeReorganization(double processed_writes) {
        RDMA_ASSERT(versions_->last_sequence_ > SUBRANGE_WARMUP_NPUTS);
        SubRanges *ref = latest_subranges_;
        double fair_ratio = 1.0 / (double) options_.num_memtable_partitions;

        // Make a copy.
        range_lock_.Lock();
        SubRanges *latest = new SubRanges(*ref);
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
                if (MinorReorgDestroyDuplicates(latest, i)) {
                    return;
                }
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
            PerformSubrangeMinorReorg(most_unfair_subrange, latest,
                                      total_inserts);
            return;
        }
        delete latest;
    }


}