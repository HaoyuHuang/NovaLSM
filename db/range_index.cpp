
//
// Created by Haoyu Huang on 6/15/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "range_index.h"

namespace leveldb {
    RangeIndex::RangeIndex(RangeIndex *current, uint32_t version_id) : ranges(
            current->ranges), range_tables(current->range_tables),
                                                                       lsm_version_id(
                                                                               version_id ==
                                                                               0
                                                                               ? current->lsm_version_id
                                                                               : version_id) {
    }

    void RangeIndex::Ref() {
        mutex_.lock();
        NOVA_ASSERT(refs_ >= 0);
        refs_ += 1;
        mutex_.unlock();
    }

    void RangeIndex::UnRef() {
        mutex_.lock();
        NOVA_ASSERT(refs_ >= 1);
        refs_ -= 1;
        if (refs_ == 0) {
            is_deleted_ = true;
        }
        mutex_.unlock();
    }

    RangeIndexManager::RangeIndexManager(VersionSet *versions,
                                         const Comparator *user_comparator)
            : versions_(versions),
              user_comparator_(user_comparator) {
        dummy_ = new RangeIndex;
        dummy_->next_ = current_;
        dummy_->prev_ = dummy_;
        current_->prev_ = dummy_;
        current_->next_ = dummy_;
    }

    RangeIndex *RangeIndexManager::current() {
        mutex_.lock();
        auto current = current_;
        mutex_.unlock();
        return current;
    }

    void RangeIndexManager::DeleteObsoleteVersions() {
        std::unordered_map<uint32_t, uint32_t> map;
        mutex_.lock();
        auto v = dummy_->next_;
        while (v != dummy_) {
            auto next = v->next_;
            if (v->is_deleted_) {
                v->prev_->next_ = v->next_;
                v->next_->prev_ = v->prev_;
                for (int i = 0; i < v->range_tables.size(); i++) {
                    auto &table = v->range_tables[i];
                    for (auto memtableid : table.memtable_ids) {
                        map[memtableid] += 1;
                    }
                }
                delete v;
                v = nullptr;
            }
            v = next;
        }
        mutex_.unlock();
    }

    void RangeIndexManager::AppendNewVersion(const SubRange &sr,
                                             uint32_t new_memtable_id,
                                             bool add_new_memtable,
                                             const std::vector<uint64_t> &removed_sstables,
                                             const std::vector<uint32_t> &removed_memtables,
                                             uint32_t lsm_version_id,
                                             const Comparator *user_comparator) {
        mutex_.lock();
        auto new_range_idx = new RangeIndex(current_, lsm_version_id);
        if (add_new_memtable) {
            int start_id = 0;
            BinarySearch(new_range_idx->ranges, sr.tiny_ranges[0].lower,
                         &start_id, user_comparator);
            while (true) {
                new_range_idx->range_tables[start_id].memtable_ids.insert(
                        new_memtable_id);
                start_id++;
                if (start_id == new_range_idx->ranges.size()) {
                    break;
                }
                if (new_range_idx->ranges[start_id].IsGreaterThanLower(
                        sr.tiny_ranges[sr.tiny_ranges.size() - 1].upper,
                        user_comparator)) {
                    break;
                }
            }
        }
        for (int i = 0; i < new_range_idx->range_tables.size(); i++) {
            auto &table = new_range_idx->range_tables[i];
            for (auto sstable : removed_sstables) {
                table.l0_sstable_ids.erase(sstable);
            }
            for (auto memtable : removed_memtables) {
                table.memtable_ids.erase(memtable);
            }
        }
        new_range_idx->refs_ = 1;
        new_range_idx->next_ = current_->next_;
        new_range_idx->prev_ = current_;
        current_->next_->prev_ = new_range_idx;
        current_->next_ = new_range_idx;
        current_ = new_range_idx;

        for (int i = 0; i < new_range_idx->range_tables.size(); i++) {
            auto &table = new_range_idx->range_tables[i];
            for (auto memtableid : table.memtable_ids) {
                versions_->mid_table_mapping_[memtableid]->RefMemTable();
            }
        }
        mutex_.unlock();
    }

}