
//
// Created by Haoyu Huang on 6/15/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "range_index.h"
#include "table/merger.h"

namespace leveldb {
    uint32_t RangeTables::Encode(char *buf) {
        uint32_t msg_size = 0;
        msg_size += EncodeFixed32(buf + msg_size, memtable_ids.size());
        msg_size += EncodeFixed32(buf + msg_size, l0_sstable_ids.size());
        for (auto mid : memtable_ids) {
            msg_size += EncodeFixed32(buf + msg_size, mid);
        }
        for (auto l0 : l0_sstable_ids) {
            msg_size += EncodeFixed64(buf + msg_size, l0);
        }
        return msg_size;
    }

    void RangeTables::Decode(Slice *buf) {
        uint32_t num_mids = 0;
        uint32_t num_l0s = 0;
        NOVA_ASSERT(DecodeFixed32(buf, &num_mids));
        NOVA_ASSERT(DecodeFixed32(buf, &num_l0s));

        for (int i = 0; i < num_mids; i++) {
            uint32_t mid = 0;
            NOVA_ASSERT(DecodeFixed32(buf, &mid));
            memtable_ids.insert(mid);
        }

        for (int i = 0; i < num_l0s; i++) {
            uint64_t l0 = 0;
            NOVA_ASSERT(DecodeFixed64(buf, &l0));
            l0_sstable_ids.insert(l0);
        }
    }


    RangeIndex::RangeIndex(ScanStats *scan_stats) : scan_stats_(scan_stats) {

    }

    RangeIndex::RangeIndex(ScanStats *scan_stats, uint32_t version_id,
                           uint32_t lsm_vid)
            : version_id_(version_id), lsm_version_id_(lsm_vid),
              scan_stats_(scan_stats) {
        is_deleted_ = false;
    }

    uint32_t RangeIndex::Encode(char *buf) {
        uint32_t msg_size = 0;
        msg_size += EncodeFixed32(buf + msg_size, lsm_version_id_);
        msg_size += EncodeFixed32(buf + msg_size, ranges_.size());
        for (int i = 0; i < ranges_.size(); i++) {
            msg_size += ranges_[i].Encode(buf + msg_size);
        }
        for (int i = 0; i < range_tables_.size(); i++) {
            msg_size += range_tables_[i].Encode(buf + msg_size);
        }
        return msg_size;
    }

    void RangeIndex::Decode(Slice *buf) {
        uint32_t num_ranges = 0;
        NOVA_ASSERT(DecodeFixed32(buf, &lsm_version_id_));
        NOVA_ASSERT(DecodeFixed32(buf, &num_ranges));
        for (int i = 0; i < num_ranges; i++) {
            Range r = {};
            r.Decode(buf);
            ranges_.push_back(r);
        }
        for (int i = 0; i < num_ranges; i++) {
            RangeTables r = {};
            r.Decode(buf);
            range_tables_.push_back(r);
        }
    }

    RangeIndex::RangeIndex(ScanStats *scan_stats, RangeIndex *current,
                           uint32_t version_id,
                           uint32_t lsm_vid) : ranges_(
            current->ranges_), range_tables_(current->range_tables_),
                                               version_id_(version_id),
                                               scan_stats_(scan_stats) {
        if (lsm_vid == 0) {
            lsm_version_id_ = current->lsm_version_id_;
        } else {
            lsm_version_id_ = lsm_vid;
        }
        is_deleted_ = false;
    }

    std::string RangeTables::DebugString() const {
        std::string debug = fmt::format("tables:{}:{} memtables:",
                                        memtable_ids.size(),
                                        l0_sstable_ids.size());
        for (auto memtableid : memtable_ids) {
            debug += std::to_string(memtableid);
            debug += ",";
        }
        debug += " sstable:";
        for (auto sstableid : l0_sstable_ids) {
            debug += std::to_string(sstableid);
            debug += ",";
        }
        return debug;
    }

    std::string RangeIndex::DebugString() const {
        std::string debug = fmt::format("v:{} lsm-version-id:{} ranges:{}\n",
                                        version_id_, lsm_version_id_,
                                        ranges_.size());
        NOVA_ASSERT(ranges_.size() == range_tables_.size())
            << fmt::format("{} {}", ranges_.size(), range_tables_.size());
        for (int i = 0; i < ranges_.size(); i++) {
            const auto &range = ranges_[i];
            const auto &tables = range_tables_[i];
            debug += fmt::format("[{}]: {} {}\n", i, range.DebugString(),
                                 tables.DebugString());
        }
        return debug;
    }

    void RangeIndexIterator::SkipToNextUserKey(const Slice &target) {
        Slice userkey = ExtractUserKey(target);
        uint64_t ukey = 0;
        nova::str_to_int(userkey.data(), &ukey, userkey.size());
        ukey += 1;
        if (!Valid()) {
            Seek(target);
        }
        if (Valid()) {
            auto range = range_index_->ranges_[index_];
            if (range.upper_int() == ukey) {
                index_++;
            }
        }
    }

    Iterator *
    GetRangeIndexFragmentIterator(void *arg, void *arg2,
                                  BlockReadContext context,
                                  const ReadOptions &options,
                                  const Slice &file_value,
                                  std::string *flag) {
        Version *v = reinterpret_cast<Version *>(arg);
        RangeIndex *range_index = reinterpret_cast<RangeIndex *>(arg2);
        NOVA_ASSERT(v);
        NOVA_ASSERT(range_index);
        std::vector<Iterator *> list;
        uint32_t index;
        index = DecodeFixed32(file_value.data());
        NOVA_ASSERT(index >= 0 && index < range_index->range_tables_.size());
        const auto &range = range_index->ranges_[index];
        const auto &range_table = range_index->range_tables_[index];
        for (uint32_t memtableid : range_table.memtable_ids) {
            auto memtable = v->vset_->mid_table_mapping_[memtableid]->memtable_;
            if (!memtable) {
                continue;
            }
//            NOVA_ASSERT(memtable) << memtableid;
            list.push_back(memtable->NewIterator(TraceType::MEMTABLE, AccessCaller::kUserIterator));
        }
        for (uint64_t sstableid : range_table.l0_sstable_ids) {
            auto meta = v->fn_files_[sstableid];
            NOVA_ASSERT(meta)
                << fmt::format("v:{}, table:{} v:{} index:{}", v->version_id_,
                               sstableid, v->DebugString(),
                               range_index->DebugString());
            list.push_back(v->table_cache_->NewIterator(
                    AccessCaller::kUserIterator,
                    options, meta, sstableid, meta->SelectReplica(), 0,
                    meta->converted_file_size));
        }
        if (range_index->scan_stats_) {
            range_index->scan_stats_->number_of_scan_memtables_ += range_index->range_tables_[index].memtable_ids.size();
            range_index->scan_stats_->number_of_scan_l0_sstables_ += range_index->range_tables_[index].l0_sstable_ids.size();
        }
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("RangeIndexScan Decode: {}",
                           index);
        if (list.empty()) {
            return NewEmptyIterator();
        }

        auto it = NewMergingIterator(v->icmp_, &list[0], list.size());
        if (flag) {
            it->Seek(InternalKey(range.lower, 0,
                                 ValueType::kTypeValue).Encode());
        }
        return it;
    }

    bool RangeIndex::Ref() {
        mutex_.lock();
        NOVA_ASSERT(refs_ >= 0);
        if (is_deleted_) {
            mutex_.unlock();
            return false;
        }
        NOVA_ASSERT(!is_deleted_);
        refs_ += 1;
        mutex_.unlock();
        return true;
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

    RangeIndexManager::RangeIndexManager(ScanStats *scan_stats,
                                         VersionSet *versions,
                                         const Comparator *user_comparator)
            : versions_(versions),
              user_comparator_(user_comparator) {
//        for (int i = 0; i < MAX_LIVE_VERSIONS; i++) {
//            range_index_version_[i] = nullptr;
//        }
        range_index_version_seq_id_ = 0;
        first_ = new RangeIndex(scan_stats);
        last_ = new RangeIndex(scan_stats);
        first_->next_ = last_;
        first_->prev_ = nullptr;
        last_->prev_ = first_;
        last_->next_ = nullptr;
        current_ = nullptr;
    }

    void RangeIndexManager::Initialize(RangeIndex *init) {
        init->refs_ += 1;
        first_->next_ = init;
        init->prev_ = first_;
        init->next_ = last_;
        last_->prev_ = init;

        for (int i = 0; i < init->range_tables_.size(); i++) {
            const auto &table = init->range_tables_[i];
            for (auto memtableid : table.memtable_ids) {
                auto memtable = versions_->mid_table_mapping_[memtableid];
                NOVA_ASSERT(memtable);
                memtable->RefMemTable();
            }
        }
        current_ = init;
    }

    RangeIndex *RangeIndexManager::current() {
        RangeIndex *current = nullptr;
        while (true) {
            mutex_.lock();
            current = current_;
            NOVA_ASSERT(current);
            if (current->Ref()) {
                if (versions_->versions_[current->lsm_version_id_]->Ref()) {
                    mutex_.unlock();
                    break;
                }
                current->UnRef();
            }
            mutex_.unlock();
        }
        return current;
    }

    void RangeIndexManager::DeleteObsoleteVersions() {
        std::unordered_map<uint32_t, uint32_t> memtable_refs;
        mutex_.lock();
        auto v = first_->next_;
        while (v != last_) {
            auto next = v->next_;
            if (v->is_deleted_) {
                v->prev_->next_ = v->next_;
                v->next_->prev_ = v->prev_;
                for (int i = 0; i < v->range_tables_.size(); i++) {
                    auto &table = v->range_tables_[i];
                    for (auto memtableid : table.memtable_ids) {
                        memtable_refs[memtableid] += 1;
                    }
                }
                NOVA_LOG(rdmaio::DEBUG)
                    << fmt::format("Delete {}", v->version_id_);
                delete v;
                v = nullptr;
            }
            v = next;
        }
        mutex_.unlock();
        for (const auto &it : memtable_refs) {
            versions_->mid_table_mapping_[it.first]->Unref("", it.second);
        }
    }

    void RangeIndexManager::AppendNewVersion(RangeIndex *new_range_idx) {
        new_range_idx->refs_ = 1;
        new_range_idx->next_ = current_->next_;
        new_range_idx->prev_ = current_;
        current_->next_->prev_ = new_range_idx;
        current_->next_ = new_range_idx;
        current_->UnRef();
        current_ = new_range_idx;
        // Ref memtables here so that a scan sees a consistent view. Also, a scan does not need to reference of the memtables anymore.
        // Otherwise, a scan may fail to ref some memtables which may produce stale data.
        for (int i = 0; i < new_range_idx->range_tables_.size(); i++) {
            const auto &table = new_range_idx->range_tables_[i];
            for (auto memtableid : table.memtable_ids) {
                auto memtable = versions_->mid_table_mapping_[memtableid];
                NOVA_ASSERT(memtable);
                memtable->RefMemTable();
            }
        }
    }

    void
    RangeIndexManager::AppendNewVersion(ScanStats *scan_stats,
                                        const RangeIndexVersionEdit &edit) {
        mutex_.lock();
        range_index_version_seq_id_ += 1;
        auto new_range_idx = new RangeIndex(scan_stats, current_,
                                            range_index_version_seq_id_,
                                            edit.lsm_version_id);
        if (edit.add_new_memtable) {
            if (edit.sr) {
                int start_id = 0;
                BinarySearch(new_range_idx->ranges_,
                             edit.sr->tiny_ranges[0].lower,
                             &start_id, user_comparator_);
                NOVA_ASSERT(start_id != -1);
                const std::string &upper = edit.sr->tiny_ranges[
                        edit.sr->tiny_ranges.size() - 1].upper;
                while (true) {
                    new_range_idx->range_tables_[start_id].memtable_ids.insert(
                            edit.new_memtable_id);
                    start_id++;
                    if (start_id == new_range_idx->ranges_.size()) {
                        break;
                    }
                    if (new_range_idx->ranges_[start_id].lower == upper ||
                        new_range_idx->ranges_[start_id].IsSmallerThanLower(
                                upper, user_comparator_)) {
                        break;
                    }
                }
            } else {
                for (int i = 0; i < new_range_idx->range_tables_.size(); i++) {
                    auto &table = new_range_idx->range_tables_[i];
                    table.memtable_ids.insert(edit.new_memtable_id);
                }
            }
        }
        if (edit.add_new_memtable && edit.removed_memtables.empty()) {

        } else {
            for (int i = 0; i < new_range_idx->range_tables_.size(); i++) {
                auto &table = new_range_idx->range_tables_[i];
                for (auto sstable : edit.removed_l0_sstables) {
                    table.l0_sstable_ids.erase(sstable);
                }
                for (auto memtable : edit.removed_memtables) {
                    table.memtable_ids.erase(memtable);
                }
                for (auto &replace_memtable : edit.replace_memtables) {
                    if (table.memtable_ids.erase(replace_memtable.first) == 1) {
                        table.l0_sstable_ids.insert(replace_memtable.second);
                    }
                }
                for (auto &replace_sstable : edit.replace_l0_sstables) {
                    if (table.l0_sstable_ids.erase(replace_sstable.first) ==
                        1) {
                        table.l0_sstable_ids.insert(
                                replace_sstable.second.begin(),
                                replace_sstable.second.end());
                    }
                }
            }
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("New version {} {}", new_range_idx->version_id_,
                               new_range_idx->DebugString());
        }
        AppendNewVersion(new_range_idx);
        mutex_.unlock();
    }
}