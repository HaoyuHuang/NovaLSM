
//
// Created by Haoyu Huang on 6/15/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "range_index.h"
#include "table/merger.h"

namespace leveldb {
    namespace {
        static void UnrefEntry(void *arg1, void *arg2) {
            Iterator *it = reinterpret_cast<Iterator *>(arg1);
            NOVA_ASSERT(it);
            delete it;
        }
    }

    RangeIndex::RangeIndex(uint32_t version_id, uint32_t lsm_version_id)
            : version_id_(version_id), lsm_version_id(lsm_version_id) {

    }

    RangeIndex::RangeIndex(RangeIndex *current, uint32_t version_id,
                           uint32_t lsm_version_id) : ranges(
            current->ranges), range_tables(current->range_tables),
                                                      version_id_(version_id),
                                                      lsm_version_id(
                                                              lsm_version_id ==
                                                              0
                                                              ? current->lsm_version_id
                                                              : lsm_version_id) {
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
                                        version_id_, lsm_version_id,
                                        ranges.size());
        NOVA_ASSERT(ranges.size() == range_tables.size())
            << fmt::format("{} {}", ranges.size(), range_tables.size());
        for (int i = 0; i < ranges.size(); i++) {
            const auto &range = ranges[i];
            const auto &tables = range_tables[i];
            debug += fmt::format("[{}]: {} {}\n", i, range.DebugString(),
                                 tables.DebugString());
        }
        return debug;
    }

    Iterator *
    GetRangeIndexFragmentIterator(void *arg, BlockReadContext context,
                                  const ReadOptions &options,
                                  const Slice &file_value,
                                  std::string *next_key) {
        Version *v = reinterpret_cast<Version *>(arg);
        NOVA_ASSERT(v);
        Slice value = file_value;
        NOVA_ASSERT(DecodeStr(&value, next_key));
        uint32_t memtableid = 0;
        uint64_t sstableid = 0;
        bool decode_memtables = true;
        std::vector<Iterator *> list;
        RangeTables tables;

        while (true) {
            if (decode_memtables) {
                NOVA_ASSERT(DecodeFixed32(&value, &memtableid));
                if (memtableid == 0) {
                    decode_memtables = false;
                    continue;
                }
                tables.memtable_ids.insert(memtableid);
                auto memtable = v->vset_->mid_table_mapping_[memtableid];
                NOVA_ASSERT(memtable);
                // this is a memtable id.
                list.push_back(
                        memtable->memtable_->NewIterator(TraceType::MEMTABLE,
                                                         AccessCaller::kUserIterator));
            } else {
                if (!DecodeFixed64(&value, &sstableid)) {
                    break;
                }
                tables.l0_sstable_ids.insert(sstableid);
                // this is a new sstable id.
                auto meta = v->fn_files_[sstableid];
                NOVA_ASSERT(meta);
                list.push_back(v->table_cache_->NewIterator(
                        AccessCaller::kUserIterator,
                        options, meta, sstableid, 0,
                        meta->converted_file_size));
            }
        }
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("RangeIndexScan Decode: {}", tables.DebugString());
        auto it = NewMergingIterator(v->icmp_, &list[0], list.size());
        it->RegisterCleanup(&UnrefEntry, it, nullptr);
        return it;
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
        first_ = new RangeIndex;
        last_ = new RangeIndex;
        first_->next_ = last_;
        first_->prev_ = nullptr;
        last_->prev_ = first_;
        last_->next_ = nullptr;
        current_ = nullptr;
    }

    void RangeIndexManager::Initialize(RangeIndex *init) {
        current_ = init;
        current_->refs_ += 1;
        first_->next_ = current_;
        current_->prev_ = first_;
        current_->next_ = last_;
        last_->prev_ = current_;

        NOVA_ASSERT(versions_->versions_[current_->lsm_version_id]->Ref())
            << current_->lsm_version_id;
        for (int i = 0; i < current_->range_tables.size(); i++) {
            const auto &table = current_->range_tables[i];
            for (auto memtableid : table.memtable_ids) {
                auto memtable = versions_->mid_table_mapping_[memtableid];
                NOVA_ASSERT(memtable);
                memtable->RefMemTable();
            }
        }
    }

    RangeIndex *RangeIndexManager::current() {
        mutex_.lock();
        auto current = current_;
        NOVA_ASSERT(current);
        current->Ref();
        mutex_.unlock();
        return current;
    }

    void RangeIndexManager::DeleteObsoleteVersions() {
        std::unordered_map<uint32_t, uint32_t> memtable_refs;
        std::vector<uint64_t> old_versions;
        mutex_.lock();
        auto v = first_->next_;
        while (v != last_) {
            auto next = v->next_;
            if (v->is_deleted_) {
                v->prev_->next_ = v->next_;
                v->next_->prev_ = v->prev_;
                for (int i = 0; i < v->range_tables.size(); i++) {
                    auto &table = v->range_tables[i];
                    for (auto memtableid : table.memtable_ids) {
                        memtable_refs[memtableid] += 1;
                    }
                }
                NOVA_LOG(rdmaio::DEBUG)
                    << fmt::format("Delete {}", v->version_id_);
                old_versions.push_back(v->lsm_version_id);
                delete v;
                v = nullptr;
            }
            v = next;
        }
        mutex_.unlock();
        for (const auto &it : memtable_refs) {
            versions_->mid_table_mapping_[it.first]->Unref("", it.second);
        }
        for (auto vid : old_versions) {
            versions_->versions_[vid]->Unref("");
        }
    }

    void
    RangeIndexManager::AppendNewVersion(const RangeIndexVersionEdit &edit) {
        mutex_.lock();
        range_index_seq_id_ += 1;
        auto new_range_idx = new RangeIndex(current_, range_index_seq_id_,
                                            edit.lsm_version_id);
        NOVA_ASSERT(versions_->versions_[new_range_idx->lsm_version_id]->Ref())
            << new_range_idx->lsm_version_id;
        if (edit.add_new_memtable) {
            NOVA_ASSERT(edit.sr);
            int start_id = 0;
            BinarySearch(new_range_idx->ranges, edit.sr->tiny_ranges[0].lower,
                         &start_id, user_comparator_);
            NOVA_ASSERT(start_id != -1);
            const std::string &upper = edit.sr->tiny_ranges[
                    edit.sr->tiny_ranges.size() - 1].upper;
            while (true) {
                new_range_idx->range_tables[start_id].memtable_ids.insert(
                        edit.new_memtable_id);
                start_id++;
                if (start_id == new_range_idx->ranges.size()) {
                    break;
                }
                if (new_range_idx->ranges[start_id].lower == upper ||
                    new_range_idx->ranges[start_id].IsSmallerThanLower(
                            upper, user_comparator_)) {
                    break;
                }
            }
        }
        for (int i = 0; i < new_range_idx->range_tables.size(); i++) {
            auto &table = new_range_idx->range_tables[i];
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
                if (table.memtable_ids.erase(replace_sstable.first) == 1) {
                    table.l0_sstable_ids.insert(replace_sstable.second.begin(),
                                                replace_sstable.second.end());
                }
            }
        }
        new_range_idx->refs_ = 1;
        new_range_idx->next_ = current_->next_;
        new_range_idx->prev_ = current_;
        current_->next_->prev_ = new_range_idx;
        current_->next_ = new_range_idx;
        current_->UnRef();
        current_ = new_range_idx;
        for (int i = 0; i < new_range_idx->range_tables.size(); i++) {
            const auto &table = new_range_idx->range_tables[i];
            for (auto memtableid : table.memtable_ids) {
                auto memtable = versions_->mid_table_mapping_[memtableid];
                NOVA_ASSERT(memtable);
                memtable->RefMemTable();
            }
        }
        NOVA_LOG(rdmaio::DEBUG) << current_->DebugString();
        mutex_.unlock();
    }
}