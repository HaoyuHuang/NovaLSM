// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <stdio.h>

#include <algorithm>
#include <fmt/core.h>
#include <getopt.h>
#include <common/nova_common.h>
#include "ltc/stoc_file_client_impl.h"
#include "common/nova_console_logging.h"

#include "db/compaction.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "leveldb/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {
    void Version::Destroy() {
        assert(refs_ == 0);

        // Remove from linked list
        prev_->next_ = next_;
        next_->prev_ = prev_;
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("Delete version {}", version_id_);

        // TODO: Drop references to files
//        for (int level = 0; level < config::kNumLevels; level++) {
//            for (size_t i = 0; i < files_[level].size(); i++) {
//                FileMetaData *f = files_[level][i];
//                assert(f->refs > 0);
//                f->refs--;
//                if (f->refs <= 0) {
////                    delete f;
//                }
//            }
//        }
    }

    Version::~Version() {

    }

    int FindFile(const InternalKeyComparator &icmp,
                 const std::vector<FileMetaData *> &files, const Slice &key) {
        uint32_t left = 0;
        uint32_t right = files.size();
        while (left < right) {
            uint32_t mid = (left + right) / 2;
            const FileMetaData *f = files[mid];
            if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) <
                0) {
                // Key at "mid.largest" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            } else {
                // Key at "mid.largest" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    static bool AfterFile(const Comparator *ucmp, const Slice *user_key,
                          const FileMetaData *f) {
        // null user_key occurs before all keys and is therefore never after *f
        return (user_key != nullptr &&
                ucmp->Compare(*user_key, f->largest.user_key()) > 0);
    }

    static bool BeforeFile(const Comparator *ucmp, const Slice *user_key,
                           const FileMetaData *f) {
        // null user_key occurs after all keys and is therefore never before *f
        return (user_key != nullptr &&
                ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
    }

    bool SomeFileOverlapsRange(const InternalKeyComparator &icmp,
                               bool disjoint_sorted_files,
                               const std::vector<FileMetaData *> &files,
                               const Slice *smallest_user_key,
                               const Slice *largest_user_key) {
        const Comparator *ucmp = icmp.user_comparator();
        if (!disjoint_sorted_files) {
            // Need to check against all files
            for (size_t i = 0; i < files.size(); i++) {
                const FileMetaData *f = files[i];
                if (AfterFile(ucmp, smallest_user_key, f) ||
                    BeforeFile(ucmp, largest_user_key, f)) {
                    // No overlap
                } else {
                    return true;  // Overlap
                }
            }
            return false;
        }

        // Binary search over file list
        uint32_t index = 0;
        if (smallest_user_key != nullptr) {
            // Find the earliest possible internal key for smallest_user_key
            InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                                  kValueTypeForSeek);
            index = FindFile(icmp, files, small_key.Encode());
        }

        if (index >= files.size()) {
            // beginning of range is after all files, so no overlap.
            return false;
        }

        return !BeforeFile(ucmp, largest_user_key, files[index]);
    }

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
    class Version::LevelFileNumIterator : public Iterator {
    public:
        LevelFileNumIterator(const InternalKeyComparator &icmp,
                             const std::vector<FileMetaData *> *flist,
                             ScanStats *scan_stats)
                : icmp_(icmp), flist_(flist),
                  index_(flist->size()),
                  scan_stats_(scan_stats) {  // Marks as invalid
        }

        bool Valid() const override { return index_ < flist_->size(); }

        void Seek(const Slice &target) override {
            index_ = FindFile(icmp_, *flist_, target);
            seeked_ = true;
        }

        void SeekToFirst() override {
            index_ = 0;
            seeked_ = true;
        }

        void SeekToLast() override {
            index_ = flist_->empty() ? 0 : flist_->size() - 1;
            seeked_ = true;
        }

        void SkipToNextUserKey(const Slice &target) override {
            if (!seeked_) {
                Seek(target);
            }

            auto userkey = ExtractUserKey(target);
            uint64_t userkeyint;
            nova::str_to_int(userkey.data(), &userkeyint, userkey.size());
            while (Valid()) {
                auto current_key = ExtractUserKey(key());
                uint64_t pivot = 0;
                nova::str_to_int(current_key.data(), &pivot,
                                 current_key.size());
                NOVA_LOG(rdmaio::DEBUG)
                    << fmt::format("Level file skip:{} {}", userkeyint, pivot);

                if (userkeyint != pivot) {
                    return;
                }
                index_++;
            }
        }

        void Next() override {
            assert(Valid());
            index_++;
        }

        void Prev() override {
            assert(Valid());
            if (index_ == 0) {
                index_ = flist_->size();  // Marks as invalid
            } else {
                index_--;
            }
        }

        Slice key() const override {
            assert(Valid());
            return (*flist_)[index_]->largest.Encode();
        }

        Slice value() const override {
            assert(Valid());
            FileMetaData &meta = *(*flist_)[index_];
            EncodeFixed64(value_buf_, meta.number);
//            if (scan_stats_) {
//                scan_stats_->number_of_scan_sstables_ += 1;
//            }
            return Slice(value_buf_, 8);
        }

        Status status() const override { return Status::OK(); }

    private:
        const InternalKeyComparator icmp_;
        const std::vector<FileMetaData *> *const flist_;
        uint32_t index_;
        ScanStats *scan_stats_ = nullptr;
        bool seeked_ = false;

        // Backing store for value().  Holds the file number and size.
        mutable char value_buf_[8];
    };

    static Iterator *
    GetFileIterator(void *arg, void *arg2, BlockReadContext context,
                    const ReadOptions &options,
                    const Slice &file_value, std::string *next_key) {
        VersionFileMap *v = reinterpret_cast<VersionFileMap *>(arg);
        NOVA_ASSERT(v);
        ScanStats *scan_stats = reinterpret_cast<ScanStats *>(arg2);
        if (scan_stats) {
            scan_stats->number_of_scan_sstables_ += 1;
        }
        NOVA_ASSERT(file_value.size() == 8);
        uint64_t fn = DecodeFixed64(file_value.data());
        FileMetaData *meta = v->file_meta(fn);
        NOVA_ASSERT(meta) << fn;
        return v->table_cache()->NewIterator(context.caller, options,
                                             meta,
                                             meta->number,
                                             meta->SelectReplica(),
                                             context.level,
                                             meta->converted_file_size);
    }

    Iterator *Version::NewConcatenatingIterator(const ReadOptions &options,
                                                int level,
                                                ScanStats *scan_stats) const {
        BlockReadContext context = {
                .caller = AccessCaller::kUserIterator,
                .file_number = 0,
                .level = level,
        };
        return NewTwoLevelIterator(
                new LevelFileNumIterator(*icmp_, &files_[level], scan_stats),
                context,
                &GetFileIterator,
                (void *) this, scan_stats, options);
    }

    void Version::AddIterators(const ReadOptions &options,
                               const RangeIndex *range_index,
                               std::vector<Iterator *> *iters,
                               ScanStats *stats) {
        NOVA_ASSERT(range_index);
        NOVA_ASSERT(iters);
        BlockReadContext context = {
                .caller = AccessCaller::kUserIterator,
                .file_number = 0,
                .level = 0,
        };
        iters->push_back(NewTwoLevelIterator(
                new RangeIndexIterator(icmp_, range_index, stats),
                context, &GetRangeIndexFragmentIterator,
                (void *) this, (void *) range_index, options, icmp_, true));
        // For levels > 0, we can use a concatenating iterator that sequentially
        // walks through the non-overlapping files in the level, opening them
        // lazily.
        for (int level = 1; level < options_->level; level++) {
            if (!files_[level].empty()) {
                iters->push_back(
                        NewConcatenatingIterator(options, level, stats));
            }
        }
    }

// Callback from TableCache::Get()
    namespace {
        enum SaverState {
            kNotFound,
            kFound,
            kDeleted,
            kCorrupt,
        };
        struct Saver {
            SaverState state;
            const Comparator *ucmp;
            Slice user_key;
            SequenceNumber *seq;
            std::string *value;
        };
    }  // namespace
    static void SaveValue(void *arg, const Slice &ikey, const Slice &v) {
        Saver *s = reinterpret_cast<Saver *>(arg);
        ParsedInternalKey parsed_key;
        if (!ParseInternalKey(ikey, &parsed_key)) {
            s->state = kCorrupt;
        } else {
            if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
                s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
                if (s->state == kFound) {
                    s->value->assign(v.data(), v.size());
                    *s->seq = parsed_key.sequence;
                }
            }
        }
    }

    static bool NewestFirst(FileMetaData *a, FileMetaData *b) {
        return a->number > b->number;
    }

    void
    Version::ForEachOverlapping(Slice user_key, Slice internal_key, void *arg,
                                bool (*func)(void *, int, FileMetaData *),
                                GetSearchScope search_scope) {
        const Comparator *ucmp = icmp_->user_comparator();
        // Search level-0 first.
        if (search_scope == GetSearchScope::kAllLevels) {
            std::vector<FileMetaData *> tmp;
            tmp.reserve(files_[0].size());
            for (uint32_t i = 0; i < files_[0].size(); i++) {
                FileMetaData *f = files_[0][i];
                if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
                    ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
                    tmp.push_back(f);
                }
            }
            for (uint32_t i = 0; i < tmp.size(); i++) {
                if (nova::NovaConfig::config->use_ordered_flush && !(*func)(arg, 0, tmp[i])) {
                    return;
                }
            }
        }

        // Search other levels.
        for (int level = 1; level < options_->level; level++) {
            size_t num_files = files_[level].size();
            if (num_files == 0) continue;

            // Binary search to find earliest index whose largest key >= internal_key.
            uint32_t index = FindFile(*icmp_, files_[level],
                                      internal_key);
            if (index < num_files) {
                FileMetaData *f = files_[level][index];
                if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
                    // All of "f" is past any data for user_key
                } else {
                    if (nova::NovaConfig::config->use_ordered_flush) {
                        // Return immediately when we find the value.
                        if (!(*func)(arg, level, f)) {
                            break;
                        }
                    } else {
                        (*func)(arg, level, f);
                    }
                }
            }
        }
    }

    Status Version::Get(const leveldb::ReadOptions &options,
                        std::vector<uint64_t> &fns,
                        const leveldb::LookupKey &key,
                        SequenceNumber *seq,
                        std::string *val, uint64_t *num_searched_files) {
        bool found = false;
        for (int i = fns.size() - 1; i >= 0; i--) {
            auto fn = fns[i];
            if (fn_files_.find(fn) == fn_files_.end()) {
                return Status::IOError(fmt::format("fn {} not found", fn));
            }

            FileMetaData *file = fn_files_[fn];
            NOVA_ASSERT(file) << fn;
            NOVA_ASSERT(file->number == fn);

            if (icmp_->user_comparator()->Compare(
                    file->smallest.user_key(), key.user_key()) > 0) {
                continue;
            }
            if (icmp_->user_comparator()->Compare(
                    file->largest.user_key(), key.user_key()) < 0) {
                continue;
            }
            *num_searched_files += 1;
            SequenceNumber tmp_seq;
            Saver saver;
            saver.state = kNotFound;
            saver.ucmp = icmp_->user_comparator();
            saver.user_key = key.user_key();
            saver.value = val;
            saver.seq = &tmp_seq;
            Status s = table_cache_->Get(options,
                                         file,
                                         file->number,
                                         file->SelectReplica(),
                                         file->converted_file_size,
                                         0,
                                         key.internal_key(),
                                         &saver,
                                         SaveValue);
            if (saver.state == kFound) {
                found = true;
                if (tmp_seq > *seq) {
                    // A newer value.
                    *seq = tmp_seq;
//                    val->assign(tmp_val);
                }
            }
        }
        if (found) {
            return Status::OK();
        }
        return Status::NotFound("Not found in L0");
    }

    Status Version::Get(const ReadOptions &options, const LookupKey &k,
                        SequenceNumber *seq,
                        std::string *value, GetStats *stats,
                        GetSearchScope search_scope,
                        uint64_t *num_searched_files) {
        stats->seek_file = nullptr;
        stats->seek_file_level = -1;

        struct State {
            Saver saver;
            GetStats *stats;
            const ReadOptions *options;
            Slice ikey;
            FileMetaData *last_file_read;
            int last_file_read_level;

            TableCache *table_cache;
            Status s;
            bool found;
            uint64_t *num_searched_files;

            static bool Match(void *arg, int level, FileMetaData *f) {
                State *state = reinterpret_cast<State *>(arg);
                if (state->stats->seek_file == nullptr &&
                    state->last_file_read != nullptr) {
                    // We have had more than one seek for this read.  Charge the 1st file.
                    state->stats->seek_file = state->last_file_read;
                    state->stats->seek_file_level = state->last_file_read_level;
                }

                state->last_file_read = f;
                state->last_file_read_level = level;
                state->s = state->table_cache->Get(*state->options,
                                                   f,
                                                   f->number,
                                                   f->SelectReplica(),
                                                   f->converted_file_size,
                                                   level,
                                                   state->ikey,
                                                   &state->saver,
                                                   SaveValue);
                (*state->num_searched_files) += 1;

                if (!state->s.ok()) {
                    state->found = true;
                    return false;
                }
                switch (state->saver.state) {
                    case kNotFound:
                        return true;  // Keep searching in other files
                    case kFound:
                        state->found = true;
                        return false;
                    case kDeleted:
                        return false;
                    case kCorrupt:
                        state->s =
                                Status::Corruption("corrupted key for ",
                                                   state->saver.user_key);
                        state->found = true;
                        return false;
                }

                // Not reached. Added to avoid false compilation warnings of
                // "control reaches end of non-void function".
                return false;
            }
        };

        State state;
        state.found = false;
        state.stats = stats;
        state.last_file_read = nullptr;
        state.last_file_read_level = -1;
        state.options = &options;
        state.ikey = k.internal_key();
        state.table_cache = table_cache_;
        state.saver.state = kNotFound;
        state.saver.ucmp = icmp_->user_comparator();
        state.saver.user_key = k.user_key();
        state.saver.seq = seq;
        state.saver.value = value;
        state.num_searched_files = num_searched_files;
        ForEachOverlapping(state.saver.user_key, state.ikey, &state,
                           &State::Match, search_scope);
        return state.found ? state.s : Status::NotFound("Not found in L1.");
    }

    void Version::Ref() {
        ++refs_;
    }

    uint32_t Version::Unref() {
        uint32_t refs = 0;
        assert(refs_ >= 1);
        --refs_;
        refs = refs_;
        return refs;
    }

    void Version::ComputeOverlappingFilesPerTable(
            std::unordered_map<uint64_t, leveldb::FileMetaData *> *files,
            std::vector<leveldb::OverlappingStats> *num_overlapping) {
        for (auto pivot_it = files->begin();
             pivot_it != files->end(); pivot_it++) {
            Slice lower = pivot_it->second->smallest.user_key();
            Slice upper = pivot_it->second->largest.user_key();

            OverlappingStats stats = {};
            stats.num_overlapping_tables = 1;
            stats.total_size = pivot_it->second->file_size;
            nova::str_to_int(lower.data(), &stats.smallest, lower.size());
            nova::str_to_int(upper.data(), &stats.largest, upper.size());

            for (auto comp_it = files->begin();
                 comp_it != files->end(); comp_it++) {
                if (comp_it == pivot_it) {
                    continue;
                }
                if (icmp_->user_comparator()->Compare(lower,
                                                      comp_it->second->largest.user_key()) >
                    0) {
                    continue;
                }
                if (icmp_->user_comparator()->Compare(upper,
                                                      comp_it->second->smallest.user_key()) <
                    0) {
                    continue;
                }
                stats.num_overlapping_tables += 1;
                stats.total_size += comp_it->second->file_size;
            }
            num_overlapping->push_back(stats);
        }

        {
            auto comp = [&](const OverlappingStats &a,
                            const OverlappingStats &b) {
                return a.num_overlapping_tables > b.num_overlapping_tables;
            };
            std::sort(num_overlapping->begin(), num_overlapping->end(),
                      comp);
        }
    }

    Version::OverlappingFileResult Version::ComputeOverlappingFilesInRange(
            std::vector<FileMetaData *> *files,
            int which,
            Compaction *compaction,
            const Slice &lower,
            const Slice &upper,
            Slice *new_lower,
            Slice *new_upper) {
        *new_lower = lower;
        *new_upper = upper;
        Version::OverlappingFileResult result = OVERLAP_NO_NEW_TABLES;
        auto it = files->begin();
        // Find all overlapping tables.
        while (it != files->end()) {
            auto *file = *it;
            if (icmp_->user_comparator()->Compare(lower,
                                                  file->largest.user_key()) >
                0) {
                it++;
                continue;
            }
            if (icmp_->user_comparator()->Compare(upper,
                                                  file->smallest.user_key()) <
                0) {
                it++;
                continue;
            }

            // overlap.
            result = OVERLAP_FOUND_NEW_TABLES;
            compaction->inputs_[which].push_back(file);
            // Take the smallest user key.
            if (icmp_->user_comparator()->Compare(file->smallest.user_key(),
                                                  *new_lower) <
                0) {
                *new_lower = file->smallest.user_key();
            }
            if (icmp_->user_comparator()->Compare(file->largest.user_key(),
                                                  *new_upper) >
                0) {
                *new_upper = file->largest.user_key();
            }
            if (compaction->inputs_[which].size() >
                options_->max_num_sstables_in_nonoverlapping_set) {
                return OVERLAP_EXCEED_MAX;
            }
            it = files->erase(it);
        }
        return result;
    }

    void Version::ComputeOverlappingFilesForRange(
            std::vector<FileMetaData *> *l0inputs,
            std::vector<FileMetaData *> *l1inputs,
            Compaction *compaction) {
        Slice lower = (*l0inputs->begin())->smallest.user_key();
        Slice upper = (*l0inputs->begin())->largest.user_key();;
        Slice new_lower = lower;
        Slice new_upper = upper;

        while (true) {
            OverlappingFileResult l0_result = ComputeOverlappingFilesInRange(
                    l0inputs, 0,
                    compaction,
                    lower, upper,
                    &new_lower,
                    &new_upper);
            lower = new_lower;
            upper = new_upper;
            OverlappingFileResult l1_result = ComputeOverlappingFilesInRange(
                    l1inputs, 1,
                    compaction,
                    lower, upper,
                    &new_lower,
                    &new_upper);
            lower = new_lower;
            upper = new_upper;

            if (compaction->num_input_files(0) +
                compaction->num_input_files(1) >
                options_->max_num_sstables_in_nonoverlapping_set) {
                break;
            }
            if (l0_result == OVERLAP_EXCEED_MAX ||
                l1_result == OVERLAP_EXCEED_MAX) {
                break;
            }
            if (l0_result == OVERLAP_FOUND_NEW_TABLES ||
                l1_result == OVERLAP_FOUND_NEW_TABLES) {
                continue;
            }
            break;
        }
    }

    void Version::ComputeOverlappingFilesStats(
            std::unordered_map<uint64_t, leveldb::FileMetaData *> *files,
            std::vector<OverlappingStats> *num_overlapping) {
        // Compute overlapping sstables at L0.
        while (!files->empty()) {
            uint64_t pivot = files->begin()->first;
            Slice lower = files->begin()->second->smallest.user_key();
            Slice upper = files->begin()->second->largest.user_key();
            OverlappingStats stats = {};
            stats.num_overlapping_tables = 1;
            stats.total_size = files->begin()->second->file_size;

            files->erase(pivot);
            auto it = files->begin();

            // Find all overlapping tables.
            while (it != files->end()) {
                auto *file = it->second;
                if (icmp_->user_comparator()->Compare(lower,
                                                      file->largest.user_key()) >
                    0) {
                    it++;
                    continue;
                }
                if (icmp_->user_comparator()->Compare(upper,
                                                      file->smallest.user_key()) <
                    0) {
                    it++;
                    continue;
                }
                // overlap.
                stats.num_overlapping_tables++;
                stats.total_size += file->file_size;
                // Take the smallest user key.
                if (icmp_->user_comparator()->Compare(file->smallest.user_key(),
                                                      lower) <
                    0) {
                    lower = file->smallest.user_key();
                }
                if (icmp_->user_comparator()->Compare(file->largest.user_key(),
                                                      upper) >
                    0) {
                    upper = file->largest.user_key();
                }
                it = files->erase(it);
                it = files->begin();
            }
            nova::str_to_int(lower.data(), &stats.smallest, lower.size());
            nova::str_to_int(upper.data(), &stats.largest, upper.size());
            num_overlapping->push_back(stats);
        }

        {
            auto comp = [&](const OverlappingStats &a,
                            const OverlappingStats &b) {
                return a.smallest < b.smallest;
            };
            std::sort(num_overlapping->begin(), num_overlapping->end(),
                      comp);
        }
    }

    void Version::QueryStats(leveldb::DBStats *stats, bool detailed_stats) {
        stats->needs_compaction = NeedsCompaction();
        if (!detailed_stats) {
            for (int level = 0; level < options_->level; level++) {
                const std::vector<FileMetaData *> &files = files_[level];
                if (level == 0) {
                    stats->num_l0_sstables += files.size();
                }
                for (size_t i = 0; i < files.size(); i++) {
                    stats->dbsize += files[i]->file_size;
                    if (stats->sstable_size_dist) {
                        stats->sstable_size_dist[files[i]->file_size / 1024 /
                                                 1024] += 1;
                    }
                }
            }
            return;
        }
        std::unordered_map<uint64_t, FileMetaData *> all_fnfile;
        std::unordered_map<uint64_t, FileMetaData *> new_fnfile;
        for (int level = 0; level < options_->level; level++) {
            const std::vector<FileMetaData *> &files = files_[level];
            if (level == 0) {
                stats->num_l0_sstables += files.size();
            }
            for (size_t i = 0; i < files.size(); i++) {
                stats->dbsize += files[i]->file_size;
                stats->sstable_size_dist[files[i]->file_size / 1024 /
                                         1024] += 1;
                if (level == 0) {
                    all_fnfile[files[i]->number] = files[i];
                    if (last_fnfile.find(files[i]->number) ==
                        last_fnfile.end()) {
                        new_fnfile[files[i]->number] = files[i];
                        last_fnfile[files[i]->number] = files[i];
                    }
                }
            }
        }

        stats->new_l0_sstables_since_last_query = new_fnfile.size();
        ComputeOverlappingFilesPerTable(&all_fnfile,
                                        &stats->num_overlapping_sstables_per_table);
        ComputeOverlappingFilesStats(&all_fnfile,
                                     &stats->num_overlapping_sstables);
        NOVA_ASSERT(all_fnfile.empty());

        ComputeOverlappingFilesPerTable(&new_fnfile,
                                        &stats->num_overlapping_sstables_per_table_since_last_query);
        ComputeOverlappingFilesStats(&new_fnfile,
                                     &stats->num_overlapping_sstables_since_last_query);
        NOVA_ASSERT(new_fnfile.empty());
    }

    std::string Version::DebugString() const {
        std::string r;
        r.append("vid: ");
        AppendNumberTo(&r, version_id_);
        r.append("compaction-level: ");
        AppendNumberTo(&r, compaction_level_);

        r.append("compaction-score: ");
        r.append(std::to_string(compaction_score_));
        r.append("l0-bytes: ");
        r.append(std::to_string(l0_bytes_));
        r.append("\n");

        for (int level = 0; level < options_->level; level++) {
            r.append("sstables,");
            AppendNumberTo(&r, level);
            r.append(",");
            AppendNumberTo(&r, files_[level].size());
            r.append(",");
            uint64_t size = 0;
            const std::vector<FileMetaData *> &files = files_[level];
            for (size_t i = 0; i < files.size(); i++) {
                size += files[i]->file_size;
            }
            AppendNumberTo(&r, size);
            r.append("\n");

            // E.g.,
            //   --- level 1 ---
            //   17:123['a' .. 'd']
            //   20:43['e' .. 'g']
            r.append("--- level ");
            AppendNumberTo(&r, level);
            r.append(" ---\n");
            for (size_t i = 0; i < files.size(); i++) {
                r.append(files[i]->DebugString());
                r.append("\n");
            }
        }
        return r;
    }

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
    class VersionSet::Builder {
    private:
        // Helper to sort by v->files_[file_number].smallest
        struct BySmallestKey {
            const InternalKeyComparator *internal_comparator;

            bool operator()(FileMetaData *f1, FileMetaData *f2) const {
                int r = internal_comparator->Compare(f1->smallest,
                                                     f2->smallest);
                if (r != 0) {
                    return (r < 0);
                } else {
                    // Break ties by file number
                    return (f1->number < f2->number);
                }
            }
        };

        typedef std::set<FileMetaData *, BySmallestKey> FileSet;
        struct LevelState {
            std::set<uint64_t> deleted_files;
            FileSet *added_files;
        };

        VersionSet *vset_;
        Version *base_;
        std::vector<LevelState> levels_;
        std::unordered_map<uint64_t, FileMetaData *> added_files_map;
        bool update_replica_locations_ = false;

    public:
        // Initialize a builder with the files from *base and other info from *vset
        Builder(VersionSet *vset, Version *base) : vset_(vset), base_(base) {
            Version *v = vset->versions_[base->version_id_]->Ref();
            NOVA_ASSERT(v == base);
            BySmallestKey cmp;
            cmp.internal_comparator = &vset_->icmp_;
            levels_.resize(vset->options_->level);
            for (int level = 0; level < vset->options_->level; level++) {
                levels_[level].added_files = new FileSet(cmp);
            }
        }

        ~Builder() {
            for (int level = 0; level < levels_.size(); level++) {
                const FileSet *added = levels_[level].added_files;
                std::vector<FileMetaData *> to_unref;
                to_unref.reserve(added->size());
                for (FileSet::const_iterator it = added->begin();
                     it != added->end();
                     ++it) {
                    to_unref.push_back(*it);
                }
                delete added;
                for (uint32_t i = 0; i < to_unref.size(); i++) {
                    FileMetaData *f = to_unref[i];
                    f->refs--;
                    if (f->refs <= 0) {
//                        delete f;
                    }
                }
            }
            vset_->versions_[base_->version_id_]->Unref(vset_->dbname_);
        }

        // Apply all of the edits in *edit to the current state.
        void Apply(VersionEdit *edit) {
            // Update compaction pointers
//            for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
//                const int level = edit->compact_pointers_[i].first;
//                vset_->compact_pointer_[level] =
//                        edit->compact_pointers_[i].second.Encode().ToString();
//            }
            update_replica_locations_ = edit->update_replica_locations_;
            // Delete files
            for (const auto &deleted_file_set_kvp : edit->deleted_files_) {
                const int level = deleted_file_set_kvp.first;
                const uint64_t number = deleted_file_set_kvp.second.fnumber;
                levels_[level].deleted_files.insert(number);
            }

            // Add new files
            for (size_t i = 0; i < edit->new_files_.size(); i++) {
                const int level = edit->new_files_[i].first;
                FileMetaData *f = new FileMetaData(edit->new_files_[i].second);
                f->refs = 1;

                // We arrange to automatically compact this file after
                // a certain number of seeks.  Let's assume:
                //   (1) One seek costs 10ms
                //   (2) Writing or reading 1MB costs 10ms (100MB/s)
                //   (3) A compaction of 1MB does 25MB of IO:
                //         1MB read from this level
                //         10-12MB read from next level (boundaries may be misaligned)
                //         10-12MB written to next level
                // This implies that 25 seeks cost the same as the compaction
                // of 1MB of data.  I.e., one seek costs approximately the
                // same as the compaction of 40KB of data.  We are a little
                // conservative and allow approximately one seek for every 16KB
                // of data before triggering a compaction.
                f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
                if (f->allowed_seeks < 100) f->allowed_seeks = 100;

                levels_[level].deleted_files.erase(f->number);
                levels_[level].added_files->insert(f);
                added_files_map[f->number] = f;
            }
        }

        // Save the current state in *v.
        void SaveTo(Version *v) {
            BySmallestKey cmp;
            cmp.internal_comparator = &vset_->icmp_;
            for (int level = 0; level < levels_.size(); level++) {
                // Merge the set of added files with the set of pre-existing files.
                // Drop any deleted files.  Store the result in *v.
                const std::vector<FileMetaData *> &base_files = base_->files_[level];
                std::vector<FileMetaData *>::const_iterator base_iter = base_files.begin();
                std::vector<FileMetaData *>::const_iterator base_end = base_files.end();
                const FileSet *added_files = levels_[level].added_files;

                if (update_replica_locations_) {
                    v->files_[level].reserve(base_files.size());
                    // Add all files
                    for (; base_iter != base_end; ++base_iter) {
                        auto meta = *base_iter;
                        auto new_f = added_files_map.find(meta->number);
                        if (new_f == added_files_map.end()) {
                            MaybeAddFile(v, level, *base_iter);
                        } else {
                            MaybeAddFile(v, level, new_f->second);
                        }
                    }
                } else {
                    v->files_[level].reserve(base_files.size() + added_files->size());
                    for (const auto &added_file : *added_files) {
                        // Add all smaller files listed in base_
                        for (std::vector<FileMetaData *>::const_iterator bpos =
                                std::upper_bound(base_iter, base_end, added_file, cmp);
                             base_iter != bpos; ++base_iter) {
                            MaybeAddFile(v, level, *base_iter);
                        }
                        NOVA_ASSERT(added_file->compaction_status !=
                                    FileCompactionStatus::COMPACTING)
                            << fmt::format("{}@{}", added_file->number, level);
                        MaybeAddFile(v, level, added_file);
                    }

                    // Add remaining base files
                    for (; base_iter != base_end; ++base_iter) {
                        MaybeAddFile(v, level, *base_iter);
                    }
                }
#ifndef NDEBUG
                // Make sure there is no overlap in levels > 0
                if (level > 0) {
                    for (uint32_t i = 1; i < v->files_[level].size(); i++) {
                        const InternalKey &prev_end = v->files_[level][i - 1]->largest;
                        const InternalKey &this_begin = v->files_[level][i]->smallest;
                        if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
                            fprintf(stderr,
                                    "overlapping ranges in same level %s vs. %s\n",
                                    prev_end.DebugString().c_str(),
                                    this_begin.DebugString().c_str());
                            abort();
                        }
                    }
                }
#endif
            }
        }

        void MaybeAddFile(Version *v, int level, FileMetaData *f) {
            if (levels_[level].deleted_files.count(f->number) > 0) {
                // File is deleted: do nothing
            } else {
                std::vector<FileMetaData *> *files = &v->files_[level];
                if (level > 0 && !files->empty()) {
                    // Must not overlap
                    NOVA_ASSERT(vset_->icmp_.Compare((*files)[files->size() - 1]->largest, f->smallest) < 0)
                        << fmt::format("level:{} fn:{} f:{} v:{}", level, f->number, f->DebugString(),
                                       v->DebugString());
                }
                f->refs++;
                files->push_back(f);
                if (f->level == 0) {
                    v->l0_bytes_ += f->file_size;
                }
                NOVA_ASSERT(v->fn_files_.find(f->number) == v->fn_files_.end())
                    << fmt::format("{}@{}", f->number, level);
                v->fn_files_[f->number] = f;
            }
        }
    };

    VersionSet::VersionSet(const std::string &dbname, const Options *options,
                           TableCache *table_cache,
                           const InternalKeyComparator *cmp)
            : env_(options->env),
              dbname_(dbname),
              options_(options),
              table_cache_(table_cache),
              icmp_(*cmp),
              next_file_number_(2),
              last_sequence_(1),
              descriptor_file_(nullptr),
              descriptor_log_(nullptr),
              version_id_seq_(0),
              dummy_versions_(cmp, table_cache, options, version_id_seq_++,
                              nullptr),
              current_(nullptr) {
        for (int i = 0; i < MAX_LIVE_MEMTABLES; i++) {
            mid_table_mapping_[i] = new AtomicMemTable;
            mid_table_mapping_[i]->generation_id_ = 0;
            mid_table_mapping_[i]->is_scheduled_for_flushing = false;
            mid_table_mapping_[i]->nentries_ = 0;
            versions_[i] = new AtomicVersion;
        }
        AppendVersion(
                new Version(cmp, table_cache, options, version_id_seq_++,
                            this));
    }

    VersionSet::~VersionSet() {
        versions_[current_->version_id_]->Unref(dbname_);
        assert(dummy_versions_.next_ ==
               &dummy_versions_);  // List must be empty
        delete descriptor_log_;
        delete descriptor_file_;
    }

    void VersionSet::AppendVersion(Version *v) {
        // Make "v" current
        assert(v->refs_ == 0);
        assert(v != current_);
        if (current_ != nullptr) {
            versions_[current_->version_id_]->Unref(dbname_);
        }
        current_ = v;

        // Append to linked list
        v->prev_ = dummy_versions_.prev_;
        v->next_ = &dummy_versions_;
        v->prev_->next_ = v;
        v->next_->prev_ = v;

        versions_[v->version_id_]->SetVersion(v);
        current_version_id_.store(v->version_id_);
    }

    void VersionSet::AppendChangesToManifest(leveldb::VersionEdit *edit,
                                             StoCWritableFileClient *manifest_file,
                                             const std::vector<uint32_t> &stoc_id) {
        if (!manifest_file) {
            return;
        }
        manifest_lock_.lock();
        edit->SetNextFile(next_file_number_);
        edit->SetLastSequence(last_sequence_);
        char *edit_str = manifest_file->Buf();
        uint32_t msg_size = edit->EncodeTo(edit_str);
        if (NOVA_LOG_LEVEL == rdmaio::DEBUG) {
            VersionEdit decode;
            NOVA_ASSERT(decode.DecodeFrom(Slice(edit_str, msg_size)).ok());
            NOVA_LOG(rdmaio::DEBUG) << decode.DebugString();
        }
        current_manifest_file_size_ += msg_size;
        if (current_manifest_file_size_ < nova::NovaConfig::config->manifest_file_size) {
            NOVA_ASSERT(manifest_file->SyncAppend(Slice(edit_str, msg_size),
                                                  stoc_id).ok());
        } else {
            if (log_error_) {
                NOVA_LOG(rdmaio::INFO) << fmt::format("size %s too large", current_manifest_file_size_);
                log_error_ = false;
            }
        }
        manifest_lock_.unlock();
    }

    Status VersionSet::LogAndApply(VersionEdit *edit, Version *v, bool normal_update, StoCClient *client) {
        {
            Builder builder(this, current_);
            builder.Apply(edit);
            builder.SaveTo(v);
        }
        Finalize(v);
        if (edit->update_replica_locations_) {
            ReadOptions options;
            options.verify_checksums = options_->paranoid_checks;
            options.fill_cache = false;
            options.thread_id = 0;
            options.mem_manager = options_->mem_manager;
            options.stoc_client = client;

            for (auto &file : edit->new_files_) {
                auto fname = TableFileName(dbname_, file.second.number, FileInternalType::kFileData, 0);
                NOVA_ASSERT(env_->LockFile(fname, file.second.number).ok());
                env_->DeleteFile(fname);
                auto new_meta = v->fn_files_.find(file.second.number);
                NOVA_LOG(rdmaio::INFO)
                    << fmt::format("db[{}]: Update table cache new:{}", dbname_, file.second.DebugString());
                NOVA_ASSERT(new_meta != v->fn_files_.end());
                Cache::Handle *handle = nullptr;
                table_cache_->Evict(file.second.number, false);
                Status s = table_cache_->FindTable(AccessCaller::kUserGet, options, new_meta->second,
                                                   new_meta->second->number, 0, new_meta->second->converted_file_size,
                                                   file.first, &handle, true);
                NOVA_ASSERT(s.ok()) << s.ToString();
                table_cache_->cache_->Release(handle);
                NOVA_ASSERT(env_->UnlockFile(fname, file.second.number).ok());
            }
        }
        NOVA_LOG(rdmaio::DEBUG) << v->DebugString();
        AppendVersion(v);
        return Status::OK();
    }

    void VersionSet::Restore(Slice *buf, uint32_t version_id, uint64_t last_sequence, uint64_t next_file_number) {
        Version *version = new Version(&icmp_, table_cache_, options_, version_id, this);
        version->Decode(buf);
        // Install recovered version
        Finalize(version);
        AppendVersion(version);
        version_id_seq_ = version_id + 1;
        next_file_number_ = next_file_number + 1;
        last_sequence_ = last_sequence;
        // Do not delete this version.
        versions_[version_id]->Ref();
    }

    Status VersionSet::Recover(Slice record,
                               std::vector<SubRange> *subrange_edits) {
        uint64_t next_file = 0;
        uint64_t last_sequence = 0;
        Builder builder(this, current_);
        Slice input = record;
        Slice next;
        while (true) {
            VersionEdit edit;
            Status s = edit.DecodeFrom(input, &next);
            input = next;
            if (!s.ok()) {
                NOVA_LOG(rdmaio::INFO)
                    << fmt::format("Read manifest file {} bytes",
                                   (uint64_t) (next.data()) -
                                   (uint64_t) (record.data()));
                current_manifest_file_size_ = (uint64_t) (next.data()) -
                                              (uint64_t) (record.data());
                break;
            }
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("stats:{} edit:{}", s.ToString(),
                               edit.DebugString());

            builder.Apply(&edit);
            if (edit.has_next_file_number_) {
                next_file = std::max(next_file, edit.next_file_number_);
            }
            if (edit.has_last_sequence_) {
                last_sequence = std::max(last_sequence,
                                         edit.last_sequence_);
            }
            if (edit.new_subranges_.empty()) {
                continue;
            }
            subrange_edits->clear();
            subrange_edits->resize(edit.new_subranges_.size());
            for (int i = 0; i < edit.new_subranges_.size(); i++) {
                SubRange &sr = edit.new_subranges_[i];
                (*subrange_edits)[sr.decoded_subrange_id] = sr;
            }
        }

        Version *v = new Version(&icmp_, table_cache_, options_,
                                 version_id_seq_++, this);
        builder.SaveTo(v);
        // Install recovered version
        Finalize(v);
        AppendVersion(v);
        next_file_number_ = next_file + 1;
        last_sequence_ = last_sequence;
        for (auto file : v->fn_files_) {
            file.second->memtable_ids.clear();
        }
        return Status::OK();
    }

    void VersionSet::MarkFileNumberUsed(uint64_t number) {
        if (next_file_number_ <= number) {
            next_file_number_ = number + 1;
        }
    }

    void VersionSet::Finalize(Version *v) {
        // Precomputed best level for next compaction
        int best_level = -1;
        double best_score = -1;
        for (int level = 0; level < options_->level - 1; level++) {
            double score;
            // Compute the ratio of current size to size limit.
            const uint64_t level_bytes = TotalFileSize(v->files_[level]);
            if (level == 0 && nova::NovaConfig::config->cfgs.size() > 1 &&
                v->files_[level].size() > options_->l0nfiles_start_compaction_trigger) {
                score = 99999;
            } else {
                if (level_bytes > 0 && level == 0 && options_->l0bytes_start_compaction_trigger == 0) {
                    score = 99999;
                } else {
                    score = static_cast<double>(level_bytes) / MaxBytesForLevel(*options_, level);
                }
            }
            if (score > best_score) {
                best_level = level;
                best_score = score;
            }
        }
        v->compaction_level_ = best_level;
        v->compaction_score_ = best_score;
    }

    int VersionSet::NumLevelFiles(int level) const {
        assert(level >= 0);
        assert(level < options_->level);
        return current_->files_[level].size();
    }

    uint64_t
    VersionSet::ApproximateOffsetOf(Version *v, const InternalKey &ikey) {
        uint64_t result = 0;
        for (int level = 0; level < options_->level; level++) {
            const std::vector<FileMetaData *> &files = v->files_[level];
            for (size_t i = 0; i < files.size(); i++) {
                if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
                    // Entire file is before "ikey", so just add the file size
                    result += files[i]->file_size;
                } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
                    // Entire file is after "ikey", so ignore
                    if (level > 0) {
                        // Files other than level 0 are sorted by meta->smallest, so
                        // no further files in this level will contain data for
                        // "ikey".
                        break;
                    }
                } else {
                    // "ikey" falls in the range for this table.  Add the
                    // approximate offset of "ikey" within the table.
//                    Table *tableptr;
//                    Iterator *iter = table_cache_->NewIterator(
//                            AccessCaller::kApproximateSize,
//                            ReadOptions(), files[i]->number, level,
//                            files[i]->file_size, &tableptr);
//                    if (tableptr != nullptr) {
//                        result += tableptr->ApproximateOffsetOf(ikey.Encode());
//                    }
//                    delete iter;
                }
            }
        }
        return result;
    }

    void VersionSet::DeleteObsoleteVersions() {
        Version *v = dummy_versions_.next_;
        while (v != &dummy_versions_) {
            Version *next = v->next_;
            if (versions_[v->version_id()]->deleted) {
                v->Destroy();
                delete v;
                v = nullptr;
            }
            v = next;
        }

    }

    void VersionSet::AddLiveFiles(std::set<uint64_t> *live,
                                  uint32_t compacting_version_id) {
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("Skipping {}", compacting_version_id);
        for (Version *v = dummy_versions_.next_; v != &dummy_versions_; v = v->next_) {
            if (v->version_id_ == compacting_version_id) {
                continue;
            }
            for (int level = 0; level < options_->level; level++) {
                const std::vector<FileMetaData *> &files = v->files_[level];
                for (size_t i = 0; i < files.size(); i++) {
                    live->insert(files[i]->number);
                }
            }
        }
    }

//// Stores the minimal range that covers all entries in inputs in
//// *smallest, *largest.
//// REQUIRES: inputs is not empty
//    void VersionSet::GetRange(const std::vector<FileMetaData *> &inputs,
//                              InternalKey *smallest, InternalKey *largest) {
//        assert(!inputs.empty());
//        smallest->Clear();
//        largest->Clear();
//        for (size_t i = 0; i < inputs.size(); i++) {
//            FileMetaData *f = inputs[i];
//            if (i == 0) {
//                *smallest = f->smallest;
//                *largest = f->largest;
//            } else {
//                if (icmp_.Compare(f->smallest, *smallest) < 0) {
//                    *smallest = f->smallest;
//                }
//                if (icmp_.Compare(f->largest, *largest) > 0) {
//                    *largest = f->largest;
//                }
//            }
//        }
//    }

    uint32_t Version::Encode(char *buf) {
        uint32_t msg_size = 0;
        msg_size += EncodeFixed32(buf + msg_size, files_.size());
        for (int level = 0; level < files_.size(); level++) {
            msg_size += EncodeFixed32(buf + msg_size, files_[level].size());
            for (auto meta : files_[level]) {
                msg_size += meta->Encode(buf + msg_size);
            }
        }
        return msg_size;
    }

    void Version::Decode(Slice *buf) {
        uint32_t read_size = 0;
        uint32_t levels;
        NOVA_ASSERT(DecodeFixed32(buf, &levels));
        NOVA_ASSERT(levels == options_->level);
        for (int level = 0; level < levels; level++) {
            uint32_t nfiles = 0;
            NOVA_ASSERT(DecodeFixed32(buf, &nfiles));
            for (int i = 0; i < nfiles; i++) {
                FileMetaData *meta = new FileMetaData;
                NOVA_ASSERT(meta->Decode(buf, false));
                files_[level].push_back(meta);
                fn_files_[meta->number] = meta;
                if (level == 0) {
                    l0_bytes_ += meta->file_size;
                }
            }
        }
    }

    uint32_t
    VersionSet::EncodeTableIdMapping(char *buf, uint32_t latest_memtableid) {
        uint32_t msg_size = 0;
        for (int i = 0; i < latest_memtableid; i++) {
            auto atomic_memtable = mid_table_mapping_[i];
            atomic_memtable->mutex_.lock();
            if (atomic_memtable->memtable_ == nullptr && atomic_memtable->l0_file_numbers_.empty()) {
                atomic_memtable->mutex_.unlock();
                continue;
            }
            msg_size += EncodeFixed32(buf + msg_size, atomic_memtable->memtable_id_);
            msg_size += atomic_memtable->Encode(buf + msg_size);
            atomic_memtable->mutex_.unlock();
            NOVA_LOG(rdmaio::INFO) << fmt::format("tableid mapping: {}", atomic_memtable->memtable_id_);
        }
        msg_size += EncodeFixed32(buf + msg_size, 0);
        return msg_size;
    }

    void VersionSet::DecodeTableIdMapping(Slice *buf, const InternalKeyComparator &cmp,
                                          std::unordered_map<uint32_t, leveldb::MemTableLogFilePair> *mid_table_map) {
        while (true) {
            uint32_t mid = 0;
            NOVA_ASSERT(DecodeFixed32(buf, &mid));
            if (mid == 0) {
                break;
            }
            NOVA_LOG(rdmaio::INFO) << fmt::format("Decode tableid mapping: {}", mid);
            NOVA_ASSERT(mid < MAX_LIVE_MEMTABLES);
            if (!mid_table_mapping_[mid]->Decode(buf, cmp)) {
                NOVA_LOG(rdmaio::INFO)
                    << fmt::format("MemTable does not exist in memtable partitions {}:{}", dbname_, mid);
            }
            if (mid_table_mapping_[mid]->memtable_) {
                (*mid_table_map)[mid].memtable = mid_table_mapping_[mid]->memtable_;
            }
        }
    }

    void VersionSet::AddCompactedInputs(leveldb::Compaction *c,
                                        std::unordered_map<uint64_t, leveldb::FileMetaData> *map) {
        for (int which = 0; which < 2; which++) {
            if (!c->inputs_[which].empty()) {
                const std::vector<FileMetaData *> &files = c->inputs_[which];
                for (size_t i = 0; i < files.size(); i++) {
                    (*map)[files[i]->number] = *files[i];
                }
            }
        }
    }

    Iterator *
    Compaction::MakeInputIterator(TableCache *table_cache, EnvBGThread *bg_thread) {
        ReadOptions options;
        options.verify_checksums = options_->paranoid_checks;
        options.fill_cache = false;
        options.thread_id = bg_thread->thread_id();
        options.mem_manager = bg_thread->mem_manager();
        options.stoc_client = bg_thread->stoc_client();

        // Level-0 files have to be merged together.  For other levels,
        // we will make a concatenating iterator per level.
        // TODO(opt): use concatenating iterator for level-0 if there is no overlap
        const int space = (level_ == 0 ? inputs_[0].size() + 1 : 2);
        Iterator **list = new Iterator *[space];
        int num = 0;
        AccessCaller caller = AccessCaller::kCompaction;
        if (nova::NovaConfig::config->use_local_disk &&
            nova::NovaConfig::config->servers.size() == 1) {
            caller = AccessCaller::kUserIterator;
        }
        for (int which = 0; which < 2; which++) {
            if (!inputs_[which].empty()) {
                if (level_ + which == 0) {
                    const std::vector<FileMetaData *> &files = inputs_[which];
                    for (size_t i = 0; i < files.size(); i++) {
                        list[num++] = table_cache->NewIterator(
                                caller, options,
                                files[i],
                                files[i]->number,
                                files[i]->SelectReplica(),
                                level_ + which,
                                files[i]->converted_file_size);
                    }
                } else {
                    // Create concatenating iterator for the files from this level
                    BlockReadContext context = {
                            .caller = caller,
                            .file_number = 0,
                            .level = level_ + which,
                    };
                    list[num++] = NewTwoLevelIterator(
                            new Version::LevelFileNumIterator(*icmp_,
                                                              &inputs_[which],
                                                              nullptr),
                            context,
                            &GetFileIterator, input_version_, nullptr, options);
                }
            }
        }
        assert(num <= space);
        Iterator *result = NewMergingIterator(icmp_, list, num);
        delete[] list;
        return result;
    }

    bool Version::AssertNonOverlappingSet(
            const std::vector<leveldb::Compaction *> &compactions,
            std::string *reason) {
        // Assert the sets are non overlapping and a file must exist in only one set.
        if (compactions.empty()) {
            return true;
        }
        std::set<uint64_t> seen_files;
        struct Range {
            Slice smallest = {};
            Slice largest = {};
        };
        std::vector<Range> ranges;

        std::string debug;
        for (int i = 0; i < compactions.size(); i++) {
            auto c = compactions[i];
            debug += fmt::format("set{}:{}\n", i,
                                 c->DebugString(icmp_->user_comparator()));
        }

        for (int i = 0; i < compactions.size(); i++) {
            auto c = compactions[i];
            if (c->inputs_[0].size() + c->inputs_[1].size() >
                options_->max_num_sstables_in_nonoverlapping_set) {
                *reason = fmt::format(
                        "compaction exceeds maximum number of tables {}",
                        debug);
                return false;
            }

            Range r = {};
            for (int which = 0; which < 2; which++) {
                for (auto file : c->inputs_[which]) {
                    if (seen_files.find(file->number) != seen_files.end()) {
                        *reason = fmt::format("file {} exists in two sets {}",
                                              file->number, debug);
                        return false;
                    }
                    seen_files.insert(file->number);
                    if (r.smallest.empty() ||
                        icmp_->user_comparator()->Compare(
                                file->smallest.user_key(),
                                r.smallest) < 0) {
                        r.smallest = file->smallest.user_key();
                    }
                    if (r.largest.empty() ||
                        icmp_->user_comparator()->Compare(
                                file->largest.user_key(),
                                r.largest) < 0) {
                        r.largest = file->largest.user_key();
                    }
                }
            }
            if (icmp_->user_comparator()->Compare(r.smallest, r.largest) > 0) {
                *reason = fmt::format(
                        "the smallest is larger than largest in range {} {}", i,
                        debug);
                return false;
            }
            if (c->level() != c->target_level()) {
                ranges.push_back(r);
            }
        }

        // check sets are non overlapping.
        NOVA_ASSERT(ranges.size() <= compactions.size());
        for (int pivot = 0; pivot < ranges.size(); pivot++) {
            Range &p = ranges[pivot];
            for (int i = 0; i < ranges.size(); i++) {
                if (i == pivot) {
                    i++;
                    continue;
                }
                if (icmp_->user_comparator()->Compare(p.largest,
                                                      ranges[i].smallest) < 0) {
                    i++;
                    continue;
                }
                if (icmp_->user_comparator()->Compare(ranges[i].smallest,
                                                      p.largest) < 0) {
                    i++;
                    continue;
                }
                // overlap.
                *reason = fmt::format(
                        "two ranges are overlapping {} {} {}", pivot, i, debug);
                i++;
                return false;
            }
        }
        return true;
    }

    void Version::ComputeNonOverlappingSet(
            std::vector<leveldb::Compaction *> *compactions_result, bool *delete_due_to_low_overlap) {
        std::vector<FileMetaData *> l0files;
        std::vector<FileMetaData *> l1files;
        std::vector<Compaction *> compactions;
        int level = compaction_level_;
        NOVA_ASSERT(level >= 0 && level + 1 < options_->level);
        // fill in l0 and l1 files.
        for (int which = 0; which < 2; which++) {
            for (int i = 0; i < files_[level + which].size(); i++) {
                if (which == 0) {
                    l0files.push_back(files_[level + which][i]);
                } else {
                    l1files.push_back(files_[level + which][i]);
                }
            }
        }
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Compacting level {} {}:{}", level, l0files.size(), l1files.size());
        int set_index = 0;
        uint64_t input_size = 64;
        uint64_t compaction_size = options_->max_num_coordinated_compaction_nonoverlapping_sets;
        while (!l0files.empty() && compactions.size() < input_size) {
            auto compaction = new Compaction(this, icmp_, options_, level, level + 1);
            // Make a copy.
            {
                std::vector<FileMetaData *> l0copy(l0files);
                std::vector<FileMetaData *> l1copy(l1files);
                ComputeOverlappingFilesForRange(&l0copy, &l1copy, compaction);
                NOVA_LOG(rdmaio::DEBUG)
                    << fmt::format("Original set {}: {}", set_index,
                                   compaction->DebugString(
                                           icmp_->user_comparator()));
            }

            std::string update_reason;
            int num_l0files = compaction->num_input_files(0);
            int num_l1files = compaction->num_input_files(1);
            bool remove_overlapping_tables = true;
            // Too many tables in a set.
            // Remove files in a set so that its number of files does not exceed max.
            if (num_l0files + num_l1files >
                options_->max_num_sstables_in_nonoverlapping_set) {
                update_reason = fmt::format("Set too large l0:{} l1:{}",
                                            num_l0files, num_l1files);

                compaction->inputs_[0].resize(1);
                compaction->inputs_[1].clear();
                // recompute the overlapping tables at L1 for the first L0 SSTable.
                Slice smallest = compaction->inputs_[0][0]->smallest.user_key();
                Slice largest = compaction->inputs_[0][0]->largest.user_key();
                GetOverlappingInputs(l1files, smallest, largest,
                                     &compaction->inputs_[1]);
                int new_l1files = compaction->num_input_files(1);

                std::set<uint64_t> skip_files;
                skip_files.insert(compaction->inputs_[0][0]->number);
                NOVA_ASSERT(compaction->num_input_files(0) == 1);

                if (1 + new_l1files >
                    options_->max_num_sstables_in_nonoverlapping_set) {
                    update_reason += fmt::format(" Merge wide sstable l1:{}",
                                                 new_l1files);
                    remove_overlapping_tables = false;
                    // Merge this wide L0 SSTable.
                    GetOverlappingInputs(l0files, smallest, largest,
                                         &compaction->inputs_[0],
                                         options_->max_num_sstables_in_nonoverlapping_set -
                                         1, skip_files);

                    // User overlapping L1 SSTables as a guide to rebuild these L0 SSTables.
                    for (auto l1o : compaction->inputs_[1]) {
                        compaction->grandparents_.push_back(l1o);
                    }
                    compaction->inputs_[1].clear();
                    NOVA_ASSERT(compaction->inputs_[0].size() <=
                                options_->max_num_sstables_in_nonoverlapping_set);
                    compaction->target_level_ = level;
                } else if (new_l1files > 0 && 1 + new_l1files <
                                              options_->max_num_sstables_in_nonoverlapping_set) {
                    // Expand the compaction set to include more sstables at L0.
                    smallest = compaction->inputs_[1][0]->smallest.user_key();
                    largest = compaction->inputs_[1][new_l1files -
                                                     1]->largest.user_key();
                    int limit =
                            options_->max_num_sstables_in_nonoverlapping_set -
                            new_l1files - 1;
                    update_reason += fmt::format(
                            " Expand L0 tables: l0:{} l1:{} limit:{}",
                            compaction->inputs_[0].size(),
                            compaction->inputs_[1].size(),
                            limit);
                    // Only add L0 files that are within the range.
                    GetOverlappingInputs(l0files, smallest, largest,
                                         &compaction->inputs_[0], limit,
                                         skip_files, true);
                }
            }
            if (!update_reason.empty()) {
                NOVA_LOG(rdmaio::DEBUG)
                    << fmt::format("Updated set {}: {} Reason:{}", set_index,
                                   compaction->DebugString(
                                           icmp_->user_comparator()),
                                   update_reason);
            }
            NOVA_ASSERT(compaction->num_input_files(0) +
                        compaction->num_input_files(1) <=
                        options_->max_num_sstables_in_nonoverlapping_set);
            if (remove_overlapping_tables) {
                Slice smallest;
                Slice largest;
                std::vector<FileMetaData *> tables = compaction->inputs_[0];
                tables.insert(tables.end(), compaction->inputs_[1].begin(),
                              compaction->inputs_[1].end());
                GetRange(tables, &smallest, &largest);
                // Expand the range with boundary sstables.
                ExpandRangeWithBoundaries(l1files, &smallest, &largest);
                RemoveOverlapTablesWithRange(&l0files, smallest, largest);
                RemoveOverlapTablesWithRange(&l1files, smallest, largest);
            }
            RemoveTables(&l0files, compaction->inputs_[0]);
            RemoveTables(&l1files, compaction->inputs_[1]);
            set_index++;
//            if (!nova::NovaConfig::config->enable_subrange_reorg && level > 0 &&
//                compaction->inputs_[0].size() * 6 < compaction->inputs_[1].size()) {
//                NOVA_LOG(rdmaio::DEBUG) << fmt::format("Delete compaction due to too few overlaps {}:{}", level,
//                                                       compaction->DebugString(icmp_->user_comparator()));
//                *delete_due_to_low_overlap = true;
//                // too few overlaps.
//                delete compaction;
//                continue;
//            }

            std::sort(compaction->inputs_[1].begin(),
                      compaction->inputs_[1].end(),
                      [&](FileMetaData *f1, FileMetaData *f2) {
                          return icmp_->Compare(f1->smallest, f2->smallest) < 0;
                      });
            compactions.push_back(compaction);
        }

        if (compactions.size() > compaction_size) {
            std::sort(compactions.begin(), compactions.end(),
                      [](Compaction *c1, Compaction *c2) {
                          return c1->inputs_[0].size() > c2->inputs_[0].size();
                      });
            for (int i = compaction_size; i < compactions.size(); i++) {
                delete compactions[i];
                compactions[i] = nullptr;
            }
            compactions.resize(compaction_size);
        }
        if (!compactions.empty()) {
            compactions_result->insert(compactions_result->begin(),
                                       compactions.begin(), compactions.end());
        }
    }

    void Version::GetOverlappingInputs(
            std::vector<leveldb::FileMetaData *> &inputs,
            const leveldb::Slice &begin, const leveldb::Slice &end,
            std::vector<leveldb::FileMetaData *> *outputs, uint32_t limit,
            const std::set<uint64_t> &skip_files, bool contained) {
        if (limit == 0) {
            return;
        }
        int new_files = 0;
        const Comparator *user_cmp = icmp_->user_comparator();
        for (int i = 0; i < inputs.size(); i++) {
            FileMetaData *f = inputs[i];
            if (skip_files.find(f->number) != skip_files.end()) {
                continue;
            }
            const Slice file_start = f->smallest.user_key();
            const Slice file_limit = f->largest.user_key();
            bool add_file = false;
            if (user_cmp->Compare(file_limit, begin) < 0) {
                // "f" is completely before specified range; skip it
            } else if (user_cmp->Compare(file_start, end) > 0) {
                // "f" is completely after specified range; skip it
            } else {
                // overlap
                if (!contained) {
                    add_file = true;
                } else {
                    if (user_cmp->Compare(begin, file_start) < 0 &&
                        user_cmp->Compare(end, file_limit) > 0) {
                        add_file = true;
                    }
                }
            }
            if (add_file) {
                outputs->push_back(f);
                new_files++;
                if (new_files == limit) {
                    break;
                }
            }
        }
    }

    void Version::ExpandRangeWithBoundaries(
            const std::vector<leveldb::FileMetaData *> &inputs,
            leveldb::Slice *smallest, leveldb::Slice *largest) {
        int lower = -1;
        int upper = inputs.size();
        for (size_t i = 0; i < inputs.size(); i++) {
            FileMetaData *f = inputs[i];
            if (icmp_->user_comparator()->Compare(f->smallest.user_key(),
                                                  *smallest) == 0) {
                lower = i;
            }
            if (icmp_->user_comparator()->Compare(f->largest.user_key(),
                                                  *largest) == 0) {
                upper = i;
                break;
            }
        }
        NOVA_ASSERT(lower <= upper);
        while (lower > 0) {
            auto prior = inputs[lower - 1];
            if (icmp_->user_comparator()->Compare(*smallest,
                                                  prior->largest.user_key()) ==
                0) {
                *smallest = prior->smallest.user_key();
                lower--;
                continue;
            }
            break;
        }
        int size = (int) inputs.size() - 1;
        while (upper < size) {
            auto next = inputs[upper + 1];
            if (icmp_->user_comparator()->Compare(*largest,
                                                  next->smallest.user_key()) ==
                0) {
                *largest = next->largest.user_key();
                upper++;
                continue;
            }
            break;
        }
    }

    void Version::GetRange(const std::vector<leveldb::FileMetaData *> &inputs,
                           leveldb::Slice *smallest, leveldb::Slice *largest) {
        for (size_t i = 0; i < inputs.size(); i++) {
            FileMetaData *f = inputs[i];
            if (i == 0) {
                *smallest = f->smallest.user_key();
                *largest = f->largest.user_key();
            } else {
                if (icmp_->user_comparator()->Compare(f->smallest.user_key(),
                                                      *smallest) < 0) {
                    *smallest = f->smallest.user_key();
                }
                if (icmp_->user_comparator()->Compare(f->largest.user_key(),
                                                      *largest) > 0) {
                    *largest = f->largest.user_key();
                }
            }
        }
    }

    void Version::RemoveOverlapTablesWithRange(
            std::vector<leveldb::FileMetaData *> *inputs,
            const leveldb::Slice &smallest, const leveldb::Slice &largest) {
        const Comparator *user_cmp = icmp_->user_comparator();
        auto it = inputs->begin();
        while (it != inputs->end()) {
            FileMetaData *f = *it;
            const Slice lower = f->smallest.user_key();
            const Slice upper = f->largest.user_key();
            if (user_cmp->Compare(upper, smallest) < 0) {
                // "f" is completely before specified range; skip it
                it++;
            } else if (user_cmp->Compare(lower, largest) > 0) {
                // "f" is completely after specified range; skip it
                it++;
            } else {
                // overlap
                it = inputs->erase(it);
            }
        }
    }

    void Version::RemoveTables(std::vector<leveldb::FileMetaData *> *inputs,
                               const std::vector<leveldb::FileMetaData *> &remove_tables) {
        if (remove_tables.empty()) {
            return;
        }
        std::set<uint64_t> remove_fns;
        for (auto f : remove_tables) {
            remove_fns.insert(f->number);
        }
        auto it = inputs->begin();
        while (it != inputs->end()) {
            if (remove_fns.find((*it)->number) != remove_fns.end()) {
                it = inputs->erase(it);
            } else {
                it++;
            }
        }
    }

// Finds the largest key in a vector of files. Returns true if files it not
// empty.
    bool FindLargestKey(const InternalKeyComparator &icmp,
                        const std::vector<FileMetaData *> &files,
                        InternalKey *largest_key) {
        if (files.empty()) {
            return false;
        }
        *largest_key = files[0]->largest;
        for (size_t i = 1; i < files.size(); ++i) {
            FileMetaData *f = files[i];
            if (icmp.Compare(f->largest, *largest_key) > 0) {
                *largest_key = f->largest;
            }
        }
        return true;
    }

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
    FileMetaData *FindSmallestBoundaryFile(
            const InternalKeyComparator &icmp,
            const std::vector<FileMetaData *> &level_files,
            const InternalKey &largest_key) {
        const Comparator *user_cmp = icmp.user_comparator();
        FileMetaData *smallest_boundary_file = nullptr;
        for (size_t i = 0; i < level_files.size(); ++i) {
            FileMetaData *f = level_files[i];
            if (icmp.Compare(f->smallest, largest_key) > 0 &&
                user_cmp->Compare(f->smallest.user_key(),
                                  largest_key.user_key()) ==
                0) {
                if (smallest_boundary_file == nullptr ||
                    icmp.Compare(f->smallest,
                                 smallest_boundary_file->smallest) < 0) {
                    smallest_boundary_file = f;
                }
            }
        }
        return smallest_boundary_file;
    }

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
    void AddBoundaryInputs(const InternalKeyComparator &icmp,
                           const std::vector<FileMetaData *> &level_files,
                           std::vector<FileMetaData *> *compaction_files) {
        InternalKey largest_key;

        // Quick return if compaction_files is empty.
        if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
            return;
        }

        bool continue_searching = true;
        while (continue_searching) {
            FileMetaData *smallest_boundary_file =
                    FindSmallestBoundaryFile(icmp, level_files, largest_key);

            // If a boundary file was found advance largest_key, otherwise we're done.
            if (smallest_boundary_file != NULL) {
                compaction_files->push_back(smallest_boundary_file);
                largest_key = smallest_boundary_file->largest;
            } else {
                continue_searching = false;
            }
        }
    }

    void AtomicVersion::SetVersion(leveldb::Version *v) {
        mutex.lock();
        NOVA_ASSERT(!version);
        version = v;
        v->Ref();
        mutex.unlock();
    }

    bool AtomicVersion::SetCompaction() {
        bool success = false;
        mutex.lock();
        NOVA_ASSERT(version);
        NOVA_ASSERT(!deleted);
        NOVA_ASSERT(version->refs_ >= 1);
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("comv {} {}", version->version_id_, version->refs_);
        // Since we have installed a new version. If refcount == 1, it means only the compaction thread is referencing it.
        if (version->refs_ == 1) {
            success = true;
        }
        is_compacting = true;
        mutex.unlock();
        return success;
    }

    Version *AtomicVersion::Ref() {
        Version *v = nullptr;
        mutex.lock();
        if (version != nullptr && !deleted && !is_compacting) {
            v = version;
            v->Ref();
        }
        mutex.unlock();
        return v;
    }

    void AtomicVersion::Unref(const std::string &dbname) {
        mutex.lock();
        if (version && !deleted) {
            uint32_t vid = version->version_id();
            uint32_t refs = version->Unref();
            if (refs <= 0) {
                deleted = true;
            }
        }
        mutex.unlock();
    }

}  // namespace leveldb