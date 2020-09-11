// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>
#include <atomic>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "memtable.h"
#include "ltc/stoc_file_client_impl.h"
#include "table_cache.h"
#include "range_index.h"

// Maintain this many live memtables.
// The program exits when the number of memtables exceeds this threshold.
#define MAX_LIVE_MEMTABLES 100000

namespace leveldb {

    namespace log {
        class Writer;
    }

    class Compaction;

    class Iterator;

    class MemTable;

    class TableBuilder;

    class TableCache;

    class Version;

    class VersionSet;

    class WritableFile;

    class RangeIndex;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
    int FindFile(const InternalKeyComparator &icmp,
                 const std::vector<FileMetaData *> &files, const Slice &key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
    bool SomeFileOverlapsRange(const InternalKeyComparator &icmp,
                               bool disjoint_sorted_files,
                               const std::vector<FileMetaData *> &files,
                               const Slice *smallest_user_key,
                               const Slice *largest_user_key);

    struct CompactionPriority {
        int level;
        double score;
    };

    enum GetSearchScope {
        kAllLevels = 0,
        kAllL0AndAllLevels = 1,
        kL1AndAbove = 2
    };

    class VersionFileMap {
    public:
        VersionFileMap(TableCache *table_cache) : table_cache_(table_cache) {}

        TableCache *table_cache() {
            return table_cache_;
        }

        FileMetaData *file_meta(uint64_t fn) {
            return fn_files_[fn];
        }

        TableCache *table_cache_;
        std::unordered_map<uint64_t, FileMetaData *> fn_files_;
    };

    class Version : public VersionFileMap {
    public:
        // Lookup the value for key.  If found, store it in *val and
        // return OK.  Else return a non-OK status.  Fills *stats.
        // REQUIRES: lock is not held
        struct GetStats {
            FileMetaData *seek_file;
            int seek_file_level;
        };

        bool NeedsCompaction() {
            if (compaction_score_ >= 1.0) {
                NOVA_ASSERT(compaction_level_ >= 0 &&
                            compaction_level_ + 1 < options_->level);
            }
            return compaction_score_ >= 1.0;
        }

        // Append to *iters a sequence of iterators that will
        // yield the contents of this Version when merged together.
        // REQUIRES: This version has been saved (see VersionSet::SaveTo)
        void AddIterators(const ReadOptions &, const RangeIndex *range_index,
                          std::vector<Iterator *> *iters, ScanStats *stats);

        Status
        Get(const ReadOptions &, const LookupKey &key, SequenceNumber *seq,
            std::string *val, GetStats *stats, GetSearchScope search_scope,
            uint64_t *num_searched_files);

        Status Get(const ReadOptions &, std::vector<uint64_t> &fns,
                   const LookupKey &key,
                   SequenceNumber *seq,
                   std::string *val, uint64_t *num_searched_files);

        // Reference count management (so Versions do not disappear out from
        // under live iterators)
        void Ref();

        uint32_t Unref();

        // Return a human readable string that describes this version's contents.
        std::string DebugString() const;

        void QueryStats(DBStats *stats, bool detailed_stats);

        void ComputeOverlappingFilesStats(
                std::unordered_map<uint64_t, FileMetaData *> *files,
                std::vector<OverlappingStats> *num_overlapping);

        enum OverlappingFileResult {
            OVERLAP_FOUND_NEW_TABLES,
            OVERLAP_EXCEED_MAX,
            OVERLAP_NO_NEW_TABLES
        };

        OverlappingFileResult ComputeOverlappingFilesInRange(
                std::vector<FileMetaData *> *files,
                int which,
                Compaction *compaction,
                const Slice &lower,
                const Slice &upper,
                Slice *new_lower,
                Slice *new_upper);

        void ComputeOverlappingFilesForRange(
                std::vector<FileMetaData *> *l0inputs,
                std::vector<FileMetaData *> *l1inputs,
                Compaction *compaction);

        void ComputeOverlappingFilesPerTable(
                std::unordered_map<uint64_t, FileMetaData *> *files,
                std::vector<OverlappingStats> *num_overlapping);

        void ComputeNonOverlappingSet(std::vector<Compaction *> *compactions, bool *delete_due_to_low_overlap);

        bool
        AssertNonOverlappingSet(const std::vector<Compaction *> &compactions,
                                std::string *reason);

        static std::unordered_map<uint64_t, FileMetaData *> last_fnfile;

        uint32_t version_id() {
            return version_id_;
        }

        void Destroy();

        explicit Version(const InternalKeyComparator *icmp,
                         TableCache *table_cache,
                         const Options *const options, uint32_t version_id,
                         VersionSet *vset)
                : VersionFileMap(table_cache),
                  icmp_(icmp),
                  options_(options),
                  next_(this),
                  prev_(this),
                  refs_(0),
                  file_to_compact_(nullptr),
                  file_to_compact_level_(-1), version_id_(version_id),
                  compaction_level_(-1), compaction_score_(-1), vset_(vset) {
            files_.resize(options->level);
        };

        ~Version();

        // List of files per level
        std::vector<std::vector<FileMetaData *>> files_;
        uint32_t version_id_ = 0;
        uint64_t l0_bytes_ = 0;
        VersionSet *vset_ = nullptr;
        const InternalKeyComparator *icmp_;
        const Options *const options_;

        uint32_t Encode(char *buf);

        void Decode(Slice *buf);

        int refs_ = 0;          // Number of live refs to this version

    private:
        friend class Compaction;

        friend class VersionSet;

        class LevelFileNumIterator;

        Version(const Version &) = delete;

        Version &operator=(const Version &) = delete;

        void GetOverlappingInputs(
                std::vector<FileMetaData *> &inputs,
                const Slice &begin,  // nullptr means before all keys
                const Slice &end,    // nullptr means after all keys
                std::vector<FileMetaData *> *outputs,
                uint32_t limit = UINT32_MAX,
                const std::set<uint64_t> &skip_files = {},
                bool contained = false);

        void GetRange(const std::vector<FileMetaData *> &inputs,
                      Slice *smallest,
                      Slice *largest);

        void
        ExpandRangeWithBoundaries(const std::vector<FileMetaData *> &inputs,
                                  Slice *smallest,
                                  Slice *largest);

        void RemoveOverlapTablesWithRange(std::vector<FileMetaData *> *inputs,
                                          const Slice &smallest,
                                          const Slice &largest);

        void RemoveTables(std::vector<FileMetaData *> *inputs,
                          const std::vector<leveldb::FileMetaData *> &remove_tables);


        Iterator *
        NewConcatenatingIterator(const ReadOptions &, int level,
                                 ScanStats *scan_stats) const;

        // Call func(arg, level, f) for every file that overlaps user_key in
        // order from newest to oldest.  If an invocation of func returns
        // false, makes no more calls.
        //
        // REQUIRES: user portion of internal_key == user_key.
        void ForEachOverlapping(Slice user_key, Slice internal_key, void *arg,
                                bool (*func)(void *, int, FileMetaData *),
                                GetSearchScope search_scope);

        Version *next_;     // Next version in linked list
        Version *prev_;     // Previous version in linked list


        // Next file to compact based on seek stats.
        FileMetaData *file_to_compact_;
        int file_to_compact_level_;

        // Level that should be compacted next and its compaction score.
        // Score < 1 means compaction is not strictly needed.  These fields
        // are initialized by Finalize().
        // Level that should be compacted next and its compaction score.
        // Score < 1 means compaction is not strictly needed.  These fields
        // are initialized by Finalize().
        double compaction_score_ = -1;
        int compaction_level_ = -1;
    };

    class AtomicVersion {
    public:
        void SetVersion(Version *v);

        Version *Ref();

        void Unref(const std::string &dbname);

        bool SetCompaction();

        std::mutex mutex;
        Version *version = nullptr;
        bool deleted = false;
        bool is_compacting = false;
    };

    class VersionSet {
    public:
        VersionSet(const std::string &dbname, const Options *options,
                   TableCache *table_cache, const InternalKeyComparator *);

        VersionSet(const VersionSet &) = delete;

        VersionSet &operator=(const VersionSet &) = delete;

        ~VersionSet();

        // Apply *edit to the current version to form a new descriptor that
        // is both saved to persistent state and installed as the new
        // current version.  Will release *mu while actually writing to the file.
        // REQUIRES: *mu is held on entry.
        // REQUIRES: no other thread concurrently calls LogAndApply()
        Status LogAndApply(VersionEdit *edit, Version *new_version,
                           bool install_new_version, StoCClient* client = nullptr);

        void AppendChangesToManifest(VersionEdit *edit,
                                     StoCWritableFileClient *manifest_file,
                                     const std::vector<uint32_t>& stoc_id);

        uint32_t current_manifest_file_size_ = 0;
        bool log_error_ = false;

        // Recover the last saved descriptor from persistent storage.
        Status Recover(Slice manifest_file,
                       std::vector<SubRange> *subrange_edits);

        void
        Restore(Slice *buf, uint32_t version_id, uint64_t last_sequence, uint64_t next_file_number);

        // Return the current version.
        Version *current() const { return current_; }

        // Allocate and return a new file number
        uint64_t NewFileNumber() {
            return next_file_number_.fetch_add(1);
        }

        uint64_t NextFileNumber() {
            return next_file_number_;
        }

        // Return the number of Table files at the specified level.
        int NumLevelFiles(int level) const;

        // Return the last sequence number.
        uint64_t LastSequence() const {
            return last_sequence_.load();
        }

        // Set the last sequence number to s.
        void SetLastSequence(uint64_t s) {
            assert(s >= last_sequence_);
            last_sequence_ = s;
        }

        // Mark the specified file number as used.
        void MarkFileNumberUsed(uint64_t number);

        void AddCompactedInputs(Compaction *c,
                                std::unordered_map<uint64_t, FileMetaData> *map);

        // Add all files listed in any live version to *live.
        // May also mutate some internal state.
        void AddLiveFiles(std::set<uint64_t> *live,
                          uint32_t compacting_version_id);

        void AppendVersion(Version *v);

        void DeleteObsoleteVersions();

        // Return the approximate offset in the database of the data for
        // "key" as of version "v".
        uint64_t ApproximateOffsetOf(Version *v, const InternalKey &key);

        uint32_t current_version_id() {
            return current_version_id_;
        }

        std::atomic_uint_fast64_t last_sequence_;
        AtomicMemTable *mid_table_mapping_[MAX_LIVE_MEMTABLES];
        AtomicVersion *versions_[MAX_LIVE_MEMTABLES];
        std::atomic_int_fast32_t version_id_seq_;
        std::atomic_int_fast64_t next_file_number_;
        std::mutex manifest_lock_;

        uint32_t EncodeTableIdMapping(char *buf, uint32_t latest_memtableid);

        void DecodeTableIdMapping(Slice *buf, const InternalKeyComparator& cmp, std::unordered_map<uint32_t, leveldb::MemTableLogFilePair> *mid_table_map);

    private:
        class Builder;

        friend class Compaction;

        friend class Version;

        void Finalize(Version *v);

        Env *const env_;
        const std::string dbname_;
        const Options *const options_;
        TableCache *const table_cache_;
        const InternalKeyComparator icmp_;
        // Opened lazily
        WritableFile *descriptor_file_;
        log::Writer *descriptor_log_;
        Version dummy_versions_;  // Head of circular doubly-linked list of versions.
        Version *current_;        // == dummy_versions_.prev_
        std::atomic_int_fast32_t current_version_id_;
    };
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H
