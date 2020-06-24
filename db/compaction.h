
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_COMPACTION_H
#define LEVELDB_COMPACTION_H

#include "leveldb/status.h"
#include "leveldb/env_bg_thread.h"
#include "leveldb/iterator.h"

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "memtable.h"
#include "ltc/stoc_file_client_impl.h"
#include "table_cache.h"

#include "leveldb/subrange.h"
#include "ltc/stoc_client_impl.h"

#define FETCH_METADATA_BATCH_SIZE 128

namespace leveldb {

    class VersionFileMap;

    class StoCBlockClient;

    enum CompactInputType {
        kCompactInputMemTables = 0,
        kCompactInputSSTables = 1
    };

    enum CompactOutputType {
        kCompactOutputMemTables = 0,
        kCompactOutputSSTables = 1
    };

    struct CompactionTableStats {
        uint64_t num_files = 0;
        uint64_t file_size = 0;
        uint32_t level = 0;
    };

    // A Compaction encapsulates information about a compaction.
    class Compaction {
    public:
        Compaction(VersionFileMap *input_version,
                   const InternalKeyComparator *icmp,
                   const Options *options,
                   int level, int target_level);

        std::string DebugString(const Comparator *user_comparator);

        // Create an iterator that reads over the compaction inputs for "*c".
        // The caller should delete the iterator when no longer needed.
        Iterator *
        MakeInputIterator(TableCache *table_cache, EnvBGThread *bg_thread);

        // Return the level that is being compacted.  Inputs from "level"
        // and "level+1" will be merged to produce a set of "level+1" files.
        int level() const { return level_; }

        int target_level() const { return target_level_; }

        // Return the object that holds the edits to the descriptor done
        // by this compaction.
        VersionEdit *edit() { return &edit_; }

        // "which" must be either 0 or 1
        int num_input_files(int which) const { return inputs_[which].size(); }

        uint64_t num_input_file_sizes(int which) const {
            uint64_t size = 0;
            for (auto meta : inputs_[which]) {
                size += meta->file_size;
            }
            return size;
        }

        // Return the ith input file at "level()+which" ("which" must be 0 or 1).
        FileMetaData *
        input(int which, int i) const { return inputs_[which][i]; }

        // Maximum size of files to build during this compaction.
        uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

        // Is this a trivial compaction that can be implemented by just
        // moving a single input file to the next level (no merging or splitting)
        bool IsTrivialMove() const;

        // Add all inputs to this compaction as delete operations to *edit.
        void AddInputDeletions(VersionEdit *edit);

        // Returns true iff we should stop building the current output
        // before processing "internal_key".
        bool ShouldStopBefore(const Slice &internal_key);

        VersionFileMap *input_version_;

        sem_t *complete_signal_ = nullptr;
        std::atomic_bool is_completed_;

        // Each compaction reads inputs from "level_" and "level_+1"
        std::vector<FileMetaData *> inputs_[2];  // The two sets of inputs
        // State used to check for number of overlapping grandparent files
        // (parent == level_ + 1, grandparent == level_ + 2)
        std::vector<FileMetaData *> grandparents_;

    private:
        friend class Version;

        friend class VersionSet;

        int level_;
        int target_level_;
        uint64_t max_output_file_size_;
        VersionEdit edit_;
        const Options *options_;
        const InternalKeyComparator *icmp_;

        size_t grandparent_index_ = 0;  // Index in grandparent_starts_
        bool seen_key_ = false;             // Some output key has been seen
        int64_t overlapped_bytes_ = 0;  // Bytes of overlap between current output
        // and grandparent files

        // State for implementing IsBaseLevelForKey

        // level_ptrs_ holds indices into input_version_->levels_: our state
        // is that we are positioned at one of the file ranges for each
        // higher level than the ones involved in this compaction (i.e. for
        // all L >= level_ + 2).
        std::vector<size_t> level_ptrs_;
    };

    // Per level compaction stats.  stats_[level] stores the stats for
    // compactions that produced data for the specified "level".
    struct CompactionStats {
        CompactionStats() : micros(0) {}

        void Add(const CompactionStats &c) {
            this->micros += c.micros;
        }

        uint64_t micros;
        CompactionTableStats input_source = {};
        CompactionTableStats input_target = {};
        CompactionTableStats output = {};
    };

    struct CompactionState {
        // Files produced by compaction
        FileMetaData *current_output() { return &outputs[outputs.size() - 1]; }

        explicit CompactionState(Compaction *c, SubRanges *s, SequenceNumber n)
                : compaction(c),
                  subranges(s),
                  outfile(nullptr),
                  builder(nullptr),
                  total_bytes(0), smallest_snapshot(n) {}

        Compaction *const compaction = nullptr;

        SubRanges *subranges = nullptr;

        CompactionStats BuildStats();

        bool ShouldStopBefore(const Slice &internal_key,
                              const Comparator *user_comparator) {
            bool subrange_stop = false;
            bool compaction_stop = false;
            if (subranges) {
                Slice userkey = ExtractUserKey(internal_key);
                if (subrange_index == -1) {
                    // Returns the first subrange that has its userkey < upper.
                    while (subrange_index < subranges->subranges.size()) {
                        const SubRange &sr = subranges->subranges[subrange_index];
                        if (sr.IsGreaterThanUpper(userkey, user_comparator)) {
                            subrange_index++;
                        } else {
                            break;
                        }
                    }
                }
                while (subrange_index < subranges->subranges.size()) {
                    const SubRange &sr = subranges->subranges[subrange_index];
                    if (sr.IsGreaterThanUpper(userkey, user_comparator)) {
                        subrange_index++;
                        subrange_stop = true;
                    } else {
                        break;
                    }
                }
            }
            if (compaction) {
                compaction_stop = compaction->ShouldStopBefore(internal_key);
            }
            return subrange_stop || compaction_stop;
        }

        int subrange_index = 0;

        // Sequence numbers < smallest_snapshot are not significant since we
        // will never have to service a snapshot below smallest_snapshot.
        // Therefore if we have seen a sequence number S <= smallest_snapshot,
        // we can drop all entries for the same key with sequence numbers < S.
        SequenceNumber smallest_snapshot = 0;

        std::vector<FileMetaData> outputs;

        // State kept for output being generated
        MemWritableFile *outfile = nullptr;
        TableBuilder *builder = nullptr;
        uint64_t total_bytes = 0;
    };

    class CompactionJob {
    public:
        CompactionJob(std::function<uint64_t(void)> &fn_generator,
                      Env *env,
                      const std::string &dbname,
                      const Comparator *user_comparator,
                      const Options &options,
                      EnvBGThread *bg_thread, TableCache *table_cache);

        Status
        CompactTables(CompactionState *state,
                      Iterator *input,
                      CompactionStats *stats, bool drop_duplicates,
                      CompactInputType input_type,
                      CompactOutputType output_type,
                      const std::function<void(const ParsedInternalKey &ikey,
                                               const Slice &value)> &add_to_memtable = {});

    private:
        Status OpenCompactionOutputFile(CompactionState *compact);

        Status
        FinishCompactionOutputFile(const ParsedInternalKey &ik,
                                   CompactionState *compact, Iterator *input);

        std::function<uint64_t(void)> fn_generator_;
        Env *env_ = nullptr;
        EnvBGThread *bg_thread_ = nullptr;
        const std::string dbname_;
        const Comparator *user_comparator_ = nullptr;
        const Options options_;
        TableCache *table_cache_ = nullptr;
    };

    void
    FetchMetadataFilesInParallel(const std::vector<const FileMetaData *> &files,
                                 const std::string &dbname,
                                 const Options &options,
                                 StoCBlockClient *client,
                                 Env *env);

    void
    FetchMetadataFiles(const std::vector<const FileMetaData *> &files,
                       const std::string &dbname,
                       const Options &options, StoCBlockClient *client,
                       Env *env);
}


#endif //LEVELDB_COMPACTION_H
