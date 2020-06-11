
//
// Created by Haoyu Huang on 12/15/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_COMPACTION_H
#define LEVELDB_COMPACTION_H

#include "include/leveldb/db_profiler.h"
#include "version_set.h"

namespace leveldb {
    // Per level compaction stats.  stats_[level] stores the stats for
    // compactions that produced data for the specified "level".
    struct CompactionStats {
        CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

        void Add(const CompactionStats &c) {
            this->micros += c.micros;
            this->bytes_read += c.bytes_read;
            this->bytes_written += c.bytes_written;
        }

        int64_t micros;
        int64_t bytes_read;
        int64_t bytes_written;
    };

    class CompactionState {
    public:
        CompactionState(Compaction *c, Env *env,
                        const std::string &dbname,
                        const Options &options,
                        TableCache *table_cache,
                        const InternalKeyComparator internal_comparator,
                        SequenceNumber smallest_snapshot,
                        DBProfiler *db_profiler)
                : dbname_(dbname),
                  env_(env),
                  options_(options),
                  table_cache_(table_cache),
                  compaction_(c),
                  internal_comparator_(internal_comparator),
                  smallest_snapshot_(smallest_snapshot),
                  outfile_(nullptr),
                  builder_(nullptr),
                  total_bytes_(0),
                  db_profiler_(db_profiler) {
            stats_.resize(options.level);
        }

        void CleanupCompaction();

        Status DoCompactionWork();

        // Files produced by compaction
        struct Output {
            uint64_t number;
            uint64_t file_size;
            InternalKey smallest, largest;
        };

        Output *current_output() { return &outputs_[outputs_.size() - 1]; }

    private:
        Status OpenCompactionOutputFile();

        Status
        FinishCompactionOutputFile(Iterator *input);

        Status InstallCompactionResults();

        std::string dbname_;
        Env *env_;
        Options options_;
        TableCache *table_cache_;
        Compaction *const compaction_;
        const InternalKeyComparator internal_comparator_;
        // Sequence numbers < smallest_snapshot are not significant since we
        // will never have to service a snapshot below smallest_snapshot.
        // Therefore if we have seen a sequence number S <= smallest_snapshot,
        // we can drop all entries for the same key with sequence numbers < S.
        SequenceNumber smallest_snapshot_;
        std::vector<Output> outputs_;
        std::set<uint64_t> pending_outputs_;

        // State kept for output being generated
        WritableFile *outfile_;
        TableBuilder *builder_;
        uint64_t total_bytes_;
        std::vector<CompactionStats> stats_;
        DBProfiler *db_profiler_;
    };
}


#endif //LEVELDB_COMPACTION_H
