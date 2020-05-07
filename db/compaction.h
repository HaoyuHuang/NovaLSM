
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_COMPACTION_H
#define LEVELDB_COMPACTION_H

#include <leveldb/status.h>
#include <leveldb/env_bg_thread.h>
#include <leveldb/iterator.h>
#include "version_set.h"
#include "subrange.h"

namespace leveldb {
    enum CompactType {
        kCompactMemTables,
        kCompactSSTables
    };

    struct CompactionTableStats {
        uint64_t num_files = 0;
        uint64_t file_size = 0;
        uint32_t level = 0;
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
        struct Output {
            uint64_t number;
            uint64_t file_size;
            uint64_t converted_file_size;
            InternalKey smallest, largest;
            RTableHandle meta_block_handle;
            std::vector<RTableHandle> data_block_group_handles;
        };

        Output *current_output() { return &outputs[outputs.size() - 1]; }

        explicit CompactionState(Compaction *c, SubRanges *s)
                : compaction(c),
                  subranges(s),
                  smallest_snapshot(0),
                  outfile(nullptr),
                  builder(nullptr),
                  total_bytes(0) {}

        Compaction *const compaction;

        SubRanges *subranges;

        bool ShouldStopBefore(const Slice &internal_key,
                              const Comparator *user_comparator) {
            bool subrange_stop = false;
            bool compaction_stop = false;
            if (subranges) {
                Slice userkey = ExtractUserKey(internal_key);
                if (subrange_index == -1) {
                    // Returns the first subrange that has its userkey < upper.
                    while (subrange_index < subranges->subranges.size()) {
                        SubRange &sr = subranges->subranges[subrange_index];
                        if (sr.IsGreaterThanUpper(userkey, user_comparator)) {
                            subrange_index++;
                        } else {
                            break;
                        }
                    }
                }
                while (subrange_index < subranges->subranges.size()) {
                    SubRange &sr = subranges->subranges[subrange_index];
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
        SequenceNumber smallest_snapshot;

        std::vector<Output> outputs;

        // State kept for output being generated
        MemWritableFile *outfile;
        TableBuilder *builder;
        std::vector<MemWritableFile *> output_files;

        uint64_t total_bytes;
    };

    class CompactionJob {
    public:
        CompactionJob(std::function<uint64_t(void)> &fn_generator,
                      Env *env,
                      const std::string &dbname,
                      const Comparator *user_comparator,
                      const Options &options);

        Status
        CompactTables(CompactionState *state, EnvBGThread *bg_thread,
                      Iterator *input,
                      CompactionStats *stats, bool drop_duplicates,
                      CompactType type);

    private:
        Status OpenCompactionOutputFile(CompactionState *compact,
                                        EnvBGThread *bg_thread);

        Status
        FinishCompactionOutputFile(const ParsedInternalKey& ik, CompactionState *compact, Iterator *input);

        std::function<uint64_t(void)> fn_generator_;
        Env *env_;

        const std::string dbname_;
        const Comparator *user_comparator_;
        const Options &options_;

    };

}


#endif //LEVELDB_COMPACTION_H
