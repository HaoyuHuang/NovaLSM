
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
// It assumes keys are integers.
//

#ifndef LEVELDB_SUBRANGE_MANAGER_H
#define LEVELDB_SUBRANGE_MANAGER_H

#include "leveldb/subrange.h"
#include "memtable.h"
#include "version_set.h"
#include "flush_order.h"

#define SUBRANGE_WARMUP_NPUTS 1000000
#define SUBRANGE_MAJOR_REORG_INTERVAL 1000000
#define SUBRANGE_MINOR_REORG_INTERVAL 100000
#define SUBRANGE_REORG_INTERVAL 100000

#define SUBRANGE_REORG_DIFF_FROM_FAIR_THRESHOLD 20
#define SUBRANGE_MAJOR_REORG_THRESHOLD 0.3

namespace leveldb {
    class SubRangeManager {
    public:
        SubRangeManager(StoCWritableFileClient *manifest_file,
                        FlushOrder *flush_order,
                        const std::string &dbname,
                        uint32_t dbindex,
                        VersionSet *versions,
                        const Options &options,
                        const InternalKeyComparator *internal_comparator,
                        const Comparator *user_comparator,
                        std::atomic_int_fast32_t *memtable_id_seq,
                        std::vector<MemTablePartition *> *partitioned_active_memtables,
                        std::vector<uint32_t> *partitioned_imms);

        void ReorganizeSubranges();

        int SearchSubranges(const leveldb::WriteOptions &options,
                            const leveldb::Slice &key,
                            const leveldb::Slice &val,
                            SubRange **subrange);

        void ConstructSubrangesWithUniform(const Comparator *user_comparator);

        void QueryDBStats(leveldb::DBStats *db_stats);

        void ComputeCompactionThreadsAssignment(SubRanges *subranges);

        uint64_t last_major_reorg_seq_ = 0;
        uint64_t last_minor_reorg_seq_ = 0;

        uint32_t num_major_reorgs = 0;
        uint32_t num_skipped_major_reorgs = 0;
        uint32_t num_minor_reorgs = 0;
        uint32_t num_minor_reorgs_samples = 0;
        uint32_t num_minor_reorgs_for_dup = 0;
        uint32_t num_skipped_minor_reorgs = 0;

        std::atomic<SubRanges *> latest_subranges_;

        SubRanges *latest_ = nullptr;
        double total_num_inserts_since_last_major_ = 0;
        double fair_ratio_ = 0;
        VersionEdit edit_;
    private:
        uint32_t dbindex_ = 0;
        uint64_t lower_bound_ = 0;
        uint64_t upper_bound_ = 0;

        void ComputeLoadImbalance(const std::vector<double> &loads,
                                  leveldb::DBStats *db_stats);

        void ConstructRanges(const std::map<uint64_t, double> &userkey_rate,
                             double total_rate, uint64_t lower, uint64_t upper,
                             uint32_t num_ranges_to_construct,
                             bool is_constructing_subranges,
                             std::vector<Range> *ranges);

        std::vector<AtomicMemTable *> MinorSampling(int subrange_id);

        bool MajorReorg();

        bool DestroyDuplicates(int subrange_id, bool force);

        bool MinorRebalancePush(int index, bool *updated_prior);

        void MoveShareForDuplicateSubRange(int index);

        int PushTinyRanges(int subrangeId, bool stopWhenBelowFair,
                           bool *updated_prior);

        bool CreateDuplicates(int subrange_id);

        FlushOrder *flush_order_ = nullptr;
        port::Mutex range_lock_;
        StoCWritableFileClient *manifest_file_ = nullptr;
        std::string dbname_;
        VersionSet *versions_ = nullptr;
        const Options options_;
        const InternalKeyComparator *internal_comparator_ = nullptr;
        const Comparator *user_comparator_ = nullptr;
        std::atomic_int_fast32_t *memtable_id_seq_;
        std::vector<MemTablePartition *> *partitioned_active_memtables_ = nullptr;
        std::vector<uint32_t> *partitioned_imms_ = nullptr;
    };
}


#endif //LEVELDB_SUBRANGE_MANAGER_H
