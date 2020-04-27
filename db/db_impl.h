// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <deque>
#include <queue>
#include <set>
#include <string>
#include <leveldb/db_profiler.h>
#include <list>
#include <map>
#include <leveldb/cache.h>
#include <fmt/core.h>
#include <nova/nova_common.h>
#include <cc/nova_cc.h>

#include "db/dbformat.h"
#include "leveldb/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "memtable.h"

#define MAX_BUCKETS 10000000
#define SUBRANGE_WARMUP_NPUTS 1000000
#define SUBRANGE_MAJOR_REORG_INTERVAL 1000000
#define SUBRANGE_MINOR_REORG_INTERVAL 100000
#define SUBRANGE_REORG_INTERVAL 100000

namespace leveldb {

    class MemTable;

    class TableCache;

    class Version;

    class VersionEdit;

    class VersionSet;

    struct TableLocation {
        std::atomic_int_fast64_t memtable_id;
    };

    class TableLocator {
    public:
        uint64_t Lookup(const Slice &key, uint64_t hash);

        void Insert(const Slice &key, uint64_t hash, uint32_t memtableid);

    private:
        TableLocation table_locator_[MAX_BUCKETS];
    };

    class DBImpl : public DB {
    public:
        DBImpl(const Options &options, const std::string &dbname);

        DBImpl(const DBImpl &) = delete;

        DBImpl &operator=(const DBImpl &) = delete;

        ~DBImpl() override;

        // Implementations of the DB interface
        Status Put(const WriteOptions &, const Slice &key,
                   const Slice &value) override;

        Status
        GenerateLogRecords(const WriteOptions &options,
                           WriteBatch *updates) override;

        void EvictFileFromCache(uint64_t file_number) override;

        Status Delete(const WriteOptions &, const Slice &key) override;

        Status Write(const WriteOptions &options, const Slice &key,
                     const Slice &value) override;

        Status WriteStaticPartition(const WriteOptions &options,
                                    const Slice &key,
                                    const Slice &val);

        Status WriteSubrange(const WriteOptions &options,
                             const Slice &key,
                             const Slice &val);

        Status Get(const ReadOptions &options, const Slice &key,
                   std::string *value) override;

        void TestCompact(EnvBGThread *bg_thread,
                         const std::vector<EnvBGTask> &tasks) override;

        Iterator *NewIterator(const ReadOptions &) override;

        const Snapshot *GetSnapshot() override;

        void StartTracing() override;

        void ReleaseSnapshot(const Snapshot *snapshot) override;

        bool GetProperty(const Slice &property, std::string *value) override;

        void GetApproximateSizes(const Range *range, int n,
                                 uint64_t *sizes) override;

        void CompactRange(const Slice *begin, const Slice *end) override;

        // Extra methods (for testing) that are not in the public DB interface

        // Compact any files in the named level that overlap [*begin,*end]
        void TEST_CompactRange(int level, const Slice *begin, const Slice *end);

        // Force current memtable contents to be compacted.
        Status TEST_CompactMemTable();

        // Return an internal iterator over the current state of the database.
        // The keys of this iterator are internal keys (see format.h).
        // The returned iterator should be deleted when no longer needed.
        Iterator *TEST_NewInternalIterator();

        // Return the maximum overlapping data (in bytes) at next level for any
        // file at a level >= 1.
        int64_t TEST_MaxNextLevelOverlappingBytes();

        // Record a sample of bytes read at the specified internal key.
        // Samples are taken approximately once every config::kReadBytesPeriod
        // bytes.
        void RecordReadSample(Slice key);

        void PerformCompaction(EnvBGThread *bg_thread,
                               const std::vector<EnvBGTask> &tasks) override;

        void PerformSubRangeReorganization() override;

        void QueryDBStats(DBStats *db_stats) override;

        Status Recover() override;

        Status
        RecoverLogFile(const std::map<std::string, uint64_t> &logfile_buf,
                       uint32_t *recovered_log_records,
                       timeval *rdma_read_complete);

    private:
        class NovaCCRecoveryThread {
        public:
            NovaCCRecoveryThread(
                    uint32_t client_id,
                    std::vector<leveldb::MemTable *> memtables,
                    MemManager *mem_manager);

            void Recover();

            sem_t sem_;
            uint32_t recovered_log_records = 0;
            uint64_t recovery_time = 0;
            uint64_t new_memtable_time = 0;
            uint64_t max_sequence_number = 0;

            std::vector<char *> log_replicas_;


        private:
            std::vector<leveldb::MemTable *> memtables_;
            uint32_t client_id_ = 0;
            MemManager *mem_manager_;
        };

        // Compact the in-memory write buffer to disk.  Switches to a new
        // log-file/memtable and writes a new descriptor iff successful.
        // Errors are recorded in bg_error_.
        bool CompactMemTableStaticPartition(EnvBGThread *bg_thread,
                                            const std::vector<EnvBGTask> &tasks) EXCLUSIVE_LOCKS_REQUIRED(
                mutex_);

        friend class DB;

        struct CompactionState;
        struct Writer;

        DBProfiler *db_profiler_ = nullptr;

        void StealMemTable(const WriteOptions &options);

        // Information for a manual compaction
        struct ManualCompaction {
            int level;
            bool done;
            const InternalKey *begin;  // null means beginning of key range
            const InternalKey *end;    // null means end of key range
            InternalKey tmp_storage;   // Used to keep track of compaction progress
        };

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

        Iterator *NewInternalIterator(const ReadOptions &,
                                      SequenceNumber *latest_snapshot,
                                      uint32_t *seed);

        Status NewDB();

        // Recover the descriptor from persistent storage.  May do a significant
        // amount of work to recover recently logged updates.  Any changes to
        // be made to the descriptor are added to *edit.
        Status Recover(VersionEdit *edit, bool *save_manifest)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        void MaybeIgnoreError(Status *s) const;

        // Delete any unneeded files and stale in-memory entries.
        void
        DeleteObsoleteFiles(EnvBGThread *bg_thread) EXCLUSIVE_LOCKS_REQUIRED(
                mutex_);

        void
        DeleteObsoleteVersions(EnvBGThread *bg_thread) EXCLUSIVE_LOCKS_REQUIRED(
                mutex_);

        // Compact the in-memory write buffer to disk.  Switches to a new
        // log-file/memtable and writes a new descriptor iff successful.
        // Errors are recorded in bg_error_.
        bool CompactMemTable(EnvBGThread *bg_thread,
                             const std::vector<EnvBGTask> &tasks) EXCLUSIVE_LOCKS_REQUIRED(
                mutex_);


        void RecordBackgroundError(const Status &s);

        void MaybeScheduleCompaction(
                uint32_t thread_id, MemTable *imm, uint32_t partition_id,
                uint32_t imm_slot,
                unsigned int *rand_seed) EXCLUSIVE_LOCKS_REQUIRED(
                mutex_);

        bool
        PerformMajorCompaction(EnvBGThread *bg_thread,
                               const std::vector<EnvBGTask> &tasks) EXCLUSIVE_LOCKS_REQUIRED(
                mutex_);

        void CleanupCompaction(CompactionState *compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        Status
        DoCompactionWork(CompactionState *compact, EnvBGThread *bg_thread)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        Status OpenCompactionOutputFile(CompactionState *compact,
                                        EnvBGThread *bg_thread);

        Status
        FinishCompactionOutputFile(CompactionState *compact, Iterator *input);

        Status
        InstallCompactionResults(CompactionState *compact, uint32_t thread_id)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        const Comparator *user_comparator() const {
            return internal_comparator_.user_comparator();
        }

        // Constant after construction
        Env *const env_;
        uint32_t server_id_;
        uint32_t dbid_;
        const Comparator *user_comparator_;
        const InternalKeyComparator internal_comparator_;
        const InternalFilterPolicy internal_filter_policy_;
        const Options options_;  // options_.comparator == &internal_comparator_
        const bool owns_info_log_;
        const bool owns_cache_;
        const std::string dbname_;

        // table_cache_ provides its own synchronization
        TableCache *const table_cache_;

        // Lock over the persistent DB state.  Non-null iff successfully acquired.
        FileLock *db_lock_;

        // Range lock.
        port::Mutex range_lock_;
        port::CondVar memtable_available_signal_;

        int number_of_available_pinned_memtables_ = 2;
        const int min_memtables_ = 2;

        // State below is protected by mutex_
        port::Mutex mutex_;
        std::atomic<bool> shutting_down_;

        std::vector<EnvBGThread *> compaction_threads_;
        EnvBGThread *reorg_thread_;

        std::atomic_int_fast32_t memtable_id_seq_;

        // key -> memtable-id.
        TableLocator *table_locator_ = nullptr;

        // memtable pool.
        std::vector<AtomicMemTable *> active_memtables_;

        uint64_t last_major_reorg_seq_ = 0;
        uint64_t last_minor_reorg_seq_ = 0;

        uint32_t num_major_reorgs = 0;
        uint32_t num_skipped_major_reorgs = 0;
        uint32_t num_minor_reorgs = 0;
        uint32_t num_skipped_minor_reorgs = 0;

        struct SubRange {
            Slice lower;
            Slice upper;
            bool lower_inclusive = true;
            bool upper_inclusive = false;
            uint64_t ninserts = 0;
            double rate = 0;

            static Slice Copy(Slice from) {
                char *c = new char[from.size()];
                for (int i = 0; i < from.size(); i++) {
                    c[i] = from[i];
                }
                return Slice(c, from.size());
            }

            std::string DebugString() {
                std::string output;
                uint64_t low;
                uint64_t up;
                nova::str_to_int(lower.data(), &low, lower.size());
                nova::str_to_int(upper.data(), &up, upper.size());

                if (lower_inclusive) {
                    output += "[";
                } else {
                    low++;
                    output += "(";
                }
                output += lower.ToString();
                output += ",";
                output += upper.ToString();
                if (upper_inclusive) {
                    up++;
                    output += "]";
                } else {
                    output += ")";
                }

                output += fmt::format(":{}, {}%, keys={}", ninserts,
                                      (uint32_t) rate * 100.0, up - low);
                return output;
            }

            bool
            IsSmallerThanLower(const Slice &key, const Comparator *comparator) {
                int comp = comparator->Compare(key, lower);
                if (comp < 0) {
                    return true;
                }
                if (comp == 0 && !lower_inclusive) {
                    return true;
                }
                return false;
            }

            bool
            IsGreaterThanUpper(const Slice &key, const Comparator *comparator) {
                int comp = comparator->Compare(key, upper);
                if (comp > 0) {
                    return true;
                }
                if (comp == 0 && !upper_inclusive) {
                    return true;
                }
                return false;
            }

            bool IsAPoint(const Comparator *comparator) {
                if (lower_inclusive && upper_inclusive &&
                    comparator->Compare(lower, upper) == 0) {
                    return true;
                }
                return false;
            }
        };

        class SubRanges {
        public:
            ~SubRanges() {
                for (int i = 0; i < subranges.size(); i++) {
                    delete subranges[i].lower.data();
                    delete subranges[i].upper.data();
                }
            }

            SubRanges() {}

            SubRanges(const SubRanges &other) {
                subranges.resize(other.subranges.size());
                for (int i = 0; i < subranges.size(); i++) {
                    subranges[i].lower = SubRange::Copy(
                            other.subranges[i].lower);
                    subranges[i].upper = SubRange::Copy(
                            other.subranges[i].upper);
                    subranges[i].lower_inclusive = other.subranges[i].lower_inclusive;
                    subranges[i].upper_inclusive = other.subranges[i].upper_inclusive;
                    subranges[i].ninserts = other.subranges[i].ninserts;
                }
            }

            std::vector<SubRange> subranges;

            std::string DebugString() {
                std::string output;
//                output += std::to_string(subranges.size());
                output += "\n";
                for (int i = 0; i < subranges.size(); i++) {
                    output += std::to_string(i) + " ";
                    output += subranges[i].DebugString();
                    output += "\n";
                }
                return output;
            }

            void AssertSubrangeBoundary(const Comparator *comparator) {
                if (subranges.empty()) {
                    return;
                }
                Slice prior_upper;
                bool upper_inclusive;
                for (int i = 0; i < subranges.size(); i++) {
                    SubRange &sr = subranges[i];
                    RDMA_ASSERT(!sr.IsSmallerThanLower(sr.upper, comparator))
                        << DebugString();
                    if (!prior_upper.empty()) {
                        if (upper_inclusive) {
                            if (sr.lower_inclusive) {
                                RDMA_ASSERT(
                                        sr.IsSmallerThanLower(prior_upper,
                                                              comparator))
                                    << DebugString();
                            } else {
                                // Must be the same.
                                RDMA_ASSERT(comparator->Compare(prior_upper,
                                                                sr.lower) == 0)
                                    << DebugString();
                            }
                        } else {
                            RDMA_ASSERT(sr.lower_inclusive) << DebugString();
                            RDMA_ASSERT(comparator->Compare(prior_upper,
                                                            sr.lower) == 0)
                                << DebugString();
                        }
                    }
                    prior_upper = sr.upper;
                    upper_inclusive = sr.upper_inclusive;
                }
            }
        };

        int BinarySearch(SubRanges *ref, const leveldb::Slice &key);

        std::atomic<SubRanges *> latest_subranges_;

        // static partition.
        struct MemTablePartition {
            MemTablePartition() : background_work_finished_signal_(&mutex) {
            };
            MemTable *memtable;
            port::Mutex mutex;
            uint32_t partition_id = 0;
            std::vector<uint32_t> imm_slots;
            std::queue<uint32_t> available_slots;
            std::vector<uint32_t> closed_log_files;
            port::CondVar background_work_finished_signal_ GUARDED_BY(mutex);
        };

        std::vector<MemTablePartition *> partitioned_active_memtables_ GUARDED_BY(
                mutex_);
        std::vector<uint32_t> partitioned_imms_ GUARDED_BY(
                mutex_);  // Memtable being compacted

        uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

        SnapshotList snapshots_ GUARDED_BY(mutex_);

        // Set of table files to protect from deletion because they are
        // part of ongoing compactions.
        std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);
        std::map<uint64_t, FileMetaData> compacted_tables_ GUARDED_BY(mutex_);
        bool is_major_compaciton_running_ = false;
        ManualCompaction *manual_compaction_ GUARDED_BY(mutex_);

        VersionSet *const versions_ GUARDED_BY(mutex_);

        // Have we encountered a background error in paranoid mode?
        Status bg_error_ GUARDED_BY(mutex_);

        CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);
        std::string current_log_file_name_ GUARDED_BY(mutex_);
        std::vector<uint32_t> closed_memtable_log_files_  GUARDED_BY(
                range_lock_);

        bool WriteStaticPartition(const leveldb::WriteOptions &options,
                                  const leveldb::Slice &key,
                                  const leveldb::Slice &value,
                                  uint32_t partition_id,
                                  bool should_wait, uint64_t last_sequence,
                                  SubRange *subrange);

        void PerformSubrangeMajorReorg(SubRanges *latest,
                                       double total_inserts);

        void PerformSubrangeMinorReorg(int subrange_id, SubRanges *latest,
                                       double total_inserts);

        NovaCCMemFile *manifest_file_ = nullptr;
        unsigned int rand_seed_ = 0;
    };

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
    Options SanitizeOptions(const std::string &db,
                            const InternalKeyComparator *icmp,
                            const InternalFilterPolicy *ipolicy,
                            const Options &src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_