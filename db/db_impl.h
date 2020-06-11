// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <deque>
#include <set>
#include <string>
#include <leveldb/db_profiler.h>
#include <list>
#include <util/mutexlock.h>

#include "db/dbformat.h"
#include "leveldb/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

    class MemTable;

    class TableCache;

    class Version;

    class VersionEdit;

    class VersionSet;

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

        Status Delete(const WriteOptions &, const Slice &key) override;

        Status Write(const WriteOptions &options, WriteBatch *updates) override;

        Status Get(const ReadOptions &options, const Slice &key,
                   std::string *value) override;

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

        void SetL0StartCompactionBytes(uint64_t value) override;

        uint64_t L0CurrentBytes() override;

        void FlushMemTable(leveldb::NovaLogRecordMode log_record_mode) override;

        void ScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_) override;

        void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        Logger* infoLog() override {
            return options_.info_log;
        };
    private:
        friend class DB;

        struct CompactionState;
        struct Writer;

        DBProfiler *db_profiler_ = nullptr;
        uint64_t l0_start_compaction_bytes = 0;

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
        void DeleteObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        // Compact the in-memory write buffer to disk.  Switches to a new
        // log-file/memtable and writes a new descriptor iff successful.
        // Errors are recorded in bg_error_.
        void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        Status
        RecoverLogFile(uint64_t log_number, bool last_log, bool *save_manifest,
                       VersionEdit *edit, SequenceNumber *max_sequence)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        Status WriteLevel0Table(MemTable *mem, VersionEdit *edit, Version *base)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        Status MakeRoomForWrite(bool force /* compact even if there is room? */,
                                const WriteOptions &write_options)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        WriteBatch *BuildBatchGroup(Writer **last_writer)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        void RecordBackgroundError(const Status &s);

        static void BGWork(void *db);

        void BackgroundCall();

        void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        void CleanupCompaction(CompactionState *compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        Status DoCompactionWork(CompactionState *compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        Status OpenCompactionOutputFile(CompactionState *compact);

        Status
        FinishCompactionOutputFile(CompactionState *compact, Iterator *input);

        Status InstallCompactionResults(CompactionState *compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        const Comparator *user_comparator() const {
            return internal_comparator_.user_comparator();
        }

        void Schedule(void (*function)(void *arg), void *arg);

        // Constant after construction
        Env *const env_;
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

        // State below is protected by mutex_
        port::Mutex mutex_;
        std::atomic<bool> shutting_down_;
        port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
        int current_bg_thread_id_ = 0;
        std::vector<EnvBGThread *> bg_threads_;
        MemTable *mem_;
        MemTable *imm_ GUARDED_BY(mutex_);  // Memtable being compacted
        std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
        WritableFile *logfile_;
        uint64_t logfile_number_ GUARDED_BY(mutex_);
        log::Writer *log_;
        uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

        // Queue of writers.
        std::deque<Writer *> writers_ GUARDED_BY(mutex_);
        WriteBatch *tmp_batch_ GUARDED_BY(mutex_);

        SnapshotList snapshots_ GUARDED_BY(mutex_);

        // Set of table files to protect from deletion because they are
        // part of ongoing compactions.
        std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

        // Has a background compaction been scheduled or is running?
        bool background_compaction_scheduled_ GUARDED_BY(mutex_);

        ManualCompaction *manual_compaction_ GUARDED_BY(mutex_);

        VersionSet *const versions_ GUARDED_BY(mutex_);

        // Have we encountered a background error in paranoid mode?
        Status bg_error_ GUARDED_BY(mutex_);

        std::vector<CompactionStats> stats_ GUARDED_BY(mutex_);
        std::string current_log_file_name_ GUARDED_BY(mutex_);
        std::list<std::string> closed_log_files_  GUARDED_BY(mutex_);
    };

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
    Options SanitizeOptions(const std::string &db,
                            const InternalKeyComparator *icmp,
                            const InternalFilterPolicy *ipolicy,
                            const Options &src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_