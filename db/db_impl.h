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

#include <list>
#include <map>
#include <fmt/core.h>
#include <semaphore.h>

#include "common/nova_common.h"
#include "leveldb/db_profiler.h"
#include "leveldb/cache.h"
#include "ltc/stoc_file_client_impl.h"

#include "db/dbformat.h"
#include "leveldb/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "memtable.h"
#include "leveldb/subrange.h"
#include "subrange_manager.h"
#include "compaction.h"
#include "lookup_index.h"
#include "range_index.h"

#include "log/log_recovery.h"

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

        void QueryFailedReplicas(uint32_t failed_stoc_id,
                                 bool is_stoc_failed,
                                 std::unordered_map<uint32_t, std::vector<ReplicationPair>> *stoc_repl_pairs,
                                 int level,
                                 ReconstructReplicasStats *stats) override;

        // Implementations of the DB interface
        Status Put(const WriteOptions &, const Slice &key,
                   const Slice &value) override;

        void EvictFileFromCache(uint64_t file_number) override;

        uint32_t FlushMemTables(bool flush_active_memtable) override;

        Status Delete(const WriteOptions &, const Slice &key) override;

        Status WriteMemTablePool(const WriteOptions &options, const Slice &key,
                                 const Slice &val) override;

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

        void PerformCompaction(EnvBGThread *bg_thread,
                               const std::vector<EnvBGTask> &tasks) override;

        void PerformSubRangeReorganization() override;

        void QueryDBStats(DBStats *db_stats) override;

        Status Recover() override;

        Status
        RecoverLogFile(
                const std::unordered_map<std::string, uint64_t> &logfile_buf,
                uint32_t *recovered_log_records,
                timeval *rdma_read_complete);

        void
        CoordinateMajorCompaction() override;

        void GenerateLogRecord(const WriteOptions &options,
                               SequenceNumber last_sequence,
                               const Slice &key, const Slice &val,
                               uint32_t memtable_id);

        void GenerateLogRecord(const WriteOptions &options,
                               const std::vector<LevelDBLogRecord> &log_records,
                               uint32_t memtable_id);

        void StartCoordinatedCompaction();

        void StopCoordinatedCompaction();

        void StopCompaction();

        uint32_t EncodeMemTablePartitions(char *buf);

        void
        DecodeMemTablePartitions(Slice *buf, std::unordered_map<uint32_t, leveldb::MemTableLogFilePair> *mid_table_map);

        const std::string &dbname() override;

        uint32_t EncodeDBMetadata(char *buf, nova::StoCInMemoryLogFileManager *log_manager, uint32_t cfg_id);

        void
        RecoverDBMetadata(const Slice &buf, uint32_t version_id, uint64_t last_sequence, uint64_t next_file_number,
                          uint64_t memtable_id_seq, nova::StoCInMemoryLogFileManager *log_manager,
                          std::unordered_map<uint32_t, leveldb::MemTableLogFilePair> *mid_table_map);

        void ScheduleFlushMemTableTask(
                int thread_id,
                uint32_t memtable_id,
                MemTable *imm,
                uint32_t partition_id, uint32_t imm_slot,
                unsigned int *rand_seed, bool merge_memtables_without_flushing);

        const Options options_;  // options_.comparator == &internal_comparator_
        nova::StoCInMemoryLogFileManager *log_manager_ = nullptr;
        std::vector<EnvBGThread *> bg_flush_memtable_threads_;

        void ScheduleFileDeletionTask();

        void UpdateFileMetaReplicaLocations(
                const std::vector<leveldb::ReplicationPair> &results, uint32_t stoc_server_id, int level, StoCClient* client) override ;

        std::atomic_bool is_loading_db_;

    private:
        void ObtainStoCFilesOfSSTable(std::vector<std::string> *files_to_delete,
                                      std::unordered_map<uint32_t, std::vector<SSTableStoCFilePair>> *server_pairs,
                                      const FileMetaData &meta) const;

        Status GetWithLookupIndex(const ReadOptions &options, const Slice &key,
                                  std::string *value);

        Status GetWithRangeIndex(const ReadOptions &options, const Slice &key,
                                 std::string *value);

        std::atomic_bool start_compaction_;
        std::atomic_bool start_coordinated_compaction_;
        std::atomic_bool terminate_coordinated_compaction_;

        void CleanupLSMCompaction(CompactionState *state,
                                  VersionEdit &edit,
                                  RangeIndexVersionEdit &range_edit,
                                  std::unordered_map<uint32_t, MemTableL0FilesEdit> &edits,
                                  CompactionRequest *compaction_req,
                                  uint32_t compacting_version_id);

        bool ComputeCompactions(Version *current,
                                std::vector<Compaction *> *compactions,
                                VersionEdit *edit,
                                RangeIndexVersionEdit *range_edit,
                                bool *delete_due_to_low_overlap,
                                std::unordered_map<uint32_t, leveldb::MemTableL0FilesEdit> *memtableid_l0fns);

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
        void CompactMemTableStaticPartition(EnvBGThread *bg_thread,
                                            const std::vector<EnvBGTask> &tasks,
                                            VersionEdit *edit,
                                            bool prune_memtable);

        bool CompactMultipleMemTablesStaticPartitionToMemTable(
                int partition_id,
                EnvBGThread *bg_thread,
                const std::vector<EnvBGTask> &tasks,
                std::vector<uint32_t> *closed_memtable_log_files);

        friend class DB;

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

        Iterator *NewInternalIterator(const ReadOptions &,
                                      SequenceNumber *latest_snapshot,
                                      uint32_t *seed);

        // Delete any unneeded files and stale in-memory entries.
        void
        ObtainObsoleteFiles(EnvBGThread *bg_thread,
                            std::vector<std::string> *files_to_delete,
                            std::unordered_map<uint32_t, std::vector<SSTableStoCFilePair>> *server_pairs,
                            uint32_t compacting_version_id);

        void DeleteFiles(EnvBGThread *bg_thread,
                         std::vector<std::string> &files_to_delete,
                         std::unordered_map<uint32_t, std::vector<SSTableStoCFilePair>> &server_pairs);

        void
        ObtainLookupIndexEdits(CompactionState *state,
                               std::unordered_map<uint32_t, MemTableL0FilesEdit> *memtableid_l0fns);

        void UpdateLookupIndex(uint32_t version_id,
                               const std::unordered_map<uint32_t, MemTableL0FilesEdit> &edits);

        void
        DeleteObsoleteVersions(EnvBGThread *bg_thread) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        // Compact the in-memory write buffer to disk.  Switches to a new
        // log-file/memtable and writes a new descriptor iff successful.
        // Errors are recorded in bg_error_.
        bool CompactMemTable(EnvBGThread *bg_thread,
                             const std::vector<EnvBGTask> &tasks) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        void RecordBackgroundError(const Status &s);

        void ScheduleCompactionTask(int thread_id, void *compaction);

        void ScheduleFileDeletionTask(int thread_id);

        Status
        InstallCompactionResults(CompactionState *compact, VersionEdit *edit, int target_level);

        const Comparator *user_comparator() const {
            return internal_comparator_.user_comparator();
        }

        FlushOrder *flush_order_;

        // Constant after construction
        Env *const env_;
        uint32_t server_id_;
        uint32_t dbid_;
        const Comparator *user_comparator_;
        const InternalKeyComparator internal_comparator_;
        const InternalFilterPolicy internal_filter_policy_;
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

        port::Mutex mutex_;
        std::atomic<bool> shutting_down_;
        port::Mutex l0_stop_write_mutex_;
        port::CondVar l0_stop_write_signal_;

        std::vector<EnvBGThread *> bg_compaction_threads_;
        EnvBGThread *reorg_thread_;
        EnvBGThread *compaction_coordinator_thread_;

        std::atomic_int_fast32_t memtable_id_seq_;

        SubRangeManager *subrange_manager_ = nullptr;

        // key -> memtable-id.
        LookupIndex *lookup_index_ = nullptr;
        RangeIndexManager *range_index_manager_ = nullptr;

        // memtable pool.
        std::vector<AtomicMemTable *> active_memtables_;
        // partitioned memtables.
        std::vector<MemTablePartition *> partitioned_active_memtables_ GUARDED_BY(mutex_);
        std::vector<uint32_t> partitioned_imms_ GUARDED_BY(mutex_);  // Memtable being compacted

        uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

        SnapshotList snapshots_ GUARDED_BY(mutex_);

        // Set of table files to protect from deletion because they are
        // part of ongoing compactions.
        std::unordered_map<uint64_t, FileMetaData> compacted_tables_ GUARDED_BY(mutex_);
        bool is_major_compaciton_running_ = false;
        ManualCompaction *manual_compaction_ GUARDED_BY(mutex_);

        VersionSet *const versions_ GUARDED_BY(mutex_);

        port::Mutex mutex_compacting_tables;
        std::set<uint64_t> compacting_tables_;

        // Have we encountered a background error in paranoid mode?
        Status bg_error_ GUARDED_BY(mutex_);

        std::string current_log_file_name_ GUARDED_BY(mutex_);
        std::vector<uint32_t> closed_memtable_log_files_  GUARDED_BY(range_lock_);

        bool WriteStaticPartition(const leveldb::WriteOptions &options,
                                  const leveldb::Slice &key,
                                  const leveldb::Slice &value,
                                  uint32_t partition_id,
                                  bool should_wait, uint64_t last_sequence,
                                  SubRange *subrange);

        StoCWritableFileClient *manifest_file_ = nullptr;
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