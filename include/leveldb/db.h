// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_DB_H_
#define STORAGE_LEVELDB_INCLUDE_DB_H_

#include <atomic>
#include <stdint.h>
#include <stdio.h>
#include <fmt/core.h>

#include "leveldb/export.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "rdma/rdma_msg_callback.h"

namespace leveldb {

// Update CMakeLists.txt if you change these
    static const int kMajorVersion = 1;
    static const int kMinorVersion = 22;

    struct Options;
    struct ReadOptions;
    struct WriteOptions;

    class WriteBatch;

// Abstract handle to particular state of a DB.
// A Snapshot is an immutable object and can therefore be safely
// accessed from multiple threads without any external synchronization.
    class LEVELDB_EXPORT Snapshot {
    protected:
        virtual ~Snapshot();
    };

    struct OverlappingStats {
        uint32_t num_overlapping_tables = 0;
        uint64_t total_size = 0;
        uint64_t smallest;
        uint64_t largest;

        std::string DebugStringNum() {
            return fmt::format("{}", num_overlapping_tables);
        }

        std::string DebugString() {
            return fmt::format("[{},{}]:{}", smallest, largest,
                               num_overlapping_tables);
        }
    };

    struct LoadImbalanceMetric {
        double maximum_load_imbalance = 0.0;
        double stdev = 0.0;
    };

    struct DBStats {
        uint32_t num_l0_sstables = 0;
        uint64_t dbsize = 0;
        LoadImbalanceMetric load_imbalance = {};
        uint32_t num_major_reorgs = 0.0;
        uint32_t num_minor_reorgs = 0.0;
        uint32_t num_skipped_major_reorgs = 0.0;
        uint32_t num_skipped_minor_reorgs = 0.0;
        uint32_t num_minor_reorgs_for_dup = 0.0;
        uint32_t num_minor_reorgs_samples = 0.0;
        bool needs_compaction = false;

        uint32_t new_l0_sstables_since_last_query = 0.0;

        std::vector<OverlappingStats> num_overlapping_sstables_per_table;
        std::vector<OverlappingStats> num_overlapping_sstables;
        std::vector<OverlappingStats> num_overlapping_sstables_per_table_since_last_query;
        std::vector<OverlappingStats> num_overlapping_sstables_since_last_query;
        uint32_t *sstable_size_dist = nullptr;
    };

    struct ReconstructReplicasStats {
        uint32_t total_num_failed_metafiles = 0;
        uint32_t total_num_failed_datafiles = 0;
        uint32_t total_failed_metafiles_bytes = 0;
        uint32_t total_failed_datafiles_bytes = 0;

        uint32_t recover_num_metafiles = 0;
        uint32_t recover_num_datafiles = 0;
        uint64_t recover_metafiles_bytes = 0;
        uint64_t recover_datafiles_bytes = 0;

        std::string DebugString() const;
    };

// A DB is a persistent ordered map from keys to values.
// A DB is safe for concurrent access from multiple threads without
// any external synchronization.
    class LEVELDB_EXPORT DB {
    public:
        // Open the database with the specified "name".
        // Stores a pointer to a heap-allocated database in *dbptr and returns
        // OK on success.
        // Stores nullptr in *dbptr and returns a non-OK status on error.
        // Caller should delete *dbptr when it is no longer needed.
        static Status Open(const Options &options, const std::string &name,
                           DB **dbptr);

        virtual const std::string &dbname() = 0;

        virtual void QueryFailedReplicas(uint32_t failed_stoc_id,
                                         bool is_stoc_failed,
                                         std::unordered_map<uint32_t, std::vector<ReplicationPair>> *stoc_repl_pairs,
                                         int level, ReconstructReplicasStats* stats) = 0;

        virtual void UpdateFileMetaReplicaLocations(
                const std::vector<leveldb::ReplicationPair> &results, uint32_t stoc_server_id, int level, StoCClient* client) = 0;

        virtual Status Recover() = 0;

        DB() = default;

        DB(const DB &) = delete;

        DB &operator=(const DB &) = delete;

        virtual ~DB();

        virtual void QueryDBStats(DBStats *db_stats) = 0;

        // Set the database entry for "key" to "value".  Returns OK on success,
        // and a non-OK status on error.
        // Note: consider setting options.sync = true.
        virtual Status Put(const WriteOptions &options, const Slice &key,
                           const Slice &value) = 0;

        virtual void EvictFileFromCache(uint64_t file_number) = 0;

        // Remove the database entry (if any) for "key".  Returns OK on
        // success, and a non-OK status on error.  It is not an error if "key"
        // did not exist in the database.
        // Note: consider setting options.sync = true.
        virtual Status
        Delete(const WriteOptions &options, const Slice &key) = 0;

        // Apply the specified updates to the database.
        // Returns OK on success, non-OK on failure.
        // Note: consider setting options.sync = true.
        virtual Status
        WriteMemTablePool(const WriteOptions &options, const Slice &key,
                          const Slice &value) = 0;

        // If the database contains an entry for "key" store the
        // corresponding value in *value and return OK.
        //
        // If there is no entry for "key" leave *value unchanged and return
        // a status for which Status::IsNotFound() returns true.
        //
        // May return some other Status on an error.
        virtual Status Get(const ReadOptions &options, const Slice &key,
                           std::string *value) = 0;

        virtual void StartTracing() = 0;

        virtual void TestCompact(EnvBGThread *bg_thread,
                                 const std::vector<EnvBGTask> &tasks) = 0;

        // Return a heap-allocated iterator over the contents of the database.
        // The result of NewIterator() is initially invalid (caller must
        // call one of the Seek methods on the iterator before using it).
        //
        // Caller should delete the iterator when it is no longer needed.
        // The returned iterator should be deleted before this db is deleted.
        virtual Iterator *NewIterator(const ReadOptions &options) = 0;

        virtual uint32_t FlushMemTables(bool flush_active_memtable) = 0;

        // Return a handle to the current DB state.  Iterators created with
        // this handle will all observe a stable snapshot of the current DB
        // state.  The caller must call ReleaseSnapshot(result) when the
        // snapshot is no longer needed.
        virtual const Snapshot *GetSnapshot() = 0;

        // Release a previously acquired snapshot.  The caller must not
        // use "snapshot" after this call.
        virtual void ReleaseSnapshot(const Snapshot *snapshot) = 0;

        // DB implementations can export properties about their state
        // via this method.  If "property" is a valid property understood by this
        // DB implementation, fills "*value" with its current value and returns
        // true.  Otherwise returns false.
        //
        //
        // Valid property names include:
        //
        //  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
        //     where <N> is an ASCII representation of a level number (e.g. "0").
        //  "leveldb.stats" - returns a multi-line string that describes statistics
        //     about the internal operation of the DB.
        //  "leveldb.sstables" - returns a multi-line string that describes all
        //     of the sstables that make up the db contents.
        //  "leveldb.approximate-memory-usage" - returns the approximate number of
        //     bytes of memory in use by the DB.
        virtual bool GetProperty(const Slice &property, std::string *value) = 0;

        // For each i in [0,n-1], store in "sizes[i]", the approximate
        // file system space used by keys in "[range[i].start .. range[i].limit)".
        //
        // Note that the returned sizes measure file system space usage, so
        // if the user data compresses by a factor of ten, the returned
        // sizes will be one-tenth the size of the corresponding user data size.
        //
        // The results may not include the sizes of recently written data.
        virtual void GetApproximateSizes(const Range *range, int n,
                                         uint64_t *sizes) = 0;

        // Compact the underlying storage for the key range [*begin,*end].
        // In particular, deleted and overwritten versions are discarded,
        // and the data is rearranged to reduce the cost of operations
        // needed to access the data.  This operation should typically only
        // be invoked by users who understand the underlying implementation.
        //
        // begin==nullptr is treated as a key before all keys in the database.
        // end==nullptr is treated as a key after all keys in the database.
        // Therefore the following call will compact the entire database:
        //    db->CompactRange(nullptr, nullptr);
//        virtual void CompactRange(const Slice *begin, const Slice *end) = 0;

        virtual void PerformCompaction(EnvBGThread *bg_thread,
                                       const std::vector<EnvBGTask> &tasks) = 0;

        virtual void CoordinateMajorCompaction() = 0;

        virtual void PerformSubRangeReorganization() = 0;

        virtual void StartCoordinatedCompaction() = 0;

//        std::vector<DB *> dbs_;
//        std::vector<nova::RDMAMsgCallback *> rdma_threads_;

        uint64_t number_of_memtable_hits_ = 0;
        uint64_t number_of_gets_ = 0;
        uint64_t number_of_files_to_search_for_get_ = 0;

        ScanStats scan_stats = {};

        uint64_t number_of_active_memtables_ = 0;
        std::atomic_int_fast64_t number_of_immutable_memtables_;
        uint64_t number_of_steals_ = 0;
        uint64_t number_of_wait_due_to_contention_ = 0;
        uint64_t processed_writes_ = 0;
        uint64_t number_of_puts_no_wait_ = 0;
        uint64_t number_of_puts_wait_ = 0;
    };

// Destroy the contents of the specified database.
// Be very careful using this method.
//
// Note: For backwards compatibility, if DestroyDB is unable to list the
// database files, Status::OK() will still be returned masking this failure.
    LEVELDB_EXPORT Status DestroyDB(const std::string &name,
                                    const Options &options);

// If a DB cannot be opened, you may attempt to call this method to
// resurrect as much of the contents of the database as possible.
// Some data may be lost, so be careful when calling this function
// on a database that contains important information.
    LEVELDB_EXPORT Status RepairDB(const std::string &dbname,
                                   const Options &options);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_DB_H_
