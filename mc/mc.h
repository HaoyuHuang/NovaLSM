
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_MC_H
#define LEVELDB_MC_H

#include "port/port.h"
#include "port/thread_annotations.h"

#include "nova/nova_common.h"

#include "leveldb/status.h"
#include "leveldb/write_batch.h"
#include "db/table_cache.h"
#include "leveldb/table.h"
#include "table/format.h"

#include "log/nova_log.h"
#include "nova_mem_manager.h"
#include "worker.h"

namespace nova {
    class MemoryComponent {
    public:
        MemoryComponent(uint32_t mc_id, leveldb::Env *env,
                        NovaMemManager *mem_manager,
                        const leveldb::Options &options,
                        const leveldb::InternalKeyComparator internal_comparator);

        char *AllocateBuf(uint32_t size);

        void FreeBuf(char *buf, uint32_t size);

        struct GetResult {
            bool hit;
            IndexEntry index;
            DataEntry data;
            leveldb::BlockContents block;
        };

        // FG: Retrieve blocks identified by the block handle.
        leveldb::Status
        Get(GlobalBlockHandle *handles, GetResult *results, int size);

        // FG: Insert blocks for future lookups.
        leveldb::Status
        Insert(GlobalBlockHandle *handles, char **blocks, int size);

        // FG: Flush the sstable from CC to MC.
        leveldb::Status
        Flush(const GlobalSSTableHandle &handle,
              leveldb::SequenceNumber last_seq,
              char *sstable, uint32_t size);

        // FG: Called when the sstable is flushed to DC.
        leveldb::Status
        FlushedToDC(const GlobalSSTableHandle &handle);

        // FG: Append log record.
        leveldb::Status AppendLogRecord(char *log_buf, int size);

        // FG: Called when the log file is written to DC.
        void DeleteLogFile(const GlobalLogFileHandle &handle);

        // FG: Called when the sstables are compacted with DC and persisted at DC. 
        void Delete(GlobalSSTableHandle *handles, int size);


    private:
        class SealLogFileRequest : public Request {
        public:
            std::map<GlobalSSTableHandle, NovaList<LogRecord>> *log_records;
            std::vector<leveldb::Slice> *log_bufs;
        };

        class SealLogFileWorker : public Worker {
        public:
            SealLogFileWorker(MemoryComponent *mc) : mc_(mc) {}

            void DoWork(Request *request) override;

        private:
            MemoryComponent *mc_;
        };

        class TrimLogFilesWorker : public Worker {
        public:
            TrimLogFilesWorker(MemoryComponent *mc) : mc_(mc) {}

            void DoWork(Request *request) override;

        private:
            MemoryComponent *mc_;
        };

        class CompactSSTablesRequest : public Request {
        public:
            std::map<GlobalSSTableHandle, leveldb::Table *> *sstables;
            leveldb::SequenceNumber latest_seq;
        };

        class CompactSSTablesWorker : public Worker {
        public:
            CompactSSTablesWorker(MemoryComponent *mc) : mc_(mc) {}

            void DoWork(Request *request) override;

        private:
            MemoryComponent *mc_;
        };

        struct SSTableContext {
            leveldb::Table *table;
            leveldb::port::Mutex mutex;
        };

        // BG: When the current log file is full.
        // One BG thread responsible for sealing log file.
        void SealLogFile(
                std::map<GlobalSSTableHandle, NovaList<LogRecord>> *log_records,
                std::vector<leveldb::Slice> *);

        // BG: When a SSTable is flushed to DC.
        // One BG thread performing trim log files.
        void TrimLogFiles();

        // BG: Merge SSTables at L0. When the SSTable group is full.
        // Multiple BG threads compacting SSTables.
        void CompactSSTables(
                std::map<GlobalSSTableHandle, leveldb::Table *> *tables,
                leveldb::SequenceNumber last_seq);

        class MCTableCache : public leveldb::TableCache {
        public:
            MCTableCache(MemoryComponent *mc, const std::string &dbname,
                         const leveldb::Options &options,
                         int entries, leveldb::DBProfiler *db_profiler) : mc_(
                    mc), leveldb::TableCache(dbname, options, entries,
                                             db_profiler) {};

            leveldb::Status
            FindTable(const nova::GlobalSSTableHandle &th,
                      leveldb::Cache::Handle **);

        private:
            MemoryComponent *mc_;
        };

        leveldb::Options options_;
        leveldb::Env *env_;
        NovaMemManager *mem_manager_;
        uint32_t mc_id_;
        const leveldb::InternalKeyComparator internal_comparator_;

        leveldb::TableCache table_cache_;
        leveldb::port::Mutex sstable_mutex_;
        std::map<GlobalSSTableHandle, SSTableContext *> sstables_l0_ GUARDED_BY(
                sstable_mutex_);
        std::map<GlobalSSTableHandle, leveldb::Table *> compacted_sstables_l0_ GUARDED_BY(
                sstable_mutex_);
        std::atomic<std::map<GlobalSSTableHandle, leveldb::Table *> *> current_sstable_group_ GUARDED_BY(
                sstable_mutex_);
        std::set<GlobalSSTableHandle> tables_flushed_to_dc_ GUARDED_BY(
                sstable_mutex_);
        std::set<GlobalSSTableHandle> gced_tables_log_records_ GUARDED_BY(
                sstable_mutex_);


        leveldb::port::Mutex log_mutex_;
        std::map<GlobalLogFileHandle, LogFile *> log_files_  GUARDED_BY(
                log_mutex_);

        uint32_t log_id_ = 0;
        uint32_t log_file_size_ = 0;
        std::atomic<std::vector<leveldb::Slice> *> current_log_bufs_;
        std::atomic<std::map<GlobalSSTableHandle, NovaList<LogRecord>> *> current_log_records_;

        SealLogFileWorker *seal_log_file_worker_;
        TrimLogFilesWorker *trim_log_file_worker_;
        Worker *compact_sstables_worker_;

        std::vector<std::thread> threads_;
    };
}
#endif //LEVELDB_MC_H
