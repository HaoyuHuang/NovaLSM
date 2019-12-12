
//
// Created by Haoyu Huang on 12/2/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_DB_PROFILER_H
#define LEVELDB_DB_PROFILER_H

#include "leveldb/export.h"
#include "port/port.h"
#include <fstream>
#include <vector>

namespace leveldb {

    enum TraceType {
        MEMTABLE = 0,
        IMMUTABLE_MEMTABLE = 1,
        INDEX_BLOCK = 2,
        FILTER_BLOCK = 3,
        DATA_BLOCK = 4,
    };

    enum AccessCaller {
        kUserGet = 1,
        kUserIterator = 2,
        kCompaction = 3,
        kUncategorized = 4,
        kApproximateSize = 5,
    };

    struct BlockReadContext {
        AccessCaller caller;
        uint64_t file_number;
        int level;
    };

    struct Access {
        enum TraceType trace_type;
        enum AccessCaller access_caller;
        // block id == 0 for memtable.
        uint64_t block_id;
        // block id == 0 for memtable.
        uint64_t sstable_id;
        // -2 memtable, -1 immutable table.
        int level;
        // block size.
        uint64_t size;
    };

    struct CompactionProfilerStats {
        uint64_t num_files = 0;
        uint64_t num_bytes_read = 0;
        uint64_t num_bytes_written = 0;
    };

    struct CompactionProfiler {
        int level = 0;
        int output_level = 0;
        CompactionProfilerStats level_stats;
        CompactionProfilerStats next_level_stats;
        CompactionProfilerStats next_level_output_stats;
    };


    class LEVELDB_EXPORT DBProfiler {
    public:
        DBProfiler(bool enabled, std::string trace_file_path);

        void StartTracing();

        void Trace(Access access);

        void Trace(CompactionProfiler compaction);

        void Close();

    private:
        void WriteToFile();

        uint64_t WRITE_BATCH_SIZE = 100;
        const bool enabled_;
        bool tracing_;
        port::Mutex mutex_;
        std::string trace_file_path_;
        std::ofstream access_trace_file_writer_ GUARDED_BY(mutex_);
        std::ofstream compaction_trace_file_writer_  GUARDED_BY(mutex_);

        Access *accesses_ GUARDED_BY(mutex_);
        uint32_t index_ GUARDED_BY(mutex_) = 0;
    };
}


#endif //LEVELDB_DB_PROFILER_H
