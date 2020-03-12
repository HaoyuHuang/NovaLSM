
//
// Created by Haoyu Huang on 12/2/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "leveldb/db_profiler.h"

namespace leveldb {
    DBProfiler::DBProfiler(bool enabled, std::string trace_file_path)
            : enabled_(enabled), trace_file_path_(trace_file_path),
              access_trace_file_writer_(
                      trace_file_path + "/access_profiler.log"),
              compaction_trace_file_writer_(
                      trace_file_path + "/compaction_profiler.log") {
//        accesses_ = new Access[WRITE_BATCH_SIZE];
        tracing_ = false;
    }

    void DBProfiler::Trace(Access access) {
//        if (!tracing_) {
//            return;
//        }
//
//        mutex_.Lock();
//        accesses_[index_] = access;
//        index_++;
//        if (index_ == WRITE_BATCH_SIZE) {
//            WriteToFile();
//        }
//        mutex_.Unlock();
    }

    void DBProfiler::Trace(CompactionProfiler compaction) {
//        if (!tracing_) {
//            return;
//        }
//
//        mutex_.Lock();
//        compaction_trace_file_writer_ << compaction.level << ","
//                                      << compaction.output_level << ","
//                                      << compaction.level_stats.num_files << ","
//                                      << compaction.level_stats.num_bytes_read
//                                      << ","
//                                      << compaction.next_level_stats.num_files
//                                      << ","
//                                      << compaction.next_level_stats.num_bytes_read
//                                      << ","
//                                      << compaction.next_level_output_stats.num_files
//                                      << ","
//                                      << compaction.next_level_output_stats.num_bytes_written
//                                      << std::endl;
//        compaction_trace_file_writer_.flush();
//        mutex_.Unlock();
    }

    void DBProfiler::StartTracing() {
        if (enabled_) {
            tracing_ = true;
        }
    }

    void DBProfiler::Close() {
        WriteToFile();
        access_trace_file_writer_.close();
        delete accesses_;
    }

    void DBProfiler::WriteToFile() {
        for (uint32_t i = 0; i < index_; i++) {
            const Access &access = accesses_[i];
            access_trace_file_writer_ << access.trace_type << ","
                                      << access.access_caller << ","
                                      << access.sstable_id
                                      << "," << access.block_id << ","
                                      << access.level << "," << access.size
                                      << std::endl;
        }
        access_trace_file_writer_.flush();
        index_ = 0;
    }
}