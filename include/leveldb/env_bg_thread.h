
//
// Created by Haoyu Huang on 2/28/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_ENV_BG_THREAD_H
#define LEVELDB_ENV_BG_THREAD_H

#include <atomic>

#include "leveldb/export.h"
#include "leveldb/status.h"
#include "leveldb/stoc_client.h"
#include "leveldb/db_types.h"

#define BUCKET_SIZE 18

namespace leveldb {

    struct EnvBGTask {
        void *db = nullptr;
        void *compaction_task = nullptr;
        bool delete_obsolete_files = false;

        // flushing memtable related attributes.
        void *memtable = nullptr;
        bool merge_memtables_without_flushing;
        uint32_t range_id = 0;
        uint32_t memtable_size_mb = 0;
        uint32_t memtable_partition_id = 0;
        uint32_t imm_slot = 0;
    };

    class LEVELDB_EXPORT EnvBGThread {
    public:
        // Arrange to run "(*function)(arg)" once in a background thread.
        //
        // "function" may run in an unspecified thread.  Multiple functions
        // added to the same Env may run concurrently in different threads.
        // I.e., the caller may not assume that background work items are
        // serialized.
        virtual bool Schedule(const EnvBGTask &task) = 0;

        virtual StoCClient *stoc_client() = 0;

        virtual MemManager *mem_manager() = 0;

        virtual uint64_t thread_id() = 0;

        virtual uint32_t num_running_tasks() = 0;

        virtual bool IsInitialized() = 0;

        virtual unsigned int* rand_seed() = 0;

        static std::atomic_int_fast32_t bg_flush_memtable_thread_id_seq;
        static std::atomic_int_fast32_t bg_compaction_thread_id_seq;
        std::atomic_int_fast32_t memtable_size[BUCKET_SIZE];
    };
}

#endif //LEVELDB_ENV_BG_THREAD_H
