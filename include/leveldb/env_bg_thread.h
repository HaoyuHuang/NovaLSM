
//
// Created by Haoyu Huang on 2/28/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_ENV_BG_THREAD_H
#define LEVELDB_ENV_BG_THREAD_H

#include "leveldb/export.h"
#include "leveldb/status.h"
#include "leveldb/cc_client.h"
#include "leveldb/db_types.h"

namespace leveldb {
    struct CompactionTask {
        void *db;
    };

    class LEVELDB_EXPORT EnvBGThread {
    public:
        // Arrange to run "(*function)(arg)" once in a background thread.
        //
        // "function" may run in an unspecified thread.  Multiple functions
        // added to the same Env may run concurrently in different threads.
        // I.e., the caller may not assume that background work items are
        // serialized.
        virtual void Schedule(const CompactionTask &task) = 0;

        virtual CCClient *dc_client() = 0;

        virtual MemManager *mem_manager() = 0;

        virtual uint64_t thread_id() = 0;

        virtual uint32_t num_running_tasks() = 0;

        virtual bool IsInitialized() = 0;
    };
}

#endif //LEVELDB_ENV_BG_THREAD_H
