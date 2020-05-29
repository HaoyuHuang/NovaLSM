
//
// Created by Haoyu Huang on 5/9/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_STORAGE_WORKER_H
#define LEVELDB_NOVA_STORAGE_WORKER_H

#include <semaphore.h>

#include "leveldb/db_types.h"
#include "db/table_cache.h"

#include "nova/nova_rdma_rc_store.h"
#include "mc/nova_mem_manager.h"
#include "log/nova_in_memory_log_manager.h"
#include "nova_rtable.h"
#include "nova_cc_server.h"

namespace nova {
    class NovaCCServer;
    class NovaStorageTask;
    class NovaServerCompleteTask;

    class NovaStorageWorker : public leveldb::EnvBGThread {
    public:
        NovaStorageWorker(leveldb::NovaRTableManager *rtable_manager,
                          std::vector<NovaCCServer *> &cc_servers,
                          const leveldb::Comparator *user_comparator,
                          const leveldb::Options &options,
                          leveldb::CCClient *client,
                          leveldb::MemManager *mem_manager,
                          uint64_t thread_id,
                          leveldb::Env *env);

        void AddTask(const NovaStorageTask &task);

        void Start();

        bool Schedule(const leveldb::EnvBGTask &task) override {
            RDMA_ASSERT(false);
        }

        leveldb::CCClient *dc_client() override {
            return client_;
        };

        leveldb::MemManager *mem_manager() override {
            return mem_manager_;
        }

        uint64_t thread_id() override {
            return thread_id_;
        }

        uint32_t num_running_tasks() override {
            return 0;
        };

        bool IsInitialized() override {
            return true;
        };

        unsigned int *rand_seed() override {
            return &rand_seed_;
        };

        static std::atomic_int_fast32_t storage_file_number_seq;

        uint32_t stat_tasks_ = 0;
        uint64_t stat_read_bytes_ = 0;
        uint64_t stat_write_bytes_ = 0;
    private:
        leveldb::NovaRTableManager *rtable_manager_;
        std::vector<NovaCCServer *> cc_servers_;

        bool is_running_ = true;
        leveldb::Env *env_;
        const leveldb::Comparator *user_comparator_;
        const leveldb::Options options_;
        leveldb::InternalKeyComparator icmp_;

        leveldb::CCClient *client_;
        leveldb::MemManager *mem_manager_;
        uint64_t thread_id_;
        unsigned int rand_seed_ = 0;

        std::mutex mutex_;
        std::list<NovaStorageTask> queue_;
        sem_t sem_;
    };
}


#endif //LEVELDB_NOVA_STORAGE_WORKER_H
