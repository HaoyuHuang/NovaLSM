
//
// Created by Haoyu Huang on 5/9/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_STORAGE_WORKER_H
#define LEVELDB_STORAGE_WORKER_H

#include <semaphore.h>

#include "leveldb/db_types.h"
#include "db/table_cache.h"

#include "rdma/nova_rdma_rc_broker.h"
#include "common/nova_mem_manager.h"
#include "log/stoc_log_manager.h"
#include "stoc/persistent_stoc_file.h"
#include "novalsm/rdma_server.h"

namespace nova {
    class RDMAServerImpl;

    class StorageTask;

    class ServerCompleteTask;

    // A storage worker that handles storage related requests.
    class StorageWorker : public leveldb::EnvBGThread {
    public:
        StorageWorker(leveldb::StocPersistentFileManager *stoc_file_manager,
                      std::vector<RDMAServerImpl *> &rdma_servers,
                      const leveldb::Comparator *user_comparator,
                      const leveldb::Options &options,
                      leveldb::StoCClient *client,
                      leveldb::MemManager *mem_manager,
                      uint64_t thread_id,
                      leveldb::Env *env);

        void AddTask(const StorageTask &task);

        void Start();

        bool Schedule(const leveldb::EnvBGTask &task) override {
            NOVA_ASSERT(false);
        }

        leveldb::StoCClient *stoc_client() override {
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

        std::vector<leveldb::ReplicationPair> ReplicateSSTables(
                const std::string &dbname,
                const std::vector<leveldb::ReplicationPair> &replication_pairs);

        static std::atomic_int_fast32_t storage_file_number_seq;

        uint32_t stat_tasks_ = 0;
        uint64_t stat_read_bytes_ = 0;
        uint64_t stat_write_bytes_ = 0;
    private:
        leveldb::StocPersistentFileManager *stoc_file_manager_;
        std::vector<RDMAServerImpl *> rdma_servers_;

        bool is_running_ = true;
        leveldb::Env *env_;
        const leveldb::Comparator *user_comparator_;
        const leveldb::Options options_;
        leveldb::InternalKeyComparator icmp_;

        leveldb::StoCClient *client_;
        leveldb::MemManager *mem_manager_;
        uint64_t thread_id_;
        unsigned int rand_seed_ = 0;

        std::mutex mutex_;
        std::list<StorageTask> queue_;
        sem_t sem_;
    };
}


#endif //LEVELDB_STORAGE_WORKER_H
