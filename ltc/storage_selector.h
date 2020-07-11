
//
// Created by Haoyu Huang on 5/13/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
// Select StoCs to store data based on scatter policy.

#ifndef LEVELDB_STORAGE_SELECTOR_H
#define LEVELDB_STORAGE_SELECTOR_H

#include "util/env_mem.h"
#include "stoc_client_impl.h"
#include "leveldb/env.h"
#include "leveldb/table.h"

namespace leveldb {
    class StorageSelector {
    public:
        StorageSelector(unsigned int *rand_seed);

        void SelectStorageServers(StoCBlockClient *client,
                                  nova::ScatterPolicy scatter_policy,
                                  int num_storage_to_select,
                                  std::vector<uint32_t> *selected_storage);

        uint32_t SelectAvailableStoCForFailedMetaBlock(
                const std::vector<FileReplicaMetaData> &block_replica_handles,
                uint32_t failed_replica_id, uint32_t *available_replica_id);

        uint32_t SelectAvailableStoCForFailedDataBlock(
                const std::vector<FileReplicaMetaData> &block_replica_handles,
                uint32_t failed_replica_id, uint32_t failed_frag_id,
                uint32_t *available_replica_id);

        void SelectAvailableStoCs(std::vector<uint32_t> *selected_storages,
                                  uint32_t nstocs);

        void ValidateReplicas(
                const std::vector<leveldb::FileReplicaMetaData> &replicas);

        std::string ReplicaDebugString(
                const std::vector<leveldb::FileReplicaMetaData> &replicas);

        static std::atomic<nova::Servers *> available_stoc_servers;
    private:
        unsigned int *rand_seed_;
    };
}


#endif //LEVELDB_STORAGE_SELECTOR_H
