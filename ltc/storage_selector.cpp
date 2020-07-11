
//
// Created by Haoyu Huang on 5/13/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "storage_selector.h"

namespace leveldb {
    namespace {
        struct StoCStatsStatus {
            uint32_t remote_stoc_id = 0;
            uint32_t req_id = 0;
            StoCResponse *response = nullptr;
        };

        bool stoc_stats_comparator(const StoCStatsStatus &s1,
                                   const StoCStatsStatus &s2) {
            return s1.response->stoc_queue_depth <
                   s2.response->stoc_queue_depth;
        }
    }

    StorageSelector::StorageSelector(unsigned int *rand_seed) : rand_seed_(
            rand_seed) {
    }

    void StorageSelector::ValidateReplicas(
            const std::vector<leveldb::FileReplicaMetaData> &replicas) {
        // Make sure all replicas are placed on a different StoC.
        {
            // Validate metadata blocks.
            std::set<uint32_t> used_replicas;
            for (int replica_id = 0;
                 replica_id < replicas.size(); replica_id++) {
                NOVA_ASSERT(used_replicas.find(
                        replicas[replica_id].meta_block_handle.server_id) ==
                            used_replicas.end())
                    << ReplicaDebugString(replicas);
                used_replicas.insert(
                        replicas[replica_id].meta_block_handle.server_id);
            }
        }

        // Validate data blocks.
        uint32_t ndata_fragments = replicas[0].data_block_group_handles.size();
        for (int replica_id = 1; replica_id < replicas.size(); replica_id++) {
            NOVA_ASSERT(ndata_fragments ==
                        replicas[replica_id].data_block_group_handles.size())
                << ReplicaDebugString(replicas);
        }
        for (int frag_id = 0; frag_id < ndata_fragments; frag_id++) {
            std::set<uint32_t> used_replicas;
            for (int replica_id = 0;
                 replica_id < replicas.size(); replica_id++) {
                NOVA_ASSERT(used_replicas.find(
                        replicas[replica_id].data_block_group_handles[frag_id].server_id) ==
                            used_replicas.end())
                    << ReplicaDebugString(replicas);
                used_replicas.insert(
                        replicas[replica_id].data_block_group_handles[frag_id].server_id);
            }
        }

        // With more than one replica, validate a SSTable replica is stored on one StoC.
        if (replicas.size() > 1) {
            for (int replica_id = 0;
                 replica_id < replicas.size(); replica_id++) {
                uint32_t server_id = replicas[replica_id].meta_block_handle.server_id;
                for (int frag_id = 0; frag_id < ndata_fragments; frag_id++) {
                    NOVA_ASSERT(server_id ==
                                replicas[replica_id].data_block_group_handles[frag_id].server_id)
                        << ReplicaDebugString(replicas);
                }
            }
        }
    }

    std::string StorageSelector::ReplicaDebugString(
            const std::vector<leveldb::FileReplicaMetaData> &replicas) {
        std::string debug = fmt::format("{} replicas\n", replicas.size());
        for (int i = 0; i < replicas.size(); i++) {
            debug += fmt::format("rep-{}: meta:", i);
            auto &replica = replicas[i];
            debug += replica.meta_block_handle.DebugString();
            for (int j = 0; j < replica.data_block_group_handles.size(); j++) {
                debug += fmt::format(" data-{}:{}", j,
                                     replica.data_block_group_handles[j].DebugString());
            }
            debug += "\n";
        }
        return debug;
    }

    uint32_t StorageSelector::SelectAvailableStoCForFailedMetaBlock(
            const std::vector<FileReplicaMetaData> &block_replica_handles,
            uint32_t failed_replica_id, uint32_t *available_replica_id) {
        int new_stoc_id = 0;
        *available_replica_id =
                (failed_replica_id + 1) % block_replica_handles.size();
        std::set<uint32_t> used_replicas;
        for (int i = 0; i < block_replica_handles.size(); i++) {
            used_replicas.insert(
                    block_replica_handles[i].meta_block_handle.server_id);
        }
        nova::Servers *available_stocs = available_stoc_servers;
        for (int i = 0; i < available_stocs->servers.size(); i++) {
            if (used_replicas.find(available_stocs->servers[i].server_id) ==
                used_replicas.end()) {
                new_stoc_id = available_stocs->servers[i].server_id;
                break;
            }
        }
        NOVA_ASSERT(new_stoc_id != -1);
        return new_stoc_id;
    }

    uint32_t StorageSelector::SelectAvailableStoCForFailedDataBlock(
            const std::vector<FileReplicaMetaData> &block_replica_handles,
            uint32_t failed_replica_id, uint32_t failed_frag_id,
            uint32_t *available_replica_id) {
        int new_stoc_id = 0;
        *available_replica_id =
                (failed_replica_id + 1) % block_replica_handles.size();
        std::set<uint32_t> used_replicas;
        for (int i = 0; i < block_replica_handles.size(); i++) {
            used_replicas.insert(
                    block_replica_handles[i].data_block_group_handles[failed_frag_id].server_id);
        }
        nova::Servers *available_stocs = available_stoc_servers;
        for (int i = 0; i < available_stocs->servers.size(); i++) {
            if (used_replicas.find(available_stocs->servers[i].server_id) ==
                used_replicas.end()) {
                new_stoc_id = available_stocs->servers[i].server_id;
                break;
            }
        }
        NOVA_ASSERT(new_stoc_id != -1);
        return new_stoc_id;
    }

    void StorageSelector::SelectAvailableStoCs(
            std::vector<uint32_t> *selected_storages, uint32_t nstocs) {
        nova::Servers *available_stocs = available_stoc_servers;
        int startid = rand_r(rand_seed_) % available_stocs->servers.size();
        for (int i = 0; i < nstocs; i++) {
            int id = (startid + i) % available_stocs->servers.size();
            uint32_t sid = available_stocs->servers[id].server_id;
            selected_storages->push_back(sid);
        }
        NOVA_ASSERT(selected_storages->size() == nstocs);
    }

    void
    StorageSelector::SelectStorageServers(StoCBlockClient *client,
                                          nova::ScatterPolicy scatter_policy,
                                          int num_storage_to_select,
                                          std::vector<uint32_t> *selected_storage) {
        NOVA_ASSERT(client);
        selected_storage->clear();
        selected_storage->resize(num_storage_to_select);
        nova::Servers *available_stocs = available_stoc_servers;

        if (num_storage_to_select == available_stocs->servers.size()) {
            for (int i = 0; i < num_storage_to_select; i++) {
                (*selected_storage)[i] = available_stocs->servers[i].server_id;
            }
            return;
        }

        std::vector<uint32_t> candidate_storage_ids;
        if (scatter_policy == nova::ScatterPolicy::POWER_OF_TWO) {
            uint32_t start_storage_id = rand_r(rand_seed_) %
                                        available_stocs->servers.size();
            uint32_t candidates = 2 * num_storage_to_select;
            if (candidates > available_stocs->servers.size()) {
                candidates = available_stocs->servers.size();
            }
            for (int i = 0; i < candidates; i++) {
                candidate_storage_ids.push_back(start_storage_id);
                start_storage_id = (start_storage_id + 1) %
                                   available_stocs->servers.size();
            }
        } else {
            // Random.
            // Select the start storage id then round robin.
            uint32_t start_storage_id =
                    rand_r(rand_seed_) % available_stocs->servers.size();
            for (int i = 0; i < num_storage_to_select; i++) {
                (*selected_storage)[i] = available_stocs->servers[start_storage_id].server_id;
                start_storage_id = (start_storage_id + 1) %
                                   available_stocs->servers.size();
            }
        }
        if (!candidate_storage_ids.empty()) {
            std::vector<StoCStatsStatus> storage_stats;
            for (int i = 0;
                 i < candidate_storage_ids.size(); i++) {
                uint32_t server_id = available_stocs->servers[candidate_storage_ids[i]].server_id;;
                uint32_t req_id = client->InitiateReadStoCStats(server_id);
                StoCStatsStatus status;
                status.remote_stoc_id = server_id;
                status.req_id = req_id;
                status.response = new StoCResponse;
                storage_stats.push_back(status);
            }
            for (int i = 0; i < storage_stats.size(); i++) {
                client->Wait();
            }
            for (int i = 0; i < storage_stats.size(); i++) {
                NOVA_ASSERT(client->IsDone(storage_stats[i].req_id,
                                           storage_stats[i].response,
                                           nullptr));
            }
            // sort the stoc stats.
            std::sort(storage_stats.begin(), storage_stats.end(),
                      stoc_stats_comparator);
            for (int i = 0; i < num_storage_to_select; i++) {
                (*selected_storage)[i] = storage_stats[i].remote_stoc_id;
            }
            for (int i = 0; i < storage_stats.size(); i++) {
                delete storage_stats[i].response;
            }
        }
    }
}