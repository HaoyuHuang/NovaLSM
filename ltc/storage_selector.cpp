
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
            StoCResponse response;
        };

        bool stoc_stats_comparator(const StoCStatsStatus &s1,
                                   const StoCStatsStatus &s2) {
            return s1.response.stoc_queue_depth < s2.response.stoc_queue_depth;
        }
    }

    StorageSelector::StorageSelector(leveldb::StoCBlockClient *client,
                                     unsigned int *rand_seed) : client_(client),
                                                                rand_seed_(
                                                                        rand_seed) {
    }


    void
    StorageSelector::SelectStorageServers(nova::ScatterPolicy scatter_policy,
                                          int num_storage_to_select,
                                          std::vector<uint32_t> *selected_storage) {
        selected_storage->clear();
        selected_storage->resize(num_storage_to_select);
        if (num_storage_to_select ==
            nova::NovaConfig::config->stoc_servers.size()) {
            for (int i = 0; i < num_storage_to_select; i++) {
                (*selected_storage)[i] = nova::NovaConfig::config->stoc_servers[i].server_id;
            }
            return;
        }

        std::vector<uint32_t> candidate_storage_ids;
        if (scatter_policy == nova::ScatterPolicy::POWER_OF_TWO) {
            uint32_t start_storage_id = rand_r(rand_seed_) %
                                        nova::NovaConfig::config->stoc_servers.size();
            uint32_t candidates = 2 * num_storage_to_select;
            if (candidates > nova::NovaConfig::config->stoc_servers.size()) {
                candidates = nova::NovaConfig::config->stoc_servers.size();
            }
            for (int i = 0; i < candidates; i++) {
                candidate_storage_ids.push_back(start_storage_id);
                start_storage_id = (start_storage_id + 1) %
                                   nova::NovaConfig::config->stoc_servers.size();
            }
        } else {
            // Random.
            // Select the start storage id then round robin.
            uint32_t start_storage_id = rand_r(rand_seed_) %
                                        nova::NovaConfig::config->stoc_servers.size();
            for (int i = 0; i < num_storage_to_select; i++) {
                (*selected_storage)[i] = nova::NovaConfig::config->stoc_servers[start_storage_id].server_id;
                start_storage_id = (start_storage_id + 1) %
                                   nova::NovaConfig::config->stoc_servers.size();
            }
        }
        if (!candidate_storage_ids.empty()) {
            std::vector<StoCStatsStatus> storage_stats;
            for (int i = 0;
                 i < candidate_storage_ids.size(); i++) {
                uint32_t server_id = nova::NovaConfig::config->stoc_servers[candidate_storage_ids[i]].server_id;
                uint32_t req_id = client_->InitiateReadStoCStats(server_id);
                StoCStatsStatus status;
                status.remote_stoc_id = server_id;
                status.req_id = req_id;
                storage_stats.push_back(status);
            }
            for (int i = 0; i < storage_stats.size(); i++) {
                client_->Wait();
            }
            for (int i = 0; i < storage_stats.size(); i++) {
                NOVA_ASSERT(client_->IsDone(storage_stats[i].req_id,
                                            &storage_stats[i].response,
                                            nullptr));
            }
            // sort the dc stats.
            std::sort(storage_stats.begin(), storage_stats.end(),
                      stoc_stats_comparator);
            for (int i = 0; i < num_storage_to_select; i++) {
                (*selected_storage)[i] = storage_stats[i].remote_stoc_id;
            }
        }
    }
}