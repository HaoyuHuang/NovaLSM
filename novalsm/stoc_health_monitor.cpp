
//
// Created by Haoyu Huang on 7/9/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "stoc_health_monitor.h"

#include "ltc/storage_selector.h"

namespace nova {
    StoCHealthMonitor::StoCHealthMonitor(uint32_t fail_stoc_id,
                                         int exp_seconds_to_fail,
                                         leveldb::StoCBlockClient *stoc_client,
                                         std::vector<leveldb::DB *> dbs)
            : fail_stoc_id_(fail_stoc_id),
              exp_seconds_to_fail_(exp_seconds_to_fail),
              stoc_client_(stoc_client), dbs_(dbs) {

    }

    void StoCHealthMonitor::Start() {
        if (exp_seconds_to_fail_ == -1) {
            return;
        }

        uint32_t now = 0;
        while (true) {
            if (now == exp_seconds_to_fail_) {
                break;
            }
            sleep(1);
            now++;
        };

        nova::Servers *new_available_stocs;
        for (int i = 0;
             i < nova::NovaConfig::config->stoc_servers.size(); i++) {
            if (i == fail_stoc_id_) {
                continue;
            }
            new_available_stocs->servers.push_back(
                    nova::NovaConfig::config->stoc_servers[i]);
            new_available_stocs->server_ids.insert(
                    nova::NovaConfig::config->stoc_servers[i].server_id);
        }
        leveldb::StorageSelector::available_stoc_servers.store(
                new_available_stocs);

        std::unordered_map<uint32_t, std::vector<leveldb::ReplicationPair>> stoc_rerepl_pairs;
        for (int i = 0; i < dbs_.size(); i++) {
            auto db = dbs_[i];
            db->QueryFailedReplicas(fail_stoc_id_, &stoc_rerepl_pairs);
            for (auto it : stoc_rerepl_pairs) {
                stoc_client_->InitiateReplicateSSTables(it.first, db->dbname(),
                                                        it.second);
            }
            for (auto it : stoc_rerepl_pairs) {
                stoc_client_->Wait();
            }
        }

    }
}