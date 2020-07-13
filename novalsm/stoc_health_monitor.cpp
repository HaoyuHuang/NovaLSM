
//
// Created by Haoyu Huang on 7/9/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "stoc_health_monitor.h"

#include "ltc/storage_selector.h"

#define MAX_RESTORE_REPLICATION_BATCH_SIZE 10

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
        if (exp_seconds_to_fail_ == -1 || fail_stoc_id_ == -1) {
            return;
        }
        if (nova::NovaConfig::config->my_server_id
            >= nova::NovaConfig::config->ltc_servers.size()) {
            // Not a LTC.
            return;
        }
        if (nova::NovaConfig::config->number_of_sstable_replicas <= 1) {
            // no replication.
            return;
        }
        uint32_t now = 0;
        while (true) {
            if (now == exp_seconds_to_fail_) {
                break;
            }
            sleep(1);
            now++;
        }
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Fail StoC-{} at {} seconds", fail_stoc_id_,
                           exp_seconds_to_fail_);

        nova::Servers *new_available_stocs = new nova::Servers;
        for (int i = 0;
             i < nova::NovaConfig::config->stoc_servers.size(); i++) {
            if (i == fail_stoc_id_) {
                continue;
            }
            NOVA_LOG(rdmaio::INFO) << fmt::format("new server {}",
                                                  nova::NovaConfig::config->stoc_servers[i].server_id);
            new_available_stocs->servers.push_back(
                    nova::NovaConfig::config->stoc_servers[i]);
            new_available_stocs->server_ids.insert(
                    nova::NovaConfig::config->stoc_servers[i].server_id);
        }
        leveldb::StorageSelector::available_stoc_servers.store(
                new_available_stocs);

        now = 1;
        while (true) {
            if (now == nova::NovaConfig::config->failure_duration) {
                break;
            }
            sleep(1);
            now++;
        }
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Recover StoC-{} at {} seconds", fail_stoc_id_,
                           exp_seconds_to_fail_ +
                           nova::NovaConfig::config->failure_duration);

        uint32_t failed_stoc_server_id = nova::NovaConfig::config->stoc_servers[fail_stoc_id_].server_id;
        timeval repl_start{};
        gettimeofday(&repl_start, nullptr);

        std::unordered_map<uint32_t, std::vector<leveldb::ReplicationPair>> stoc_repl_pairs;
        uint32_t total_replicated = 0;
        for (int level = nova::NovaConfig::config->level - 1;
             level >= 0; level--) {
            for (int dbid = 0; dbid < dbs_.size(); dbid++) {
                stoc_repl_pairs.clear();

                timeval start{};
                gettimeofday(&start, nullptr);

                auto db = dbs_[dbid];
                db->QueryFailedReplicas(failed_stoc_server_id, &stoc_repl_pairs,
                                        level);

                if (stoc_repl_pairs.empty()) {
                    continue;
                }
                uint32_t num_batches = 0;
                for (const auto &pairs : stoc_repl_pairs) {
                    // break into pieces.
                    uint32_t stoc_id = pairs.first;
                    std::vector<leveldb::ReplicationPair> batch;
                    int i = 0;
                    while (i < pairs.second.size()) {
                        NOVA_LOG(rdmaio::INFO)
                            << fmt::format("Level {} Replicate {}",
                                           level,
                                           pairs.second[i].DebugString());
                        batch.push_back(pairs.second[i]);
                        i++;
                        if (batch.size() ==
                            MAX_RESTORE_REPLICATION_BATCH_SIZE) {
                            stoc_client_->InitiateReplicateSSTables(stoc_id,
                                                                    db->dbname(),
                                                                    batch);
                            batch.clear();
                            num_batches += 1;
                        }
                    }
                    if (!batch.empty()) {
                        stoc_client_->InitiateReplicateSSTables(stoc_id,
                                                                db->dbname(),
                                                                batch);
                        num_batches += 1;
                    }
                }
                NOVA_LOG(rdmaio::INFO)
                    << fmt::format("Replicate batches {}", num_batches);
                for (int b = 0; b < num_batches; b++) {
                    stoc_client_->Wait();
                    NOVA_LOG(rdmaio::INFO) << "Batch complete";
                }
                total_replicated += stoc_repl_pairs.size();
                timeval end{};
                gettimeofday(&end, nullptr);
                NOVA_LOG(rdmaio::INFO) << fmt::format(
                            "Restore replication factor for db-{} level-{} pairs:{} took {}",
                            dbid, level, stoc_repl_pairs.size(),
                            (end.tv_sec - start.tv_sec) * 1000000 +
                            (end.tv_usec - start.tv_usec));
            }
        }
        timeval repl_end{};
        gettimeofday(&repl_end, nullptr);
        NOVA_LOG(rdmaio::INFO) << fmt::format(
                    "Restore replication factor pairs:{} took {}",
                    total_replicated,
                    (repl_end.tv_sec - repl_start.tv_sec) * 1000000 +
                    (repl_end.tv_usec - repl_start.tv_usec));
    }
}