
//
// Created by Haoyu Huang on 2/24/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef NOVA_CONFIG_H
#define NOVA_CONFIG_H

#include <sstream>
#include <string>
#include <fstream>
#include <list>
#include <fmt/core.h>
#include <thread>
#include <syscall.h>
#include <atomic>

#include "rdma/rdma_ctrl.hpp"
#include "nova_common.h"

namespace nova {
    using namespace std;
    using namespace rdmaio;

    enum ScatterPolicy {
        SCATTER_DC_STATS,
        RANDOM,
        POWER_OF_TWO,
        POWER_OF_THREE,
        LOCAL
    };

    enum LTCMigrationPolicy {
        PROCESS_UNTIL_MIGRATION_COMPLETE,
        IMMEDIATE
    };

    struct ZipfianDist {
        uint64_t sum = 0;
        std::vector<uint64_t> accesses;
    };

    struct Configuration {
        uint32_t cfg_id = 0;
        std::vector<LTCFragment *> fragments;
        uint64_t start_time_in_seconds = 0;
        uint64_t start_time_us_ = 0;

        vector<uint32_t> ltc_servers;
        vector<uint32_t> stoc_servers;

        std::set<uint32_t> ltc_server_ids;
        std::set<uint32_t> stoc_server_ids;

        bool IsLTC();

        bool IsStoC();

        std::string DebugString();
    };

    class NovaConfig {
    public:
        NovaConfig() {
            current_cfg_id = 0;
        }

        static void ComputeLogReplicaLocations(uint32_t num_log_replicas) {
            auto init_cfg = config->cfgs[0];
            for (auto cfg : config->cfgs) {
                uint32_t start_stoc_id = 0;
                for (int i = 0; i < cfg->fragments.size(); i++) {
                    cfg->fragments[i]->log_replica_stoc_ids.clear();
                    std::set<uint32_t> set;
                    for (int r = 0; r < num_log_replicas; r++) {
                        if (config->use_local_disk &&
                                init_cfg->stoc_servers[start_stoc_id] == config->my_server_id) {
                            // Don't write the log record locally.
                            start_stoc_id = (start_stoc_id + 1) % init_cfg->stoc_servers.size();
                        }
                        cfg->fragments[i]->log_replica_stoc_ids.push_back(start_stoc_id);
                        set.insert(start_stoc_id);
                        start_stoc_id = (start_stoc_id + 1) % init_cfg->stoc_servers.size();
                    }
                    NOVA_ASSERT(set.size() == num_log_replicas);
                    NOVA_ASSERT(set.size() == cfg->fragments[i]->log_replica_stoc_ids.size());
                }
            }
        }

        static void
        ReadFragments(const std::string &path) {
            std::string line;
            ifstream file;
            file.open(path);

            Configuration *cfg = nullptr;
            uint32_t cfg_id = 0;
            while (std::getline(file, line)) {
                if (line.find("config") != std::string::npos) {
                    cfg = new Configuration;
                    cfg->cfg_id = cfg_id;
                    cfg_id++;
                    config->cfgs.push_back(cfg);
                    NOVA_ASSERT(std::getline(file, line));
                    cfg->ltc_servers = SplitByDelimiterToInt(&line, ",");
                    NOVA_ASSERT(std::getline(file, line));
                    cfg->stoc_servers = SplitByDelimiterToInt(&line, ",");
                    NOVA_ASSERT(std::getline(file, line));
                    cfg->start_time_in_seconds = std::stoi(line);

                    for (int i = 0; i < cfg->ltc_servers.size(); i++) {
                        cfg->ltc_server_ids.insert(cfg->ltc_servers[i]);
                    }
                    for (int i = 0; i < cfg->stoc_servers.size(); i++) {
                        cfg->stoc_server_ids.insert(cfg->stoc_servers[i]);
                    }
                    continue;
                }
                NOVA_LOG(INFO) << fmt::format("Read config line: {}", line);
                auto *frag = new LTCFragment();
                std::vector<std::string> tokens = SplitByDelimiter(&line, ",");
                frag->range.key_start = std::stoll(tokens[0]);
                frag->range.key_end = std::stoll(tokens[1]);
                frag->ltc_server_id = std::stoll(tokens[2]);
                frag->dbid = std::stoi(tokens[3]);
                if (cfg->cfg_id == 0) {
                    frag->is_ready_ = true;
                    frag->is_complete_ = true;
                }
                int nreplicas = (tokens.size() - 4);
                for (int i = 0; i < nreplicas; i++) {
                    frag->log_replica_stoc_ids.push_back(
                            std::stoi(tokens[i + 4]));
                }
//                NOVA_LOG(rdmaio::INFO) << fmt::format("{}", frag->DebugString());
                cfg->fragments.push_back(frag);
            }
        }

        static LTCFragment *
        home_fragment(uint64_t key, uint32_t server_cfg_id) {
            LTCFragment *home = nullptr;
            Configuration *cfg = config->cfgs[server_cfg_id];
            NOVA_ASSERT(
                    key <= cfg->fragments[cfg->fragments.size() - 1]->range.key_end);
            uint32_t l = 0;
            uint32_t r = cfg->fragments.size() - 1;

            while (l <= r) {
                uint32_t m = l + (r - l) / 2;
                home = cfg->fragments[m];
                // Check if x is present at mid
                if (key >= home->range.key_start && key < home->range.key_end) {
                    return home;
                }
                // If x greater, ignore left half
                if (key >= home->range.key_end)
                    l = m + 1;
                    // If x is smaller, ignore right half
                else
                    r = m - 1;
            }
            return nullptr;
        }

        bool enable_load_data = false;
        bool enable_rdma = false;
        bool use_ordered_flush = false;

        vector<Host> servers;
        int my_server_id = 0;

        uint64_t load_default_value_size = 0;
        int max_msg_size = 0;

        std::string db_path;

        int rdma_port = 0;
        int rdma_max_num_sends = 0;
        int rdma_doorbell_batch_size = 0;

        uint64_t max_stoc_file_size = 0;
        uint64_t sstable_size = 0;
        uint64_t manifest_file_size = 0;
        std::string stoc_files_path;

        bool use_local_disk = false;
        bool enable_subrange = false;
        bool enable_subrange_reorg = false;
        bool enable_flush_multiple_memtables = false;
        std::string memtable_type;
        std::string major_compaction_type;
        uint32_t major_compaction_max_parallism = 0;
        uint32_t major_compaction_max_tables_in_a_set = 0;

        uint64_t mem_pool_size_gb = 0;
        uint32_t num_mem_partitions = 0;
        char *nova_buf = nullptr;
        uint64_t nnovabuf = 0;

        ScatterPolicy scatter_policy = ScatterPolicy::POWER_OF_TWO;
        NovaLogRecordMode log_record_mode = NovaLogRecordMode::LOG_NONE;
        bool recover_dbs = false;
        uint32_t number_of_recovery_threads = 0;
        uint32_t number_of_sstable_metadata_replicas = 0;
        uint32_t number_of_sstable_data_replicas = 0;
        uint32_t number_of_manifest_replicas = 0;
        bool use_parity_for_sstable_data_blocks = false;

        double subrange_sampling_ratio = 0;
        std::string zipfian_dist_file_path;
        ZipfianDist zipfian_dist;
        std::string client_access_pattern;
        bool enable_detailed_db_stats = false;
        int num_tinyranges_per_subrange = 0;
        int subrange_num_keys_no_flush = 0;

        int num_conn_workers= 0;
        int num_fg_rdma_workers = 0;
        int num_compaction_workers = 0;
        int num_bg_rdma_workers = 0;
        int num_storage_workers = 0;
        int level = 0;

        int block_cache_mb = 0;
        bool enable_lookup_index = false;
        bool enable_range_index = false;
        uint32_t num_memtables = 0;
        uint32_t num_memtable_partitions = 0;
        uint64_t memtable_size_mb = 0;
        uint64_t l0_stop_write_mb = 0;
        uint64_t l0_start_compaction_mb = 0;

        int num_stocs_scatter_data_blocks = 0;
        int num_migration_threads = 0;

        LTCMigrationPolicy ltc_migration_policy = LTCMigrationPolicy::IMMEDIATE;

        void ReadZipfianDist() {
            if (zipfian_dist_file_path.empty()) {
                return;
            }

            std::string line;
            ifstream file;
            file.open(zipfian_dist_file_path);
            while (std::getline(file, line)) {
                uint64_t accesses = std::stoi(line);
                zipfian_dist.accesses.push_back(accesses);
                zipfian_dist.sum += accesses;
            }
        }

        void add_tid_mapping() {
            std::lock_guard<std::mutex> l(m);
            threads[std::this_thread::get_id()] = syscall(SYS_gettid);
        }

        void print_mapping() {
            std::lock_guard<std::mutex> l(m);
            for (auto tid : threads) {
                constexpr const int kMaxThreadIdSize = 32;
                std::ostringstream thread_stream;
                thread_stream << tid.first;
                std::string thread_id = thread_stream.str();
                if (thread_id.size() > kMaxThreadIdSize) {
                    thread_id.resize(kMaxThreadIdSize);
                }

                NOVA_LOG(INFO) << fmt::format("{}:{}", thread_id, tid.second);
            }
        }

        std::vector<Configuration *> cfgs;
        std::atomic_uint_fast32_t current_cfg_id;
        std::mutex m;
        std::map<std::thread::id, pid_t> threads;
        static NovaConfig *config;
    };

    uint64_t nrdma_buf_server();

    uint64_t nrdma_buf_unit();
}
#endif //NOVA_CONFIG_H
