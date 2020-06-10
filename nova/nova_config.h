
//
// Created by Haoyu Huang on 2/24/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef NOVA_CC_CONFIG_H
#define NOVA_CC_CONFIG_H

#include <sstream>
#include <string>
#include <fstream>
#include <list>
#include <fmt/core.h>
#include <thread>
#include <syscall.h>
#include <atomic>

#include "nova/rdma_ctrl.hpp"
#include "nova/nova_common.h"

namespace nova {
    using namespace std;
    using namespace rdmaio;

    enum ScatterPolicy {
        SCATTER_DC_STATS,
        RANDOM,
        POWER_OF_TWO,
        POWER_OF_THREE
    };

    struct ZipfianDist {
        uint64_t sum = 0;
        std::vector<uint64_t> accesses;
    };

    class NovaConfig {
    public:
        static int
        ParseNumberOfDatabases(const std::vector<CCFragment *> &fragments,
                               std::vector<CCFragment *> *db_fragments,
                               uint32_t server_id) {
            std::set<uint32_t> ndbs;
            for (int i = 0; i < fragments.size(); i++) {
                if (fragments[i]->cc_server_id == server_id) {
                    ndbs.insert(fragments[i]->dbid);
                }
            }
            db_fragments->resize(ndbs.size());

            for (int i = 0; i < fragments.size(); i++) {
                if (fragments[i]->cc_server_id == server_id) {
                    (*db_fragments)[fragments[i]->dbid] = fragments[i];
                }
            }
            return ndbs.size();
        }

        static std::unordered_map<uint32_t, std::set<uint32_t >>
        ReadDatabases(const std::vector<CCFragment *> &fragments) {
            std::unordered_map<uint32_t, std::set<uint32_t >> server_dbs;
            for (int i = 0; i < fragments.size(); i++) {
                uint32_t sid = fragments[i]->cc_server_id;
                uint32_t dbid = fragments[i]->dbid;
                server_dbs[sid].insert(dbid);
            }
            return server_dbs;
        }

        static void
        ReadFragments(const std::string &path,
                      std::vector<CCFragment *> *frags) {
            std::string line;
            ifstream file;
            file.open(path);
            while (std::getline(file, line)) {
                auto *frag = new CCFragment();
                std::vector<std::string> tokens = SplitByDelimiter(&line, ",");
                frag->range.key_start = std::stoi(tokens[0]);
                frag->range.key_end = std::stoi(tokens[1]);
                frag->cc_server_id = std::stoi(tokens[2]);
                frag->dbid = std::stoi(tokens[3]);

                int nreplicas = (tokens.size() - 4);
                for (int i = 0; i < nreplicas; i++) {
                    frag->log_replica_stoc_ids.push_back(
                            std::stoi(tokens[i + 4]));
                }
                frags->push_back(frag);
            }
            RDMA_LOG(INFO) << "CC Configuration has a total of "
                           << frags->size()
                           << " fragments.";
            for (int i = 0; i < frags->size(); i++) {
                RDMA_LOG(DEBUG) << fmt::format("frag[{}]: {}-{}-{}-{}-{}", i,
                                               (*frags)[i]->range.key_start,
                                               (*frags)[i]->range.key_end,
                                               (*frags)[i]->cc_server_id,
                                               (*frags)[i]->dbid,
                                               ToString(
                                                       (*frags)[i]->log_replica_stoc_ids));
            }
        }

        static CCFragment *home_fragment(uint64_t key) {
            CCFragment *home = nullptr;
            RDMA_ASSERT(
                    key <= config->fragments[config->fragments.size() -
                                             1]->range.key_end);
            uint32_t l = 0;
            uint32_t r = config->fragments.size() - 1;

            while (l <= r) {
                uint32_t m = l + (r - l) / 2;
                home = config->fragments[m];
                // Check if x is present at mid
                if (key >= home->range.key_start &&
                    key < home->range.key_end) {
                    break;
                }
                // If x greater, ignore left half
                if (key >= home->range.key_end)
                    l = m + 1;
                    // If x is smaller, ignore right half
                else
                    r = m - 1;
            }
            return home;
        }

        bool enable_load_data;
        bool enable_rdma;

        vector<Host> servers;
        int my_server_id;
        uint64_t load_default_value_size;
        int max_msg_size;

        std::string db_path;

        int rdma_port;
        int rdma_max_num_sends;
        int rdma_doorbell_batch_size;

        uint64_t log_buf_size;
        uint64_t rtable_size;
        uint64_t sstable_size;
        std::string rtable_path;

        bool use_local_disk;
        bool enable_subrange;
        bool enable_subrange_reorg;
        bool enable_flush_multiple_memtables;
        std::string memtable_type;
        std::string major_compaction_type;
        uint32_t major_compaction_max_parallism;
        uint32_t major_compaction_max_tables_in_a_set;

        uint64_t mem_pool_size_gb;
        uint32_t num_mem_partitions;
        char *nova_buf;
        uint64_t nnovabuf;

        ScatterPolicy scatter_policy;
        NovaLogRecordMode log_record_mode;
        bool recover_dbs;
        uint32_t number_of_recovery_threads;

        double subrange_sampling_ratio;
        std::string zipfian_dist_file_path;
        ZipfianDist zipfian_dist;
        std::string client_access_pattern;
        bool enable_detailed_db_stats;
        int num_tinyranges_per_subrange;
        int subrange_num_keys_no_flush;

        vector<Host> cc_servers;
        vector<Host> dc_servers;
        int num_conn_workers;
        int num_conn_async_workers;
        int num_compaction_workers;
        int num_rdma_compaction_workers;
        int num_storage_workers;
        int level;

        int block_cache_mb;
        int row_cache_mb;
        bool enable_table_locator;
        uint32_t num_memtables;
        uint32_t num_memtable_partitions;
        uint64_t write_buffer_size_mb;
        uint64_t l0_stop_write_mb;
        uint64_t l0_start_compaction_mb;
        std::vector<CCFragment *> fragments;
        std::vector<CCFragment *> db_fragment;
        int num_rtable_num_servers_scatter_data_blocks;

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

                RDMA_LOG(INFO) << fmt::format("{}:{}", thread_id, tid.second);
            }
        }

        std::mutex m;
        std::map<std::thread::id, pid_t> threads;
        static NovaConfig *config;
    };

    uint64_t nrdma_buf_cc();

    uint64_t nrdma_buf_unit();
}
#endif //NOVA_CC_CONFIG_H
