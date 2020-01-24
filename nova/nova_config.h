
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

#include "nova/rdma_ctrl.hpp"
#include "nova/nova_common.h"

namespace nova {
    using namespace std;
    using namespace rdmaio;

    class NovaConfig {
    public:
        bool enable_load_data;
        bool enable_rdma;

        vector<Host> servers;
        int my_server_id;
        uint64_t load_default_value_size;
        int max_msg_size;

        std::string db_path;

        int rdma_port;
        int rdma_pq_batch_size;
        int rdma_max_num_sends;
        int rdma_doorbell_batch_size;

        uint32_t log_buf_size;

        uint64_t mem_pool_size_gb;
        char *nova_buf;
        uint64_t nnovabuf;

        static NovaConfig *config;
    };

    class NovaCCConfig {
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

        static std::map<uint32_t, std::set<uint32_t >>
        ReadDatabases(const std::vector<CCFragment *> &fragments) {
            std::map<uint32_t, std::set<uint32_t >> server_dbs;
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
                    frag->cc_server_ids.push_back(std::stoi(tokens[i + 4]));
                }
                frags->push_back(frag);
            }
            RDMA_LOG(INFO) << "CC Configuration has a total of " << frags->size()
                           << " fragments.";
            for (int i = 0; i < frags->size(); i++) {
                RDMA_LOG(DEBUG) << fmt::format("frag[{}]: {}-{}-{}-{}-{}", i,
                                               (*frags)[i]->range.key_start,
                                               (*frags)[i]->range.key_end,
                                               (*frags)[i]->cc_server_id,
                                               (*frags)[i]->dbid,
                                               ToString(
                                                       (*frags)[i]->cc_server_ids));
            }
        }

        static CCFragment *home_fragment(uint64_t key) {
            CCFragment *home = nullptr;
            RDMA_ASSERT(
                    key <= cc_config->fragments[cc_config->fragments.size() -
                                                1]->range.key_end);
            uint32_t l = 0;
            uint32_t r = cc_config->fragments.size() - 1;

            while (l <= r) {
                uint32_t m = l + (r - l) / 2;
                home = cc_config->fragments[m];
                // Check if x is present at mid
                if (key >= home->range.key_start &&
                    key <= home->range.key_end) {
                    break;
                }
                // If x greater, ignore left half
                if (home->range.key_end < key)
                    l = m + 1;
                    // If x is smaller, ignore right half
                else
                    r = m - 1;
            }
            return home;
        }

        vector<Host> cc_servers;
        int num_conn_workers;
        int num_async_workers;
        int num_compaction_workers;
        int num_wb_workers;
        int block_cache_mb;
        int write_buffer_size_mb;
        std::vector<CCFragment *> fragments;
        std::vector<CCFragment *> db_fragment;

        static NovaCCConfig *cc_config;
    };

    class NovaDCConfig {
    public:
        static void
        ReadFragments(const std::string &path,
                        std::vector<DCFragment *> *frags) {
            std::string line;
            ifstream file;
            file.open(path);
            while (std::getline(file, line)) {
                auto *frag = new DCFragment();
                std::vector<std::string> tokens = SplitByDelimiter(&line, ",");
                frag->range.key_start = std::stoi(tokens[0]);
                frag->range.key_end = std::stoi(tokens[1]);
                frag->dc_server_id = std::stoi(tokens[2]);
                frags->push_back(frag);
            }
            RDMA_LOG(INFO) << "DC Configuration has a total of " << frags->size()
                           << " fragments.";
            for (int i = 0; i < frags->size(); i++) {
                RDMA_LOG(DEBUG) << fmt::format("frag[{}]: {}-{}-{}", i,
                                               (*frags)[i]->range.key_start,
                                               (*frags)[i]->range.key_end,
                                               (*frags)[i]->dc_server_id);
            }
        }

        static DCFragment *home_fragment(uint64_t key) {
            DCFragment *home = nullptr;
            RDMA_ASSERT(
                    key <= dc_config->fragments[dc_config->fragments.size() -
                                                1]->range.key_end);
            uint32_t l = 0;
            uint32_t r = dc_config->fragments.size() - 1;

            while (l <= r) {
                uint32_t m = l + (r - l) / 2;
                home = dc_config->fragments[m];
                // Check if x is present at mid
                if (key >= home->range.key_start &&
                    key <= home->range.key_end) {
                    break;
                }
                // If x greater, ignore left half
                if (home->range.key_end < key)
                    l = m + 1;
                    // If x is smaller, ignore right half
                else
                    r = m - 1;
            }
            return home;
        }

        int num_dc_workers;
        vector<Host> dc_servers;
        std::vector<DCFragment *> fragments;
        static NovaDCConfig *dc_config;
    };

    uint64_t nrdma_buf_cc();

    uint64_t nrdma_buf_dc();

    uint64_t nrdma_buf_unit();
}
#endif //NOVA_CC_CONFIG_H
