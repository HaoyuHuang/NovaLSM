
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

#include "nova/rdma_ctrl.hpp"
#include "nova/nova_common.h"

namespace nova {
    using namespace std;
    using namespace rdmaio;

    class NovaConfig {
    public:
        static int
        ParseNumberOfDatabases(const std::vector<Fragment *> &fragments,
                               std::vector<Fragment *> *db_fragments,
                               uint32_t server_id) {
            std::set<uint32_t> ndbs;
            for (int i = 0; i < fragments.size(); i++) {
                if (fragments[i]->server_ids[0] == server_id) {
                    ndbs.insert(fragments[i]->db_ids[0]);
                }
            }
            db_fragments->resize(ndbs.size());

            for (int i = 0; i < fragments.size(); i++) {
                if (fragments[i]->server_ids[0] == server_id) {
                    (*db_fragments)[fragments[i]->db_ids[0]] = fragments[i];
                }
            }
            return ndbs.size();
        }

        static std::map<uint32_t, std::set<uint32_t >>
        ReadDatabases(const std::vector<Fragment *> &fragments) {
            std::map<uint32_t, std::set<uint32_t >> server_dbs;
            for (int i = 0; i < fragments.size(); i++) {
                uint32_t sid = fragments[i]->server_ids[0];
                uint32_t dbid = fragments[i]->db_ids[0];
                server_dbs[sid].insert(dbid);
            }
            return server_dbs;
        }

        static void
        ReadFragments(const std::string &path, std::vector<Fragment *> *frags) {
            std::string line;
            ifstream file;
            file.open(path);
            while (std::getline(file, line)) {
                auto *frag = new Fragment();
                std::vector<std::string> tokens = SplitByDelimiter(&line, ",");
                frag->key_start = std::stoi(tokens[0]);
                frag->key_end = std::stoi(tokens[1]);

                int nreplicas = (tokens.size() - 2) / 2;
                int index = 2;
                for (int i = 0; i < nreplicas; i++) {
                    frag->server_ids.push_back(std::stoi(tokens[index]));
                    frag->db_ids.push_back(std::stoi(tokens[index + 1]));
                    index += 2;
                }
                frags->push_back(frag);
            }
            RDMA_LOG(INFO) << "Configuration has a total of " << frags->size()
                           << " fragments.";
            for (int i = 0; i < frags->size(); i++) {
                RDMA_LOG(DEBUG) << "frag[" << i << "]: "
                                << (*frags)[i]->key_start
                                << "-" << (*frags)[i]->key_end
                                << "-" << ToString((*frags)[i]->server_ids)
                                << "-" << ToString((*frags)[i]->db_ids);
            }
        }

        bool enable_load_data;
        bool enable_rdma;

        vector<Host> servers;
        int my_server_id;
        int recordcount;
        uint64_t load_default_value_size;
        int max_msg_size;

        std::string db_path;
        std::string profiler_file_path;
        NovaLogRecordMode log_record_mode;

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
        static Fragment *home_fragment(uint64_t key) {
            return homefragment(cc_config->fragments, key);
        }

        vector<Host> cc_servers;
        int my_cc_server_id;

        int num_conn_workers;
        int num_async_workers;
        int num_compaction_workers;
        int block_cache_mb;
        int write_buffer_size_mb;
        std::vector<Fragment *> fragments;
        std::vector<Fragment *> db_fragment;

        static NovaCCConfig *cc_config;
    };

    class NovaDCConfig {
    public:
        static Fragment *home_fragment(uint64_t key) {
            return homefragment(dc_config->fragments, key);
        }

        int num_dc_workers;
        vector<Host> dc_servers;
        int my_dc_server_id;
        std::vector<Fragment *> fragments;
        static NovaDCConfig *dc_config;
    };

    uint64_t nrdma_buf_cc();

    uint64_t nrdma_buf_dc();

    uint64_t nrdma_buf_unit();
}
#endif //NOVA_CC_CONFIG_H
