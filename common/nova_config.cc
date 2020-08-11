
//
// Created by Haoyu Huang on 1/10/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_config.h"

namespace nova {
    uint64_t nrdma_buf_unit() {
        return (NovaConfig::config->rdma_max_num_sends * 2) *
               NovaConfig::config->max_msg_size;
    }

    uint64_t nrdma_buf_server() {
        // A CC async/bg thread connects to one thread at each DC.
        uint64_t nrdmatotal = nrdma_buf_unit() *
                              (NovaConfig::config->num_fg_rdma_workers +
                               NovaConfig::config->num_bg_rdma_workers) *
                              NovaConfig::config->servers.size();
        return nrdmatotal;
    }

    std::string Configuration::DebugString() {
        std::string ltcs;
        std::string stocs;
        for (int i = 0; i < ltc_servers.size(); i++) {
            ltcs += std::to_string(ltc_servers[i]);
            ltcs += ",";
        }
        for (int i = 0; i < stoc_servers.size(); i++) {
            stocs += std::to_string(stoc_servers[i]);
            stocs += ",";
        }
        std::string debug = fmt::format("CfgId: {} StartTime:{} Number of fragment: {}\nLTCs:{}\nStoCs:{}\n",
                                        cfg_id, start_time_in_seconds, fragments.size(), ltcs, stocs);
        for (int i = 0; i < fragments.size(); i++) {
            debug += fmt::format("frag[{}]: {}-{}-{}-{}-{}\n", i,
                                 fragments[i]->range.key_start,
                                 fragments[i]->range.key_end,
                                 fragments[i]->ltc_server_id,
                                 fragments[i]->dbid,
                                 ToString(fragments[i]->log_replica_stoc_ids));
        }
        return debug;
    }

    bool Configuration::IsLTC() {
        return ltc_server_ids.find(NovaConfig::config->my_server_id) != ltc_server_ids.end();
    }

    bool Configuration::IsStoC() {
        return stoc_server_ids.find(NovaConfig::config->my_server_id) != stoc_server_ids.end();
    }
}