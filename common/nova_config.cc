
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

    uint64_t nrdma_buf_cc() {
        // A CC async/bg thread connects to one thread at each DC.
        uint64_t nrdmatotal = nrdma_buf_unit() *
                              (NovaConfig::config->num_conn_async_workers +
                               NovaConfig::config->num_rdma_compaction_workers) *
                              NovaConfig::config->servers.size();
        return nrdmatotal;
    }
}