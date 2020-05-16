
//
// Created by Haoyu Huang on 5/13/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_STORAGE_SELECTOR_H
#define LEVELDB_STORAGE_SELECTOR_H

#include "util/env_mem.h"
#include "nova_cc_client.h"
#include "leveldb/env.h"
#include "leveldb/table.h"

namespace leveldb {
    class StorageSelector {
    public:
        StorageSelector(NovaBlockCCClient *client, unsigned int *rand_seed);

        void SelectStorageServers(nova::ScatterPolicy  scatter_policy, int num_storage_to_select,
                                  std::vector<uint32_t> *selected_storage);
    private:
        NovaBlockCCClient *client_;
        unsigned int *rand_seed_;
    };
}


#endif //LEVELDB_STORAGE_SELECTOR_H
