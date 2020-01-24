
//
// Created by Haoyu Huang on 1/10/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_DC_SERVER_H
#define LEVELDB_NOVA_DC_SERVER_H

#include <thread>
#include "nova/rdma_ctrl.hpp"
#include "cc/nova_cc_server.h"

namespace nova {
    class NovaDCServer {
    public:
        NovaDCServer(rdmaio::RdmaCtrl *rdma_ctrl, char *rdmabuf,
                     std::map<uint32_t, std::set<uint32_t >> &dbs);

        void Start();
    private:
        std::vector<NovaCCServer *> dcs_;
        std::vector<std::thread> worker_threads;
    };
}


#endif //LEVELDB_NOVA_DC_SERVER_H
