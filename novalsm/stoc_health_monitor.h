
//
// Created by Haoyu Huang on 7/9/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_STOC_HEALTH_MONITOR_H
#define LEVELDB_STOC_HEALTH_MONITOR_H

#include "leveldb/db.h"
#include "ltc/stoc_client_impl.h"

namespace nova {
    class StoCHealthMonitor {
    public:
        StoCHealthMonitor(uint32_t fail_stoc_id,
                          int exp_seconds_to_fail,
                          leveldb::StoCBlockClient *stoc_client,
                          std::vector<leveldb::DB *> dbs);

        void Start();

    private:
        uint32_t fail_stoc_id_ = 0;
        int exp_seconds_to_fail_ = 0;
        leveldb::StoCBlockClient *stoc_client_ = nullptr;
        std::vector<leveldb::DB *> dbs_;
    };

}


#endif //LEVELDB_STOC_HEALTH_MONITOR_H
