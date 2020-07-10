
//
// Created by Haoyu Huang on 7/9/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_SIM_STOC_FAILURE_H
#define LEVELDB_SIM_STOC_FAILURE_H

#include "leveldb/db.h"

namespace nova {
    class SimStoCFailure {
    public:
        SimStoCFailure(uint32_t fail_stoc_id,
                       uint32_t exp_seconds_to_fail,
                       std::vector<leveldb::DB *> dbs);

        void Start();

    private:
        uint32_t fail_stoc_id_ = 0;
        uint32_t exp_seconds_to_fail_ = 0;
        leveldb::StoCClient *stoc_client_;
        std::vector<leveldb::DB *> dbs_;
    };

}


#endif //LEVELDB_SIM_STOC_FAILURE_H
