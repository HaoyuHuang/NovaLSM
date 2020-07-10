
//
// Created by Haoyu Huang on 7/9/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "sim_stoc_failure.h"

namespace nova {
    SimStoCFailure::SimStoCFailure(uint32_t fail_stoc_id,
                                   uint32_t exp_seconds_to_fail,
                                   std::vector<leveldb::DB *> dbs)
            : fail_stoc_id_(fail_stoc_id),
              exp_seconds_to_fail_(exp_seconds_to_fail), dbs_(dbs) {

    }

    void SimStoCFailure::Start() {
//        while (true) {
//
//        };

        std::unordered_map<uint32_t, std::vector<leveldb::ReplicationPair>> stoc_rerepl_pairs;
        for (int i = 0; i < dbs_.size(); i++) {
            auto db = dbs_[i];
            db->QueryReReplication(fail_stoc_id_, &stoc_rerepl_pairs);
        }

    }
}