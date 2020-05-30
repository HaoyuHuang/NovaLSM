
//
// Created by Haoyu Huang on 5/15/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_RDMA_ADMISSION_CTRL_H
#define LEVELDB_RDMA_ADMISSION_CTRL_H

#include "nova/nova_config.h"

namespace nova {
    class RDMAAdmissionCtrl {
    public:
        RDMAAdmissionCtrl() : max_pending_rdma_requests_per_endpoint_(
                NovaConfig::config->rdma_max_num_sends) {
            pending_rdma_sends_ = new int[NovaConfig::config->servers.size()];
            for (int i = 0; i < NovaConfig::config->servers.size(); i++) {
                pending_rdma_sends_[i] = 0;
            }
        }

        bool CanIssueRequest(int server_id);

        bool CanIssueRequest(const std::vector<int>& server_ids);

        void RemoveRequests(int server_id, int requests);

        void AddRequests(int server_id, int requests);

    private:
        int *pending_rdma_sends_ = nullptr;
        const uint32_t max_pending_rdma_requests_per_endpoint_ = 0;
    };

}


#endif //LEVELDB_RDMA_ADMISSION_CTRL_H