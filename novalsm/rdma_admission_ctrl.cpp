
//
// Created by Haoyu Huang on 5/15/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "rdma_admission_ctrl.h"

namespace nova {
    void RDMAAdmissionCtrl::AddRequests(int server_id, int requests) {
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("admission add request {}:{}:{}", server_id,
                           requests, pending_rdma_sends_[server_id]);
        pending_rdma_sends_[server_id] += requests;
    }

    void RDMAAdmissionCtrl::RemoveRequests(int server_id, int requests) {
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("admission remove request {}:{}:{}", server_id,
                           requests, pending_rdma_sends_[server_id]);
        NOVA_ASSERT(pending_rdma_sends_[server_id] >= requests);
        pending_rdma_sends_[server_id] -= requests;
    }

    bool RDMAAdmissionCtrl::CanIssueRequest(int server_id) {
        if (pending_rdma_sends_[server_id] <
            max_pending_rdma_requests_per_endpoint_) {
            AddRequests(server_id, 1);
            return true;
        }
        return false;
    }

    bool RDMAAdmissionCtrl::CanIssueRequest(
            const std::vector<int> &server_ids) {
        bool success = true;
        for (auto sid : server_ids) {
            if (pending_rdma_sends_[sid] >=
                max_pending_rdma_requests_per_endpoint_) {
                success = false;
            }
        }
        if (!success) {
            return false;
        }
        for (auto sid : server_ids) {
            AddRequests(sid, 1);
        }
        return true;
    }
}