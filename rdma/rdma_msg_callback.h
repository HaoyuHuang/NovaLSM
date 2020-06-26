
//
// Created by Haoyu Huang on 4/1/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RDMA_MSG_CALLBACK_H
#define RDMA_MSG_CALLBACK_H

#include <infiniband/verbs.h>

namespace nova {
    // After calling ibv_poll_cq, this function is invoked for each completion event.
    class RDMAMsgCallback {
    public:
        virtual bool
        ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id, int remote_server_id,
                      char *buf, uint32_t imm_data,
                      bool *generate_a_new_request) = 0;

    };
}
#endif //RDMA_MSG_CALLBACK_H
