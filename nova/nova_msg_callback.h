
//
// Created by Haoyu Huang on 4/1/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_MSG_CALLBACK_H
#define RLIB_NOVA_MSG_CALLBACK_H

#include <infiniband/verbs.h>

namespace nova {
    class NovaMsgCallback {
    public:
        virtual bool
        ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id, int remote_server_id,
                      char *buf, uint32_t imm_data, bool* generate_a_new_request) = 0;

    };
}
#endif //RLIB_NOVA_MSG_CALLBACK_H
