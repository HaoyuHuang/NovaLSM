
//
// Created by Haoyu Huang on 2/12/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_RDMA_WRITE_CLIENT_H
#define LEVELDB_RDMA_WRITE_CLIENT_H

#include "rdma/rdma_msg_callback.h"
#include "rdma/nova_rdma_broker.h"
#include "common/nova_mem_manager.h"

namespace nova {
    class RDMAWRITEClient : public RDMAMsgCallback {
    public:
        RDMAWRITEClient(uint32_t write_size_kb,
                        uint32_t my_server_id);

        bool
        ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id, int remote_server_id,
                      char *buf, uint32_t imm_data, bool *new_request);

        uint32_t Initiate();

        bool IsDone(uint32_t req_id);

        uint32_t thread_id_ = 0;
        NovaMemManager *mem_manager_ = nullptr;
        nova::NovaRDMABroker *rdma_broker_ = nullptr;

        uint32_t req_id = 1;
        uint32_t lower_req_id_ = 0;
        uint32_t upper_req_id_ = 0;
    private:
        struct RequestContext {
            char *local_buf = nullptr;
            int persisted = 0;
        };

        uint32_t write_size_kb_ = 0;
        uint32_t my_server_id_ = 0;
        char *buf_ = nullptr;
        std::map<uint32_t, RequestContext> req_context_;

    };
}


#endif //LEVELDB_RDMA_WRITE_CLIENT_H
