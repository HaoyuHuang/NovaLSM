
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_RDMA_STORE_H
#define RLIB_NOVA_RDMA_STORE_H

#include "rdma_ctrl.hpp"

namespace nova {
    using namespace rdmaio;

    class NovaRDMAStore {
    public:
        virtual void Init(RdmaCtrl *rdma_ctrl) = 0;

        virtual uint64_t PostRead(char *localbuf, uint32_t size, int server_id,
                                  uint64_t local_offset,
                                  uint64_t remote_addr, bool is_offset) = 0;

        virtual uint64_t
        PostSend(const char *localbuf, uint32_t size, int server_id,
                 uint32_t imm_data) = 0;

        virtual uint64_t
        PostWrite(const char *localbuf, uint32_t size, int server_id,
                  uint64_t remote_offset,
                  bool is_remote_offset, uint32_t imm_data) = 0;

        virtual void FlushPendingSends() = 0;

        virtual void FlushPendingSends(int peer_sid) = 0;

        virtual uint32_t PollSQ(int peer_sid) = 0;

        virtual uint32_t PollSQ() = 0;

        virtual void PostRecv(int peer_sid, int recv_buf_index) = 0;

        virtual void FlushPendingRecvs() = 0;

        virtual uint32_t PollRQ() = 0;

        virtual uint32_t PollRQ(int peer_sid) = 0;

        virtual char *GetSendBuf() = 0;

        virtual char *GetSendBuf(int server_id) = 0;

        virtual uint32_t store_id() = 0;
    };


    class NovaRDMANoopStore : public NovaRDMAStore {
        void Init(RdmaCtrl *rdma_ctrl) {};

        uint64_t PostRead(char *localbuf, uint32_t size, int server_id,
                          uint64_t local_offset,
                          uint64_t remote_addr, bool is_offset) { return 0; }

        uint64_t PostSend(const char *localbuf, uint32_t size, int server_id,
                          uint32_t imm_data) { return 0; }

        uint64_t PostWrite(const char *localbuf, uint32_t size, int server_id,
                           uint64_t remote_offset, bool is_remote_offset,
                           uint32_t imm_data) { return 0; }

        void FlushPendingSends(int peer_sid) {}

        void FlushPendingSends() {}

        uint32_t PollSQ(int peer_sid) { return 0; }

        uint32_t PollSQ() { return 0; }

        void PostRecv(int peer_sid, int recv_buf_index) {}

        void FlushPendingRecvs() {}

        uint32_t PollRQ() { return 0; }

        uint32_t PollRQ(int peer_sid) { return 0; }

        char *GetSendBuf() { return NULL; }

        char *GetSendBuf(int server_id) { return NULL; }

        uint32_t store_id() { return 0; }
    };
}
#endif //RLIB_NOVA_RDMA_STORE_H
