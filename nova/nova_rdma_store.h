
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_RDMA_STORE_H
#define RLIB_NOVA_RDMA_STORE_H

#include "rdma_ctrl.hpp"

namespace nova {
    class NovaRDMAStore {
    public:
        virtual void Init() = 0;

        virtual void PostRead(char *localbuf, uint32_t size, int server_id,
                              uint64_t local_offset,
                              uint64_t remote_addr, bool is_offset) = 0;

        virtual void PostSend(char *localbuf, uint32_t size, int server_id) = 0;

        virtual void PostWrite(char *localbuf, uint32_t size, int server_id,
                               uint64_t remote_offset,
                               bool is_remote_offset) = 0;

        virtual void FlushPendingSends() = 0;

        virtual void FlushPendingSends(int peer_sid) = 0;

        virtual void PollSQ(int peer_sid) = 0;

        virtual void PollSQ() = 0;

        virtual void PostRecv(int peer_sid, int recv_buf_index) = 0;

        virtual void FlushPendingRecvs() = 0;

        virtual void PollRQ() = 0;

        virtual void PollRQ(int peer_sid) = 0;

        virtual char *GetSendBuf() = 0;

        virtual char *GetSendBuf(int server_id) = 0;

        virtual uint32_t store_id() = 0;
    };


    class NovaRDMANoopStore : public NovaRDMAStore {
        void Init() {};

        void PostRead(char *localbuf, uint32_t size, int server_id,
                      uint64_t local_offset,
                      uint64_t remote_addr, bool is_offset) {}

        void PostSend(char *localbuf, uint32_t size, int server_id) {}

        void PostWrite(char *localbuf, uint32_t size, int server_id,
                       uint64_t remote_offset, bool is_remote_offset) {}

        void FlushPendingSends(int peer_sid) {}

        void FlushPendingSends() {}

        void PollSQ(int peer_sid) {}

        void PollSQ() {}

        void PostRecv(int peer_sid, int recv_buf_index) {}

        void FlushPendingRecvs() {}

        void PollRQ() {}

        void PollRQ(int peer_sid) {}

        char *GetSendBuf() { return NULL; }

        char *GetSendBuf(int server_id) { return NULL; }

        uint32_t store_id() { return 0; }
    };
}
#endif //RLIB_NOVA_RDMA_STORE_H
