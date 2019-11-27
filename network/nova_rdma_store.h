
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_RDMA_STORE_H
#define RLIB_NOVA_RDMA_STORE_H

#include "rdma_ctrl.hpp"

class NovaRDMAStore {
public:
    virtual void Init() = 0;

    virtual void PostRead(uint32_t size, int server_id, uint64_t local_offset,
                          uint64_t remote_addr, bool is_offset) = 0;

    virtual void PostSend(uint32_t size, int server_id) = 0;

    virtual void
    PostWrite(uint32_t size, int server_id, uint64_t remote_offset) = 0;

    virtual void FlushPendingSends() = 0;

    virtual void PollSQ() = 0;

    virtual void PostRecv(uint64_t wr_id) = 0;

    virtual void FlushPendingRecvs() = 0;

    virtual void PollAndProcessRQ() = 0;

    virtual char *GetSendBuf() = 0;

    virtual char *GetSendBuf(int server_id) = 0;
};


class NovaRDMANoopStore : public NovaRDMAStore {
    void Init() {};

    void PostRead(uint32_t size, int server_id, uint64_t local_offset,
                  uint64_t remote_addr, bool is_offset) {};

    void PostSend(uint32_t size, int server_id) {};

    void PostWrite(uint32_t size, int server_id, uint64_t remote_offset) {};

    void FlushPendingSends() {};

    void PollSQ() {};

    void PostRecv(uint64_t wr_id) {};

    void FlushPendingRecvs() {};

    void PollAndProcessRQ() {};

    char *GetSendBuf() { return NULL; };

    char *GetSendBuf(int server_id) { return NULL; };
};

#endif //RLIB_NOVA_RDMA_STORE_H
