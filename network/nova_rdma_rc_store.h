
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_RDMA_RC_STORE_H
#define RLIB_NOVA_RDMA_RC_STORE_H

#include "rdma_ctrl.hpp"
#include "nova_rdma_store.h"
#include "nova_mem_config.h"
#include "nova_msg_callback.h"

using namespace rdmaio;

class NovaRDMARCStore : public NovaRDMAStore {
public:
    NovaRDMARCStore(char *buf, int thread_id, NovaMsgCallback *callback) :
            rdma_buf_(buf),
            thread_id_(thread_id),
            callback_(callback) {
        RDMA_LOG(INFO) << "rc[" << thread_id << "]: " << "create rdma";
        int max_num_reads = NovaConfig::config->rdma_max_num_reads;
        int max_num_sends = NovaConfig::config->rdma_max_num_sends;
        int max_num_wrs = max_num_reads + max_num_sends;
        int max_msg_size = NovaConfig::config->max_msg_size;
        int num_servers = NovaConfig::config->servers.size();
        int doorbell_batch_size = NovaConfig::config->rdma_doorbell_batch_size;

        wcs_ = (ibv_wc *) malloc(max_num_wrs * sizeof(ibv_wc));
        qp_ = (RCQP **) malloc(num_servers * sizeof(RCQP *));
        rdma_read_buf_ = (char **) malloc(num_servers * sizeof(char *));
        rdma_write_buf_ = (char **) malloc(num_servers * sizeof(char *));
        send_sges_ = (struct ibv_sge **) malloc(
                num_servers * sizeof(struct ibv_sge *));
        send_wrs_ = (ibv_send_wr **) malloc(
                num_servers * sizeof(struct ibv_send_wr *));
        send_sge_index_ = (int *) malloc(num_servers * sizeof(int));

        npending_read_ = (int *) malloc(num_servers * sizeof(int));
        pread_index_ = (int *) malloc(num_servers * sizeof(int));
        npending_send_ = (int *) malloc(num_servers * sizeof(int));
        psend_index_ = (int *) malloc(num_servers * sizeof(int));

        recv_sges_ = (struct ibv_sge **) malloc(
                num_servers * sizeof(struct ibv_sge *));
        recv_wrs_ = (ibv_recv_wr **) malloc(
                num_servers * sizeof(struct ibv_recv_wr *));
        pending_recv_index_ = (int *) malloc(num_servers * sizeof(int));
        recv_sge_index_ = (int *) malloc(num_servers * sizeof(int));

        uint64_t nwritebuf = max_num_sends * 2 * max_msg_size;
        uint64_t nreadbuf = max_num_reads * max_msg_size;
        uint64_t nbuf = nwritebuf + nreadbuf;

        char *rdma_buf_start = buf;
        for (int i = 0; i < num_servers; i++) {
            npending_read_[i] = 0;
            pread_index_[i] = 0;
            npending_send_[i] = 0;
            psend_index_[i] = 0;

            send_sge_index_[i] = 0;
            recv_sge_index_[i] = 0;
            qp_[i] = NULL;

            rdma_write_buf_[i] = rdma_buf_start + nbuf * i;
            memset(rdma_write_buf_[i], 0, nwritebuf);

            rdma_read_buf_[i] = rdma_write_buf_[i] + nwritebuf;
            memset(rdma_read_buf_[i], 0, nreadbuf);

            pending_recv_index_[i] = 0;
            recv_sge_index_[i] = 0;

            send_sges_[i] = (ibv_sge *) malloc(
                    doorbell_batch_size * sizeof(struct ibv_sge));
            send_wrs_[i] = (ibv_send_wr *) malloc(
                    doorbell_batch_size * sizeof(struct ibv_send_wr));
            recv_sges_[i] = (ibv_sge *) malloc(
                    doorbell_batch_size * sizeof(struct ibv_sge));
            recv_wrs_[i] = (ibv_recv_wr *) malloc(
                    doorbell_batch_size * sizeof(struct ibv_recv_wr));
            for (int j = 0; j < doorbell_batch_size; j++) {
                memset(&send_sges_[i][j], 0, sizeof(struct ibv_sge));
                memset(&send_wrs_[i][j], 0, sizeof(struct ibv_send_wr));
                memset(&recv_sges_[i][j], 0, sizeof(struct ibv_sge));
                memset(&recv_wrs_[i][j], 0, sizeof(struct ibv_recv_wr));
            }
        }
        RDMA_LOG(INFO) << "rc[" << thread_id << "]: " << "created rdma";
    }

    void Init() override;

    void PostRead(uint32_t size, int server_id, uint64_t local_offset,
                  uint64_t remote_addr, bool is_offset) override;

    void PostSend(uint32_t size, int server_id) override;

    void
    PostWrite(uint32_t size, int server_id, uint64_t remote_offset) override;

    void FlushPendingSends() override;

    void PollSQ(int peer_sid);

    void PollSQ() override;

    void PostRecv(uint64_t wr_id) override;

    void FlushPendingRecvs() override;

    void PollAndProcessRQ() override;

    char *GetSendBuf() override;

    char *GetSendBuf(int server_id) override;

private:
    int thread_id_;
    char *rdma_buf_;

    // RDMA variables
    ibv_wc *wcs_;
    RCQP **qp_;
    char **rdma_read_buf_;
    char **rdma_write_buf_;

    struct ibv_sge **send_sges_;
    ibv_send_wr **send_wrs_;
    int *send_sge_index_;

    // pending reads.
    int *npending_read_;
    int *pread_index_;

    // pending writes.
    int *npending_send_;
    int *psend_index_;

    struct ibv_sge **recv_sges_;
    ibv_recv_wr **recv_wrs_;
    int *pending_recv_index_;
    int *recv_sge_index_;
    NovaMsgCallback *callback_;
};


#endif //RLIB_NOVA_RDMA_RC_STORE_H
