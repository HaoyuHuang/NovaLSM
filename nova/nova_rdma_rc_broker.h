
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_RDMA_RC_STORE_H
#define RLIB_NOVA_RDMA_RC_STORE_H

#include "rdma_ctrl.hpp"
#include "nova_rdma_broker.h"
#include "nova_config.h"
#include "nova_msg_callback.h"

namespace nova {

    using namespace rdmaio;

    // Thread local. One thread has one RDMA RC broker.
    class NovaRDMARCBroker : public NovaRDMABroker {
    public:
        NovaRDMARCBroker(char *buf, int thread_id,
                         const std::vector<QPEndPoint> &end_points,
                         NovaMsgCallback *callback) :
                rdma_buf_(buf),
                thread_id_(thread_id),
                end_points_(end_points),
                callback_(callback) {
            RDMA_LOG(INFO) << "rc[" << thread_id << "]: " << "create rdma";
            int max_num_sends = NovaConfig::config->rdma_max_num_sends;
            int max_num_wrs = max_num_sends;
            int max_msg_size = NovaConfig::config->max_msg_size;
            int num_servers = end_points_.size();
            int doorbell_batch_size = NovaConfig::config->rdma_doorbell_batch_size;

            wcs_ = (ibv_wc *) malloc(max_num_wrs * sizeof(ibv_wc));
            qp_ = (RCQP **) malloc(num_servers * sizeof(RCQP *));
            rdma_send_buf_ = (char **) malloc(num_servers * sizeof(char *));
            rdma_recv_buf_ = (char **) malloc(num_servers * sizeof(char *));
            send_sges_ = (struct ibv_sge **) malloc(
                    num_servers * sizeof(struct ibv_sge *));
            send_wrs_ = (ibv_send_wr **) malloc(
                    num_servers * sizeof(struct ibv_send_wr *));
            send_sge_index_ = (int *) malloc(num_servers * sizeof(int));

            npending_send_ = (int *) malloc(num_servers * sizeof(int));
            psend_index_ = (int *) malloc(num_servers * sizeof(int));

            uint64_t nsendbuf = max_num_sends * max_msg_size;
            uint64_t nrecvbuf = max_num_sends * max_msg_size;
            uint64_t nbuf = nsendbuf + nrecvbuf;

            char *rdma_buf_start = buf;
            for (int i = 0; i < num_servers; i++) {
                npending_send_[i] = 0;
                psend_index_[i] = 0;

                send_sge_index_[i] = 0;
                qp_[i] = NULL;

                rdma_recv_buf_[i] = rdma_buf_start + nbuf * i;
                memset(rdma_recv_buf_[i], 0, nrecvbuf);

                rdma_send_buf_[i] = rdma_recv_buf_[i] + nrecvbuf;
                memset(rdma_send_buf_[i], 0, nsendbuf);

                send_sges_[i] = (ibv_sge *) malloc(
                        doorbell_batch_size * sizeof(struct ibv_sge));
                send_wrs_[i] = (ibv_send_wr *) malloc(
                        doorbell_batch_size * sizeof(struct ibv_send_wr));
                for (int j = 0; j < doorbell_batch_size; j++) {
                    memset(&send_sges_[i][j], 0, sizeof(struct ibv_sge));
                    memset(&send_wrs_[i][j], 0, sizeof(struct ibv_send_wr));
                }
            }
            RDMA_LOG(INFO) << "rc[" << thread_id << "]: " << "created rdma";
        }

        void Init();

        void PostRead(char *localbuf, uint32_t size, int server_id,
                      uint64_t local_offset,
                      uint64_t remote_addr, bool is_remote_offset);

        void PostSend(char *localbuf, uint32_t size, int server_id);

        void PostWrite(char *localbuf, uint32_t size, int server_id,
                       uint64_t remote_offset, bool is_remote_offset);

        void FlushPendingSends();

        void FlushPendingSends(int peer_sid) override;

        void PollSQ(int peer_sid);

        void PollSQ();

        void PostRecv(int peer_sid, int recv_buf_index);

        void FlushPendingRecvs();

        void PollRQ();

        void PollRQ(int peer_sid);

        char *GetSendBuf();

        char *GetSendBuf(int server_id);

        uint32_t store_id() { return thread_id_; }

    private:
        void PostRDMASEND(char *localbuf, ibv_wr_opcode type, uint32_t size,
                          int server_id,
                          uint64_t local_offset,
                          uint64_t remote_addr, bool is_offset);

        std::vector<QPEndPoint> end_points_;
        int thread_id_;
        char *rdma_buf_;

        // RDMA variables
        ibv_wc *wcs_;
        RCQP **qp_;
        char **rdma_send_buf_;
        char **rdma_recv_buf_;

        struct ibv_sge **send_sges_;
        ibv_send_wr **send_wrs_;
        int *send_sge_index_;

        // pending sends.
        int *npending_send_;
        int *psend_index_;
        NovaMsgCallback *callback_;
    };
}

#endif //RLIB_NOVA_RDMA_RC_STORE_H
