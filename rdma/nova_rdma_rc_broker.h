
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_RDMA_RC_STORE_H
#define RLIB_NOVA_RDMA_RC_STORE_H

#include <fmt/core.h>

#include "rdma_ctrl.hpp"
#include "nova_rdma_broker.h"
#include "rdma_msg_callback.h"
#include "common/nova_common.h"


namespace nova {

    using namespace rdmaio;

    // Thread local. One thread has one RDMA RC Broker.
    // It maintains a circular buffer to issue RDMA SENDs.
    class NovaRDMARCBroker : public NovaRDMABroker {
    public:
        NovaRDMARCBroker(char *buf, int thread_id,
                         const std::vector<QPEndPoint> &end_points,
                         int total_num_servers,
                         uint32_t max_num_sends,
                         uint32_t max_msg_size,
                         uint32_t doorbell_batch_size,
                         uint32_t my_server_id,
                         char *mr_buf,
                         uint64_t mr_size,
                         uint64_t rdma_port,
                         RDMAMsgCallback *callback) :
                rdma_buf_(buf),
                thread_id_(thread_id),
                end_points_(end_points),
                max_num_sends_(max_num_sends),
                max_msg_size_(max_msg_size),
                doorbell_batch_size_(doorbell_batch_size),
                my_server_id_(my_server_id),
                mr_buf_(mr_buf),
                mr_size_(mr_size),
                rdma_port_(rdma_port),
                callback_(callback) {
            NOVA_LOG(DEBUG)
                << fmt::format("rc[{}]: create rdma {} {} {} {} {} {} {}.",
                               thread_id_,
                               max_num_sends_,
                               max_msg_size_,
                               doorbell_batch_size_,
                               my_server_id_,
                               mr_size_,
                               rdma_port_, end_points_.size());
            int max_num_wrs = max_num_sends;
            int num_servers = end_points_.size();

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
            server_qp_idx_map_ = new int[total_num_servers];
            for (int i = 0; i < total_num_servers; i++) {
                server_qp_idx_map_[i] = -1;
            }

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
                server_qp_idx_map_[end_points[i].server_id] = i;
            }
        }

        void Init(RdmaCtrl *rdma_ctrl);

        uint64_t PostRead(char *localbuf, uint32_t size, int server_id,
                          uint64_t local_offset,
                          uint64_t remote_addr, bool is_remote_offset);

        uint64_t PostSend(const char *localbuf, uint32_t size, int server_id,
                          uint32_t imm_data);

        uint64_t PostWrite(const char *localbuf, uint32_t size, int server_id,
                           uint64_t remote_offset, bool is_remote_offset,
                           uint32_t imm_data);

        void FlushPendingSends();

        void FlushPendingSends(int peer_sid) override;

        uint32_t PollSQ(int peer_sid, uint32_t *new_requests);

        void PostRecv(int peer_sid, int recv_buf_index);

        void FlushPendingRecvs();

        uint32_t PollRQ(int peer_sid, uint32_t *new_requests);

        char *GetSendBuf();

        char *GetSendBuf(int server_id);

        uint32_t broker_id() { return thread_id_; }

        void ReinitializeQPs(rdmaio::RdmaCtrl *rdma_ctrl);

        const std::vector<QPEndPoint> &end_points() {
            return end_points_;
        }

    private:
        uint32_t to_qp_idx(uint32_t server_id);

        void FlushSendsOnQP(int qp_idx);

        uint64_t
        PostRDMASEND(const char *localbuf, ibv_wr_opcode type, uint32_t size,
                     int qp_idx,
                     uint64_t local_offset,
                     uint64_t remote_addr, bool is_offset,
                     uint32_t imm_data);

        const uint32_t my_server_id_ = 0;
        const char *mr_buf_ = nullptr;
        const uint64_t mr_size_ = 0;
        const uint64_t rdma_port_ = 0;
        const uint32_t max_num_sends_ = 0;
        const uint32_t max_msg_size_ = 0;
        const uint32_t doorbell_batch_size_ = 0;

        const int thread_id_ = 0;
        const char *rdma_buf_ = nullptr;

        // RDMA variables
        int *server_qp_idx_map_;
        std::vector<QPEndPoint> end_points_;
        ibv_wc *wcs_ = nullptr;
        RCQP **qp_ = nullptr;
        char **rdma_send_buf_ = nullptr;
        char **rdma_recv_buf_ = nullptr;

        struct ibv_sge **send_sges_ = nullptr;
        ibv_send_wr **send_wrs_ = nullptr;

        // pending sends.
        int *send_sge_index_ = nullptr;
        int *npending_send_ = nullptr;
        int *psend_index_ = nullptr;
        RDMAMsgCallback *callback_ = nullptr;

        void InitializeQPs(RdmaCtrl *rdma_ctrl);

        void DestroyQPs(RdmaCtrl *rdma_ctrl);
    };
}

#endif //RLIB_NOVA_RDMA_RC_STORE_H
