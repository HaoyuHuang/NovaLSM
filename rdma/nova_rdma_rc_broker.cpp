
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <malloc.h>
#include <fmt/core.h>
#include "nova_rdma_rc_broker.h"

namespace nova {
    mutex open_device_mutex;
    bool is_device_opened = false;
    RNicHandler *device = nullptr;

    uint32_t NovaRDMARCBroker::to_qp_idx(uint32_t server_id) {
        NOVA_ASSERT(server_qp_idx_map_[server_id] != -1);
        return server_qp_idx_map_[server_id];
    }

    void NovaRDMARCBroker::Init(RdmaCtrl *rdma_ctrl) {
        NOVA_LOG(INFO) << "RDMA client thread " << thread_id_
                       << " initializing";
        RdmaCtrl::DevIdx idx{.dev_id = 0, .port_id = 1}; // using the first RNIC's first port
        const char *cache_buf = mr_buf_;
        uint64_t my_memory_id = my_server_id_;

        open_device_mutex.lock();
        if (!is_device_opened) {
            device = rdma_ctrl->open_device(idx);
            is_device_opened = true;
            NOVA_ASSERT(
                    rdma_ctrl->register_memory(my_memory_id,
                                               cache_buf,
                                               mr_size_,
                                               device));
        }
        open_device_mutex.unlock();

        NOVA_LOG(INFO) << "rdma-rc[" << thread_id_ << "]: register bytes "
                       << mr_size_
                       << " my memory id: "
                       << my_memory_id;
        InitializeQPs(rdma_ctrl);
        NOVA_LOG(INFO)
            << fmt::format("RDMA client thread {} initialized", thread_id_);
    }

    void NovaRDMARCBroker::ReinitializeQPs(rdmaio::RdmaCtrl *rdma_ctrl) {
        DestroyQPs(rdma_ctrl);
        InitializeQPs(rdma_ctrl);
    }

    void NovaRDMARCBroker::DestroyQPs(rdmaio::RdmaCtrl *rdma_ctrl) {
        int num_servers = end_points_.size();
        for (int i = 0; i < num_servers; i++) {
            npending_send_[i] = 0;
            psend_index_[i] = 0;
            send_sge_index_[i] = 0;
            QPEndPoint peer_store = end_points_[i];
            QPIdx my_rc_key = create_rc_idx(my_server_id_, thread_id_,
                                            peer_store.server_id);
            rdma_ctrl->destroy_rc_qp(my_rc_key);
            qp_[i] = nullptr;
        }
    }

    void NovaRDMARCBroker::InitializeQPs(RdmaCtrl *rdma_ctrl) {
        uint64_t my_memory_id = my_server_id_;
        int num_servers = end_points_.size();

        for (int peer_id = 0; peer_id < num_servers; peer_id++) {
            QPEndPoint peer_store = end_points_[peer_id];
            QPIdx my_rc_key = create_rc_idx(my_server_id_, thread_id_,
                                            peer_store.server_id);
            QPIdx peer_rc_key = create_rc_idx(peer_store.server_id,
                                              peer_store.thread_id,
                                              my_server_id_);
            uint64_t peer_memory_id = static_cast<uint64_t >(peer_store.server_id);
            NOVA_LOG(DEBUG) << "rdma-rc[" << thread_id_
                            << "]: my rc key " << my_rc_key.node_id << ":"
                            << my_rc_key.worker_id << ":" << my_rc_key.index;

            NOVA_LOG(DEBUG) << "rdma-rc[" << thread_id_
                            << "]: connecting to peer rc key "
                            << peer_store.host.ip << ":" << peer_rc_key.node_id
                            << ":" << peer_rc_key.worker_id << ":"
                            << peer_rc_key.index;
            MemoryAttr local_mr = rdma_ctrl->get_local_mr(
                    my_memory_id);
            ibv_cq *cq = rdma_ctrl->create_cq(
                    device, max_num_sends_);
            ibv_cq *recv_cq = rdma_ctrl->create_cq(
                    device, max_num_sends_);
            qp_[peer_id] = rdma_ctrl->create_rc_qp(my_rc_key,
                                                   device,
                                                   &local_mr,
                                                   cq, recv_cq);
            // get remote server's memory information
            MemoryAttr remote_mr = {};
            while (QP::get_remote_mr(peer_store.host.ip,
                                     rdma_port_,
                                     peer_memory_id, &remote_mr) != SUCC) {
                usleep(CONN_SLEEP);
            }
            qp_[peer_id]->bind_remote_mr(remote_mr);
            NOVA_LOG(INFO) << "rdma-rc[" << thread_id_
                           << "]: connect to server "
                           << peer_store.host.ip << ":" << peer_store.host.port
                           << ":" << peer_store.thread_id;
            // bind to the previous allocated mr
            while (qp_[peer_id]->connect(peer_store.host.ip,
                                         rdma_port_,
                                         peer_rc_key) != SUCC) {
                usleep(CONN_SLEEP);
            }
            NOVA_LOG(INFO)
                << fmt::format(
                        "rdma-rc[{}]: connected to server {}:{}:{}. Posting {} recvs.",
                        thread_id_, peer_store.host.ip,
                        peer_store.host.port, peer_store.thread_id,
                        max_num_sends_);

            for (int i = 0; i < max_num_sends_; i++) {
                PostRecv(peer_store.server_id, i);
            }
        }
    }

    uint64_t
    NovaRDMARCBroker::PostRDMASEND(const char *localbuf, ibv_wr_opcode opcode,
                                   uint32_t size,
                                   int server_id,
                                   uint64_t local_offset,
                                   uint64_t remote_addr, bool is_offset,
                                   uint32_t imm_data) {
        uint32_t qp_idx = to_qp_idx(server_id);
        uint64_t wr_id = psend_index_[qp_idx];
        const char *sendbuf = rdma_send_buf_[qp_idx] + wr_id * max_msg_size_;
        if (localbuf != nullptr) {
            sendbuf = localbuf;
        }
        int ssge_idx = send_sge_index_[qp_idx];
        ibv_sge *ssge = send_sges_[qp_idx];
        ibv_send_wr *swr = send_wrs_[qp_idx];
        ssge[ssge_idx].addr = (uintptr_t) sendbuf + local_offset;
        ssge[ssge_idx].length = size;
        ssge[ssge_idx].lkey = qp_[qp_idx]->local_mr_.key;
        swr[ssge_idx].wr_id = wr_id;
        swr[ssge_idx].sg_list = &ssge[ssge_idx];
        swr[ssge_idx].num_sge = 1;
        swr[ssge_idx].opcode = opcode;
        swr[ssge_idx].imm_data = imm_data;
        swr[ssge_idx].send_flags = IBV_SEND_SIGNALED;
        if (is_offset) {
            swr[ssge_idx].wr.rdma.remote_addr =
                    qp_[qp_idx]->remote_mr_.buf + remote_addr;
        } else {
            swr[ssge_idx].wr.rdma.remote_addr = remote_addr;
        }
        swr[ssge_idx].wr.rdma.rkey = qp_[qp_idx]->remote_mr_.key;
        swr[ssge_idx].next = NULL;
        psend_index_[qp_idx]++;
        npending_send_[qp_idx]++;
        send_sge_index_[qp_idx]++;
        NOVA_LOG(DEBUG) << fmt::format(
                    "rdma-rc[{}]: SQ: rdma {} request to server {} wr:{} imm:{} roffset:{} isoff:{} size:{} p:{}:{}",
                    thread_id_, ibv_wr_opcode_str(opcode), server_id, wr_id,
                    imm_data,
                    remote_addr, is_offset, size, psend_index_[qp_idx],
                    npending_send_[qp_idx]);
        FlushSendsOnQP(qp_idx);
        NOVA_ASSERT(npending_send_[qp_idx] <= max_num_sends_);

        if (psend_index_[qp_idx] == max_num_sends_) {
            psend_index_[qp_idx] = 0;
        }
        return wr_id;
    }

    uint64_t
    NovaRDMARCBroker::PostRead(char *localbuf, uint32_t size, int server_id,
                               uint64_t local_offset,
                               uint64_t remote_addr, bool is_offset) {
        return PostRDMASEND(localbuf, IBV_WR_RDMA_READ, size, server_id,
                            local_offset,
                            remote_addr, is_offset, 0);
    }

    uint64_t
    NovaRDMARCBroker::PostSend(const char *localbuf, uint32_t size,
                               int server_id,
                               uint32_t imm_data) {
        ibv_wr_opcode wr = IBV_WR_SEND_WITH_IMM;
        NOVA_ASSERT(size < max_msg_size_)
            << fmt::format("{} {} {}", localbuf[0], size, max_msg_size_);
        return PostRDMASEND(localbuf, wr, size, server_id, 0, 0, false,
                            imm_data);
    }

    void NovaRDMARCBroker::FlushSendsOnQP(int qp_idx) {
        if (send_sge_index_[qp_idx] == 0) {
            return;
        }
        NOVA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: "
                        << "flush pending sends "
                        << send_sge_index_[qp_idx];
        send_wrs_[qp_idx][send_sge_index_[qp_idx] - 1].next = NULL;
        send_sge_index_[qp_idx] = 0;
        ibv_send_wr *bad_sr;
        int ret = ibv_post_send(qp_[qp_idx]->qp_, &send_wrs_[qp_idx][0],
                                &bad_sr);
        NOVA_ASSERT(ret == 0) << ret;
    }

    void NovaRDMARCBroker::FlushPendingSends(int server_id) {
        uint32_t qp_idx = to_qp_idx(server_id);
        FlushSendsOnQP(qp_idx);
    }

    void NovaRDMARCBroker::FlushPendingSends() {
        for (int peer_id = 0; peer_id < end_points_.size(); peer_id++) {
            QPEndPoint peer_store = end_points_[peer_id];
            FlushPendingSends(peer_store.server_id);
        }
    }

    uint64_t
    NovaRDMARCBroker::PostWrite(const char *localbuf, uint32_t size,
                                int server_id,
                                uint64_t remote_offset, bool is_remote_offset,
                                uint32_t imm_data) {
        ibv_wr_opcode wr = IBV_WR_RDMA_WRITE;
        if (imm_data != 0) {
            wr = IBV_WR_RDMA_WRITE_WITH_IMM;
        }
        return PostRDMASEND(localbuf, wr, size, server_id, 0,
                            remote_offset, is_remote_offset, imm_data);
    }

    uint32_t NovaRDMARCBroker::PollSQ(int server_id, uint32_t *new_requests) {
        uint32_t qp_idx = to_qp_idx(server_id);
        int npending = npending_send_[qp_idx];
        if (npending == 0) {
            return 0;
        }

        // FIFO.
        int n = ibv_poll_cq(qp_[qp_idx]->cq_, max_num_sends_, wcs_);
        bool generate_new_request = false;
        for (int i = 0; i < n; i++) {
            NOVA_ASSERT(wcs_[i].status == IBV_WC_SUCCESS)
                << "rdma-rc[" << thread_id_ << "]: " << "SQ error wc status "
                << wcs_[i].status << " str:"
                << ibv_wc_status_str(wcs_[i].status) << " serverid "
                << server_id;

            NOVA_LOG(DEBUG) << fmt::format(
                        "rdma-rc[{}]: SQ: poll complete from server {} wr:{} op:{}",
                        thread_id_, server_id, wcs_[i].wr_id,
                        ibv_wc_opcode_str(wcs_[i].opcode));
            char *buf = rdma_send_buf_[qp_idx] +
                        wcs_[i].wr_id * max_msg_size_;
            callback_->ProcessRDMAWC(wcs_[i].opcode, wcs_[i].wr_id, server_id,
                                     buf, wcs_[i].imm_data,
                                     &generate_new_request);
            if (generate_new_request) {
                (*new_requests)++;
            }
            // Send is complete.
            buf[0] = 0;
            buf[1] = 0;
            npending_send_[qp_idx] -= 1;
        }
        return n;
    }

    void NovaRDMARCBroker::PostRecv(int server_id, int recv_buf_index) {
        uint32_t qp_idx = to_qp_idx(server_id);
        char *local_buf =
                rdma_recv_buf_[qp_idx] + max_msg_size_ * recv_buf_index;
        local_buf[0] = 0;
        local_buf[1] = 0;
        auto ret = qp_[qp_idx]->post_recv(local_buf, max_msg_size_,
                                          recv_buf_index);
        NOVA_ASSERT(ret == SUCC) << ret;
    }

    void NovaRDMARCBroker::FlushPendingRecvs() {}

    uint32_t NovaRDMARCBroker::PollRQ(int server_id, uint32_t *new_requests) {
        uint32_t qp_idx = to_qp_idx(server_id);
        int n = ibv_poll_cq(qp_[qp_idx]->recv_cq_, max_num_sends_, wcs_);
        bool generate_new_request = false;
        for (int i = 0; i < n; i++) {
            uint64_t wr_id = wcs_[i].wr_id;
            NOVA_ASSERT(wr_id < max_num_sends_);
            NOVA_ASSERT(wcs_[i].status == IBV_WC_SUCCESS)
                << "rdma-rc[" << thread_id_ << "]: " << "RQ error wc status "
                << ibv_wc_status_str(wcs_[i].status);

            NOVA_LOG(DEBUG)
                << fmt::format(
                        "rdma-rc[{}]: RQ: received from server {} wr:{} imm:{}",
                        thread_id_, server_id, wr_id, wcs_[i].imm_data);
            char *buf = rdma_recv_buf_[qp_idx] + max_msg_size_ * wr_id;
            callback_->ProcessRDMAWC(wcs_[i].opcode, wcs_[i].wr_id, server_id,
                                     buf, wcs_[i].imm_data, &generate_new_request);
            if (generate_new_request) {
                (*new_requests)++;
            }
            // Post another receive event.
            PostRecv(server_id, wr_id);
        }
        return n;
    }

    char *NovaRDMARCBroker::GetSendBuf() {
        return nullptr;
    }

    char *NovaRDMARCBroker::GetSendBuf(int server_id) {
        uint32_t qp_idx = to_qp_idx(server_id);
        return rdma_send_buf_[qp_idx] +
               psend_index_[qp_idx] * max_msg_size_;
    }
}