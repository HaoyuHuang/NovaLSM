
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <malloc.h>
#include <fmt/core.h>
#include "nova_rdma_rc_store.h"

namespace nova {
    mutex open_device_mutex;
    bool is_device_opened = false;
    RNicHandler *device = nullptr;

    uint32_t NovaRDMARCStore::to_qp_idx(uint32_t server_id) {
        return server_id - end_points_[0].server_id;
    }

    void NovaRDMARCStore::Init(RdmaCtrl *rdma_ctrl) {
        RDMA_LOG(INFO) << "RDMA client thread " << thread_id_
                       << " initializing";
        RdmaCtrl::DevIdx idx{.dev_id = 0, .port_id = 1}; // using the first RNIC's first port
        char *cache_buf = NovaConfig::config->nova_buf;
        int num_servers = end_points_.size();
        int max_num_sends = NovaConfig::config->rdma_max_num_sends;
        int max_num_wrs = max_num_sends;
        int my_server_id = NovaConfig::config->my_server_id;
        uint64_t my_memory_id = my_server_id;

        open_device_mutex.lock();
        if (!is_device_opened) {
            device = rdma_ctrl->open_device(idx);
            is_device_opened = true;
            RDMA_ASSERT(
                    rdma_ctrl->register_memory(my_memory_id,
                                               cache_buf,
                                               NovaConfig::config->nnovabuf,
                                               device));
        }
        open_device_mutex.unlock();

        RDMA_LOG(INFO) << "rdma-rc[" << thread_id_ << "]: register bytes "
                       << NovaConfig::config->nnovabuf
                       << " my memory id: "
                       << my_memory_id;
        for (int peer_id = 0; peer_id < num_servers; peer_id++) {
            QPEndPoint peer_store = end_points_[peer_id];
            QPIdx my_rc_key = create_rc_idx(my_server_id, thread_id_,
                                            peer_store.server_id);
            QPIdx peer_rc_key = create_rc_idx(peer_store.server_id,
                                              peer_store.thread_id,
                                              my_server_id);
            uint64_t peer_memory_id = static_cast<uint64_t >(peer_store.server_id);
            RDMA_LOG(INFO) << "rdma-rc[" << thread_id_
                           << "]: my rc key " << my_rc_key.node_id << ":"
                           << my_rc_key.worker_id << ":" << my_rc_key.index;

            RDMA_LOG(INFO) << "rdma-rc[" << thread_id_
                           << "]: connecting to peer rc key "
                           << peer_store.host.ip << ":" << peer_rc_key.node_id
                           << ":" << peer_rc_key.worker_id << ":"
                           << peer_rc_key.index;
            MemoryAttr local_mr = rdma_ctrl->get_local_mr(
                    my_memory_id);
            ibv_cq *cq = rdma_ctrl->create_cq(
                    device, max_num_wrs);
            ibv_cq *recv_cq = rdma_ctrl->create_cq(
                    device, max_num_wrs);
            qp_[peer_id] = rdma_ctrl->create_rc_qp(my_rc_key,
                                                   device,
                                                   &local_mr,
                                                   cq, recv_cq);
            // get remote server's memory information
            MemoryAttr remote_mr;
            while (QP::get_remote_mr(peer_store.host.ip,
                                     NovaConfig::config->rdma_port,
                                     peer_memory_id, &remote_mr) != SUCC) {
                usleep(CONN_SLEEP);
            }
            qp_[peer_id]->bind_remote_mr(remote_mr);
            RDMA_LOG(INFO) << "rdma-rc[" << thread_id_
                           << "]: connect to server "
                           << peer_store.host.ip << ":" << peer_store.host.port
                           << ":" << peer_store.thread_id;
            // bind to the previous allocated mr
            while (qp_[peer_id]->connect(peer_store.host.ip,
                                         NovaConfig::config->rdma_port,
                                         peer_rc_key) != SUCC) {
                usleep(CONN_SLEEP);
            }
            RDMA_LOG(INFO)
                << fmt::format(
                        "rdma-rc[{}]: connected to server {}:{}:{}. Posting {} recvs.",
                        thread_id_, peer_store.host.ip,
                        peer_store.host.port, peer_store.thread_id,
                        max_num_sends);

            for (int i = 0; i < max_num_sends; i++) {
                PostRecv(peer_store.server_id, i);
            }
        }
        RDMA_LOG(INFO)
            << fmt::format("RDMA client thread {} initialized", thread_id_);
    }

    uint64_t
    NovaRDMARCStore::PostRDMASEND(const char *localbuf, ibv_wr_opcode opcode,
                                  uint32_t size,
                                  int server_id,
                                  uint64_t local_offset,
                                  uint64_t remote_addr, bool is_offset,
                                  uint32_t imm_data) {
        uint32_t qp_idx = to_qp_idx(server_id);
        int max_msg_size_ = NovaConfig::config->max_msg_size;
        int doorbell_batch_size_ = NovaConfig::config->rdma_doorbell_batch_size;
        int max_num_sends = NovaConfig::config->rdma_max_num_sends;

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
        if (ssge_idx + 1 < doorbell_batch_size_) {
            swr[ssge_idx].next = &swr[ssge_idx + 1];
        } else {
            swr[ssge_idx].next = NULL;
        }
        psend_index_[qp_idx]++;
        npending_send_[qp_idx]++;
        send_sge_index_[qp_idx]++;
        RDMA_LOG(DEBUG) << fmt::format(
                    "rdma-rc[{}]: SQ: rdma {} request to server {} wr:{} imm:{} roffset:{} isoff:{} size:{} p:{}:{}",
                    thread_id_, ibv_wr_opcode_str(opcode), server_id, wr_id,
                    imm_data,
                    remote_addr, is_offset, size, psend_index_[qp_idx],
                    npending_send_[qp_idx]);
        if (send_sge_index_[qp_idx] == doorbell_batch_size_) {
            // post send a batch of requests.
            send_sge_index_[qp_idx] = 0;
            ibv_send_wr *bad_sr;
            int ret = ibv_post_send(qp_[qp_idx]->qp_, &swr[0], &bad_sr);
            RDMA_ASSERT(ret == 0) << ret;
            RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: "
                            << "SQ: posting "
                            << doorbell_batch_size_
                            << " requests";
        }

        while (npending_send_[qp_idx] == max_num_sends) {
            // poll sq as it is full.
            PollSQ(server_id);
        }

        if (psend_index_[qp_idx] == max_num_sends) {
            psend_index_[qp_idx] = 0;
        }
        return wr_id;
    }

    uint64_t
    NovaRDMARCStore::PostRead(char *localbuf, uint32_t size, int server_id,
                              uint64_t local_offset,
                              uint64_t remote_addr, bool is_offset) {
        return PostRDMASEND(localbuf, IBV_WR_RDMA_READ, size, server_id,
                            local_offset,
                            remote_addr, is_offset, 0);
    }

    uint64_t
    NovaRDMARCStore::PostSend(const char *localbuf, uint32_t size,
                              int server_id,
                              uint32_t imm_data) {
        ibv_wr_opcode wr = IBV_WR_SEND;
        if (imm_data != 0) {
            wr = IBV_WR_SEND_WITH_IMM;
        }
        return PostRDMASEND(localbuf, wr, size, server_id, 0, 0, false,
                            imm_data);
    }

    void NovaRDMARCStore::FlushPendingSends(int server_id) {
        if (server_id == NovaConfig::config->my_server_id) {
            return;
        }
        uint32_t qp_idx = to_qp_idx(server_id);
        if (send_sge_index_[qp_idx] == 0) {
            return;
        }
        RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: "
                        << "flush pending sends "
                        << send_sge_index_[qp_idx];
        send_wrs_[qp_idx][send_sge_index_[qp_idx] - 1].next = NULL;
        send_sge_index_[qp_idx] = 0;
        ibv_send_wr *bad_sr;
        int ret = ibv_post_send(qp_[qp_idx]->qp_, &send_wrs_[qp_idx][0],
                                &bad_sr);
        RDMA_ASSERT(ret == 0) << ret;
    }


    void NovaRDMARCStore::FlushPendingSends() {
        for (int peer_id = 0; peer_id < end_points_.size(); peer_id++) {
            QPEndPoint peer_store = end_points_[peer_id];
            FlushPendingSends(peer_store.server_id);
        }
    }

    uint64_t
    NovaRDMARCStore::PostWrite(const char *localbuf, uint32_t size,
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

    uint32_t NovaRDMARCStore::PollSQ(int server_id) {
        if (server_id == NovaConfig::config->my_server_id) {
            return 0;
        }
        uint32_t qp_idx = to_qp_idx(server_id);
        int npending = npending_send_[qp_idx];
        if (npending == 0) {
            return 0;
        }

        int max_num_wrs = NovaConfig::config->rdma_max_num_sends;
        // FIFO.
        int n = ibv_poll_cq(qp_[qp_idx]->cq_, max_num_wrs, wcs_);
        for (int i = 0; i < n; i++) {
            RDMA_ASSERT(wcs_[i].status == IBV_WC_SUCCESS)
                << "rdma-rc[" << thread_id_ << "]: " << "SQ error wc status "
                << wcs_[i].status << " str:"
                << ibv_wc_status_str(wcs_[i].status);

            RDMA_LOG(DEBUG) << fmt::format(
                        "rdma-rc[{}]: SQ: poll complete from server {} wr:{} op:{}",
                        thread_id_, server_id, wcs_[i].wr_id,
                        ibv_wc_opcode_str(wcs_[i].opcode));
            char *buf = rdma_send_buf_[qp_idx] +
                        wcs_[i].wr_id * NovaConfig::config->max_msg_size;
            callback_->ProcessRDMAWC(wcs_[i].opcode, wcs_[i].wr_id, server_id,
                                     buf, wcs_[i].imm_data);
            npending_send_[qp_idx] -= 1;
        }
        return n;
    }

    uint32_t NovaRDMARCStore::PollSQ() {
        uint32_t size = 0;
        for (int peer_id = 0; peer_id < end_points_.size(); peer_id++) {
            QPEndPoint peer_store = end_points_[peer_id];
            size += PollSQ(peer_store.server_id);
        }
        return size;
    }

    void NovaRDMARCStore::PostRecv(int server_id, int recv_buf_index) {
        int msg_size = NovaConfig::config->max_msg_size;
        uint32_t qp_idx = to_qp_idx(server_id);
        auto ret = qp_[qp_idx]->post_recv(
                rdma_recv_buf_[qp_idx] + msg_size * recv_buf_index, msg_size,
                recv_buf_index);
        RDMA_ASSERT(ret == SUCC) << ret;
    }

    void NovaRDMARCStore::FlushPendingRecvs() {}

    uint32_t NovaRDMARCStore::PollRQ(int server_id) {
        int max_msg_size_ = NovaConfig::config->max_msg_size;
        int max_num_wrs = NovaConfig::config->rdma_max_num_sends;
        uint32_t qp_idx = to_qp_idx(server_id);
        int n = ibv_poll_cq(qp_[qp_idx]->recv_cq_, max_num_wrs, wcs_);
        for (int i = 0; i < n; i++) {
            uint64_t wr_id = wcs_[i].wr_id;
            RDMA_ASSERT(wr_id < max_num_wrs);
            RDMA_ASSERT(wcs_[i].status == IBV_WC_SUCCESS)
                << "rdma-rc[" << thread_id_ << "]: " << "RQ error wc status "
                << ibv_wc_status_str(wcs_[i].status);

            RDMA_LOG(DEBUG)
                << fmt::format("rdma-rc[{}]: RQ: received from server {} wr:{}",
                               thread_id_, server_id, wr_id);
            char *buf = rdma_recv_buf_[qp_idx] + max_msg_size_ * wr_id;
            callback_->ProcessRDMAWC(wcs_[i].opcode, wcs_[i].wr_id, server_id,
                                     buf, wcs_[i].imm_data);
            // Post another receive event.
            PostRecv(server_id, wr_id);
        }

        // Flush all pending send requests.
        FlushPendingSends(server_id);
        return n;
    }

    uint32_t NovaRDMARCStore::PollRQ() {
        uint32_t size = 0;
        for (int peer_id = 0; peer_id < end_points_.size(); peer_id++) {
            QPEndPoint peer_store = end_points_[peer_id];
            size += PollRQ(peer_store.server_id);
        }
        return size;
    }

    char *NovaRDMARCStore::GetSendBuf() {
        return NULL;
    }

    char *NovaRDMARCStore::GetSendBuf(int server_id) {
        uint32_t qp_idx = to_qp_idx(server_id);
        int max_msg_size_ = NovaConfig::config->max_msg_size;
        return rdma_send_buf_[qp_idx] +
               psend_index_[qp_idx] * max_msg_size_;
    }
}