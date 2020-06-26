
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <malloc.h>
#include "nova_rdma_rc_broker.h"

namespace nova {
    mutex open_device_mutex;
    bool is_device_opened = false;
    RNicHandler *device = nullptr;

    void NovaRDMARCBroker::Init() {
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
            device = NovaConfig::rdma_ctrl->open_device(idx);
            is_device_opened = true;
            RDMA_ASSERT(
                    NovaConfig::rdma_ctrl->register_memory(my_memory_id,
                                                           cache_buf,
                                                           NovaConfig::config->nnovabuf,
                                                           device));
        }
        open_device_mutex.unlock();

        RDMA_LOG(INFO) << "rdma-rc[" << thread_id_ << "]: register bytes "
                       << NovaConfig::config->nnovabuf
                       << " my memory id: "
                       << my_memory_id;
        for (int peer_sid = 0; peer_sid < num_servers; peer_sid++) {
            if (peer_sid == my_server_id) {
                continue;
            }
            QPEndPoint peer_store = end_points_[peer_sid];
            QPIdx my_rc_key = create_rc_idx(my_server_id, thread_id_, peer_sid);
            QPIdx peer_rc_key = create_rc_idx(peer_sid, peer_store.thread_id,
                                              my_server_id);
            uint64_t peer_memory_id = static_cast<uint64_t >(peer_sid);
            RDMA_LOG(INFO) << "rdma-rc[" << thread_id_
                           << "]: get server memory id "
                           << peer_memory_id << " from "
                           << peer_store.host.ip << ":" << peer_store.host.port
                           << ":" << peer_store.thread_id;
            MemoryAttr local_mr = NovaConfig::rdma_ctrl->get_local_mr(
                    my_memory_id);
            ibv_cq *cq = NovaConfig::rdma_ctrl->create_cq(
                    device, max_num_wrs);
            ibv_cq *recv_cq = NovaConfig::rdma_ctrl->create_cq(
                    device, max_num_wrs);
            qp_[peer_sid] = NovaConfig::rdma_ctrl->create_rc_qp(my_rc_key,
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
            qp_[peer_sid]->bind_remote_mr(remote_mr);
            RDMA_LOG(INFO) << "rdma-rc[" << thread_id_
                           << "]: connect to server "
                           << peer_store.host.ip << ":" << peer_store.host.port
                           << ":" << peer_store.thread_id;
            // bind to the previous allocated mr
            while (qp_[peer_sid]->connect(peer_store.host.ip,
                                          NovaConfig::config->rdma_port,
                                          peer_rc_key) != SUCC) {
                usleep(CONN_SLEEP);
            }
            RDMA_LOG(INFO) << "rdma-rc[" << thread_id_
                           << "]: connected to server "
                           << peer_store.host.ip << ":" << peer_store.host.port
                           << ":" << peer_store.thread_id;

            for (int i = 0; i < max_num_sends; i++) {
                PostRecv(peer_sid, i);
            }
        }
        RDMA_LOG(INFO) << "RDMA client thread " << thread_id_ << " initialized";
    }

    void NovaRDMARCBroker::PostRDMASEND(char *localbuf, ibv_wr_opcode opcode,
                                        uint32_t size,
                                        int server_id,
                                        uint64_t local_offset,
                                        uint64_t remote_addr, bool is_offset) {
        int max_msg_size_ = NovaConfig::config->max_msg_size;
        int doorbell_batch_size_ = NovaConfig::config->rdma_doorbell_batch_size;
        int max_num_sends = NovaConfig::config->rdma_max_num_sends;

        uint64_t wr_id = psend_index_[server_id];
        char *sendbuf = rdma_send_buf_[server_id] + wr_id * max_msg_size_;
        if (localbuf != nullptr) {
            sendbuf = localbuf;
        }
        int ssge_idx = send_sge_index_[server_id];
        ibv_sge *ssge = send_sges_[server_id];
        ibv_send_wr *swr = send_wrs_[server_id];
        ssge[ssge_idx].addr = (uintptr_t) sendbuf + local_offset;
        ssge[ssge_idx].length = size;
        ssge[ssge_idx].lkey = qp_[server_id]->local_mr_.key;
        swr[ssge_idx].wr_id = wr_id;
        swr[ssge_idx].sg_list = &ssge[ssge_idx];
        swr[ssge_idx].num_sge = 1;
        swr[ssge_idx].opcode = opcode;
        swr[ssge_idx].send_flags = IBV_SEND_SIGNALED;
        if (is_offset) {
            swr[ssge_idx].wr.rdma.remote_addr =
                    qp_[server_id]->remote_mr_.buf + remote_addr;
        } else {
            swr[ssge_idx].wr.rdma.remote_addr = remote_addr;
        }
        swr[ssge_idx].wr.rdma.rkey = qp_[server_id]->remote_mr_.key;
        if (ssge_idx + 1 < doorbell_batch_size_) {
            swr[ssge_idx].next = &swr[ssge_idx + 1];
        } else {
            swr[ssge_idx].next = NULL;
        }
        psend_index_[server_id]++;
        npending_send_[server_id]++;
        send_sge_index_[server_id]++;
        RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: "
                        << "SQ: rdma " << ibv_wr_opcode_str(opcode)
                        << " request to " << server_id
                        << " roffset:"
                        << remote_addr << " offset:" << is_offset << " p:"
                        << psend_index_[server_id] << ":"
                        << npending_send_[server_id] << " buf:" << sendbuf;
        if (send_sge_index_[server_id] == doorbell_batch_size_) {
            // post send a batch of requests.
            send_sge_index_[server_id] = 0;
            ibv_send_wr *bad_sr;
            int ret = ibv_post_send(qp_[server_id]->qp_, &swr[0], &bad_sr);
            RDMA_ASSERT(ret == 0) << ret;
            RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: "
                            << "SQ: posting "
                            << doorbell_batch_size_
                            << " requests";
        }

        while (npending_send_[server_id] == max_num_sends) {
            // poll sq as it is full.
            PollSQ(server_id);
        }

        if (psend_index_[server_id] == max_num_sends) {
            psend_index_[server_id] = 0;
        }
    }

    void
    NovaRDMARCBroker::PostRead(char *localbuf, uint32_t size, int server_id,
                               uint64_t local_offset,
                               uint64_t remote_addr, bool is_offset) {
        PostRDMASEND(localbuf, IBV_WR_RDMA_READ, size, server_id, local_offset,
                     remote_addr, is_offset);
    }

    void
    NovaRDMARCBroker::PostSend(char *localbuf, uint32_t size, int server_id) {
        PostRDMASEND(localbuf, IBV_WR_SEND, size, server_id, 0, 0, false);
    }

    void NovaRDMARCBroker::FlushPendingSends(int peer_sid) {
        if (peer_sid == NovaConfig::config->my_server_id) {
            return;
        }
        if (send_sge_index_[peer_sid] == 0) {
            return;
        }
        RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: "
                        << "flush pending sends "
                        << send_sge_index_[peer_sid];
        send_wrs_[peer_sid][send_sge_index_[peer_sid] - 1].next = NULL;
        send_sge_index_[peer_sid] = 0;
        ibv_send_wr *bad_sr;
        int ret = ibv_post_send(qp_[peer_sid]->qp_, &send_wrs_[peer_sid][0],
                                &bad_sr);
        RDMA_ASSERT(ret == 0) << ret;
    }


    void NovaRDMARCBroker::FlushPendingSends() {
        for (int peer_sid = 0;
             peer_sid < NovaConfig::config->servers.size(); peer_sid++) {
            FlushPendingSends(peer_sid);
        }
    }

    void
    NovaRDMARCBroker::PostWrite(char *localbuf, uint32_t size, int server_id,
                                uint64_t remote_offset, bool is_remote_offset) {
        PostRDMASEND(localbuf, IBV_WR_RDMA_WRITE, size, server_id, 0,
                     remote_offset, is_remote_offset);
    }

    void NovaRDMARCBroker::PollSQ(int peer_sid) {
        if (peer_sid == NovaConfig::config->my_server_id) {
            return;
        }

        int npending = npending_send_[peer_sid];
        if (npending == 0) {
            return;
        }

        int max_num_wrs = NovaConfig::config->rdma_max_num_sends;
        // FIFO.
        int n = ibv_poll_cq(qp_[peer_sid]->cq_, max_num_wrs, wcs_);
        for (int i = 0; i < n; i++) {
            RDMA_ASSERT(wcs_[i].status == IBV_WC_SUCCESS)
                << "rdma-rc[" << thread_id_ << "]: " << "SQ error wc status "
                << wcs_[i].status << " str:"
                << ibv_wc_status_str(wcs_[i].status);

            RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: "
                            << "SQ: poll complete from " << peer_sid
                            << " wr:" << wcs_[i].wr_id << " op:"
                            << ibv_wc_opcode_str(wcs_[i].opcode);
            char *buf = rdma_send_buf_[peer_sid] +
                        wcs_[i].wr_id * NovaConfig::config->max_msg_size;
            callback_->ProcessRDMAWC(wcs_[i].opcode, peer_sid, buf);
            npending_send_[peer_sid] -= 1;
        }
//        if (n != 0) {
//            RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: " << "SQ: p"
//                            << psend_index_[peer_sid] << ":"
//                            << npending_send_[peer_sid];
//        }
    }

    void NovaRDMARCBroker::PollSQ() {
        for (int peer_sid = 0;
             peer_sid < NovaConfig::config->servers.size(); peer_sid++) {
            if (peer_sid == NovaConfig::config->my_server_id) {
                continue;
            }
            PollSQ(peer_sid);
        }
    }

    void NovaRDMARCBroker::PostRecv(int peer_sid, int recv_buf_index) {
        int msg_size = NovaConfig::config->max_msg_size;
        auto ret = qp_[peer_sid]->post_recv(
                rdma_recv_buf_[peer_sid] + msg_size * recv_buf_index, msg_size,
                recv_buf_index);
        RDMA_ASSERT(ret == SUCC) << ret;
    }

    void NovaRDMARCBroker::FlushPendingRecvs() {}

    void NovaRDMARCBroker::PollRQ(int peer_sid) {
        int max_msg_size_ = NovaConfig::config->max_msg_size;
        int max_num_wrs = NovaConfig::config->rdma_max_num_sends;
        int n = ibv_poll_cq(qp_[peer_sid]->recv_cq_, max_num_wrs, wcs_);
        for (int i = 0; i < n; i++) {
            uint64_t wr_id = wcs_[i].wr_id;
            if (wcs_[i].opcode != IBV_WC_RECV) {
                continue;
            }
            RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: "
                            << "RQ received from " << peer_sid
                            << " wr:" << wr_id;
            RDMA_ASSERT(wr_id < max_num_wrs);
            char *buf = rdma_recv_buf_[peer_sid] + max_msg_size_ * wr_id;
            RDMA_ASSERT(wcs_[i].status == IBV_WC_SUCCESS)
                << "rdma-rc[" << thread_id_ << "]: " << "RQ error wc status "
                << ibv_wc_status_str(wcs_[i].status);
            RDMA_ASSERT(wcs_[i].opcode == IBV_WC_RECV)
                << "rdma-rc[" << thread_id_ << "]: " << "RQ wrong op code "
                << ibv_wc_opcode_str(wcs_[i].opcode);
            callback_->ProcessRDMAWC(IBV_WC_RECV, peer_sid, buf);
            // Post another receive event.
            PostRecv(peer_sid, wr_id);
        }
    }

    void NovaRDMARCBroker::PollRQ() {
        for (int peer_sid = 0;
             peer_sid < NovaConfig::config->servers.size(); peer_sid++) {
            if (peer_sid == NovaConfig::config->my_server_id) {
                continue;
            }
            PollRQ(peer_sid);
        }
    }

    char *NovaRDMARCBroker::GetSendBuf() {
        return NULL;
    }

    char *NovaRDMARCBroker::GetSendBuf(int server_id) {
        int max_msg_size_ = NovaConfig::config->max_msg_size;
        return rdma_send_buf_[server_id] +
               psend_index_[server_id] * max_msg_size_;
    }
}