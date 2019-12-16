
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <malloc.h>
#include "nova_rdma_rc_store.h"

namespace nova {
    mutex open_device_mutex;
    bool is_device_opened = false;
    RNicHandler *device = nullptr;

    void NovaRDMARCStore::Init() {
        RDMA_LOG(INFO) << "RDMA client thread " << thread_id_ << " started";
        RdmaCtrl::DevIdx idx{.dev_id = 0, .port_id = 1}; // using the first RNIC's first port
        char *cache_buf = NovaConfig::config->nova_buf;
        int num_servers = NovaConfig::config->servers.size();
        int max_num_reads = NovaConfig::config->rdma_max_num_reads;
        int max_num_sends = NovaConfig::config->rdma_max_num_sends;
        int max_num_wrs = max_num_reads + max_num_sends;
        int my_server_id = NovaConfig::config->my_server_id;

        uint64_t my_memory_id = my_server_id;
//    RNicHandler *device = nullptr;

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

//    my_memory_id = static_cast<uint64_t >(my_server_id) << 32 |
//                            static_cast<uint64_t>(thread_id_);
//    device = NovaConfig::rdma_ctrl->open_thread_local_device(idx);
        RDMA_LOG(INFO) << "rdma-rc[" << thread_id_ << "]: register bytes "
                       << NovaConfig::config->nnovabuf
                       << " my memory id: "
                       << my_memory_id;
        for (int peer_sid = 0; peer_sid < num_servers; peer_sid++) {
            if (peer_sid == my_server_id) {
                continue;
            }
            Host peer_store = NovaConfig::config->servers[peer_sid];
            QPIdx my_rc_key = create_rc_idx(my_server_id, thread_id_, peer_sid);
            QPIdx peer_rc_key = create_rc_idx(peer_sid, thread_id_,
                                              my_server_id);
//        uint64_t peer_memory_id = static_cast<uint64_t >(peer_sid) << 32 |
//                                  static_cast<uint64_t>(thread_id_);
            uint64_t peer_memory_id = static_cast<uint64_t >(peer_sid);
            RDMA_LOG(INFO) << "rdma-rc[" << thread_id_
                           << "]: get server memory id "
                           << peer_memory_id << " from "
                           << peer_store.ip << ":" << peer_store.port;
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
            while (QP::get_remote_mr(peer_store.ip,
                                     NovaConfig::config->rdma_port,
                                     peer_memory_id, &remote_mr) != SUCC) {
                usleep(CONN_SLEEP);
            }
            qp_[peer_sid]->bind_remote_mr(remote_mr);
            RDMA_LOG(INFO) << "rdma-rc[" << thread_id_
                           << "]: connect to server "
                           << peer_store.ip << ":" << peer_store.port;
            // bind to the previous allocated mr
            while (qp_[peer_sid]->connect(peer_store.ip,
                                          NovaConfig::config->rdma_port,
                                          peer_rc_key) != SUCC) {
                usleep(CONN_SLEEP);
            }
            RDMA_LOG(INFO) << "rdma-rc[" << thread_id_
                           << "]: connected to server "
                           << peer_store.ip << ":" << peer_store.port;
        }
    }

    void
    NovaRDMARCStore::PostRead(uint32_t size, int server_id,
                              uint64_t local_offset,
                              uint64_t remote_addr, bool is_offset) {
        int max_msg_size_ = NovaConfig::config->max_msg_size;
        int doorbell_batch_size_ = NovaConfig::config->rdma_doorbell_batch_size;
        int max_num_reads = NovaConfig::config->rdma_max_num_reads;

        uint64_t wr_id = pread_index_[server_id];
        char *sendbuf = rdma_read_buf_[server_id] + wr_id * max_msg_size_;
        int ssge_idx = send_sge_index_[server_id];
        ibv_sge *ssge = send_sges_[server_id];
        ibv_send_wr *swr = send_wrs_[server_id];
        ssge[ssge_idx].addr = (uintptr_t) sendbuf + local_offset;
        ssge[ssge_idx].length = size;
        ssge[ssge_idx].lkey = qp_[server_id]->local_mr_.key;
        swr[ssge_idx].wr_id = wr_id;
        swr[ssge_idx].sg_list = &ssge[ssge_idx];
        swr[ssge_idx].num_sge = 1;
        swr[ssge_idx].opcode = IBV_WR_RDMA_READ;
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
        pread_index_[server_id]++;
        npending_read_[server_id]++;
        send_sge_index_[server_id]++;
        RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: "
                        << "SQ: rdma request to " << server_id << " roffset:"
                        << remote_addr << " offset:" << is_offset << " p:"
                        << pread_index_[server_id] << ":"
                        << npending_read_[server_id] << " buf:" << sendbuf;
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

        while (npending_read_[server_id] == max_num_reads) {
            // poll sq as it is full.
            PollSQ(server_id);
        }

        if (pread_index_[server_id] == max_num_reads) {
            pread_index_[server_id] = 0;
        }
    }

    void NovaRDMARCStore::PostSend(uint32_t size, int server_id) {

    }

    void NovaRDMARCStore::FlushPendingSends() {
        for (int peer_sid = 0;
             peer_sid < NovaConfig::config->servers.size(); peer_sid++) {
            if (peer_sid == NovaConfig::config->my_server_id) {
                continue;
            }
            if (send_sge_index_[peer_sid] == 0) {
                continue;
            }
            RDMA_LOG(DEBUG) << "flush pending reads "
                            << send_sge_index_[peer_sid];
            send_wrs_[peer_sid][send_sge_index_[peer_sid] - 1].next = NULL;
            send_sge_index_[peer_sid] = 0;
            ibv_send_wr *bad_sr;
            int ret = ibv_post_send(qp_[peer_sid]->qp_, &send_wrs_[peer_sid][0],
                                    &bad_sr);
            RDMA_ASSERT(ret == 0) << ret;
        }
    }

    void NovaRDMARCStore::PostWrite(uint32_t size, int server_id,
                                    uint64_t remote_offset) {
        int max_msg_size_ = NovaConfig::config->max_msg_size;
        int doorbell_batch_size_ = NovaConfig::config->rdma_doorbell_batch_size;
        int max_num_writes = NovaConfig::config->rdma_max_num_sends;

        uint64_t wr_id = psend_index_[server_id];
        char *sendbuf = rdma_write_buf_[server_id] + wr_id * max_msg_size_;
        int ssge_idx = send_sge_index_[server_id];
        ibv_sge *ssge = send_sges_[server_id];
        ibv_send_wr *swr = send_wrs_[server_id];
        ssge[ssge_idx].addr = (uintptr_t) sendbuf;
        ssge[ssge_idx].length = size;
        ssge[ssge_idx].lkey = qp_[server_id]->local_mr_.key;
        swr[ssge_idx].wr_id = wr_id;
        swr[ssge_idx].sg_list = &ssge[ssge_idx];
        swr[ssge_idx].num_sge = 1;
        swr[ssge_idx].opcode = IBV_WR_RDMA_WRITE;
        swr[ssge_idx].send_flags = IBV_SEND_SIGNALED;
        swr[ssge_idx].wr.rdma.remote_addr =
                qp_[server_id]->remote_mr_.buf + remote_offset;
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
                        << "SQ: rdma request to " << server_id << " p:"
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

        while (npending_send_[server_id] == max_num_writes) {
            PollSQ(server_id);
        }

        if (psend_index_[server_id] == max_num_writes) {
            psend_index_[server_id] = 0;
        }
    }

    void NovaRDMARCStore::PollSQ(int peer_sid) {
        if (peer_sid == NovaConfig::config->my_server_id) {
            return;
        }

        int npending = npending_read_[peer_sid] + npending_send_[peer_sid];
        if (npending == 0) {
            return;
        }

        int max_num_wrs = NovaConfig::config->rdma_max_num_reads +
                          NovaConfig::config->rdma_max_num_sends;
        // FIFO.
        int n = ibv_poll_cq(qp_[peer_sid]->cq_, max_num_wrs, wcs_);
        for (int i = 0; i < n; i++) {
            RDMA_ASSERT(wcs_[i].status == IBV_WC_SUCCESS)
                << "rdma-rc[" << thread_id_ << "]: " << "SQ error wc status "
                << wcs_[i].status << " str:"
                << ibv_wc_status_str(wcs_[i].status);

            RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: "
                            << "SQ: poll send complete wr:" << wcs_[i].wr_id;
            if (wcs_[i].opcode == IBV_WC_RDMA_READ) {
                char *buf = rdma_read_buf_[peer_sid] +
                            wcs_[i].wr_id * NovaConfig::config->max_msg_size;
                callback_->ProcessRDMAREAD(buf);
                npending_read_[peer_sid] -= 1;
            }
        }
        if (n != 0) {
            RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: " << "SQ: p"
                            << pread_index_[peer_sid] << ":"
                            << npending_read_[peer_sid];
        }
    }

    void NovaRDMARCStore::PollSQ() {
        if (NovaConfig::config->mode == NovaRDMAMode::NORMAL) {
            return;
        }
        if (NovaConfig::config->my_server_id == 0) {
            return;
        }
        PollSQ(0);
    }

    void NovaRDMARCStore::PostRecv(uint64_t wr_id) {}

    void NovaRDMARCStore::FlushPendingRecvs() {}

    void NovaRDMARCStore::PollAndProcessRQ() {
//    int max_msg_size_ = NovaConfig::config->max_msg_size;
//    int max_num_wrs_ = NovaConfig::config->max_num_wrs;
//
//    int n = ibv_poll_cq(qp_->recv_cq_, max_num_wrs_, wcs_);
//    for (int i = 0; i < n; i++) {
//        uint64_t wr_id = wcs_[i].wr_id;
//        char *buf = rdma_buf_ + wr_id * max_msg_size_;
//        buf += GRH_SIZE;
//        RDMA_ASSERT(wcs_[i].status == IBV_WC_SUCCESS)
//            << "rdma[" << thread_id_ << "]: " << "RQ error wc status " << ibv_wc_status_str(wcs_[i].status);
//        RDMA_ASSERT(wcs_[i].opcode == IBV_WC_RECV)
//            << "rdma[" << thread_id_ << "]: " << "RQ wrong op code " << wcs_[i].opcode;
//        if (buf[0] == API_REQ_GET) {
//            int from_server_id = 0;
//            int from_sock_fd = 0;
//            int key = 0;
//            ParseRDMAGetRequest(buf, &from_server_id, &from_sock_fd, &key);
//            RDMA_LOG(DEBUG) << "rdma[" << thread_id_ << "]: " << "RQ: received request " << from_server_id << ":"
//                            << from_sock_fd << ":" << key;
//            PostRecv(wr_id);
//            // process the request.
//            char *val = NULL;
//            int nval = 0;
//            callback_->ProcessRDMAGetRequest(from_server_id, from_sock_fd, key, val, &nval);
//            char *sendbuf = GetSendBuf();
//            GenerateRDMAGetResponse(sendbuf, from_server_id, from_sock_fd, key, val, nval);
//            PostSend(DEBUG_VALUE_SIZE, from_server_id);
//        } else if (buf[0] == API_REPLY_GET) {
//            int to_server_id = 0;
//            int to_sock_fd = 0;
//            int key = 0;
//            char *val = NULL;
//            int nval = 0;
//            ParseRDMAGetResponse(buf, &to_server_id, &to_sock_fd, &key, val, &nval);
//            RDMA_LOG(DEBUG) << "rdma[" << thread_id_ << "]: " << "RQ: received response " << to_server_id << ":"
//                            << to_sock_fd << ":" << key;
//            PostRecv(wr_id);
//            // process the response.
//            callback_->ProcessRDMAGetResponse(to_server_id, to_sock_fd, key, val, nval);
//        }
//    }
//    FlushPendingSends();
//    FlushPendingRecvs();
    }

    char *NovaRDMARCStore::GetSendBuf() {
        return NULL;
    }

    char *NovaRDMARCStore::GetSendBuf(int server_id) {
        int max_msg_size_ = NovaConfig::config->max_msg_size;
        return rdma_read_buf_[server_id] +
               pread_index_[server_id] * max_msg_size_;
    }
}