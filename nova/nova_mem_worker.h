
//
// Created by Haoyu Huang on 3/28/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_MEM_STORE_H
#define RLIB_NOVA_MEM_STORE_H


#include <event.h>
#include <cstring>
#include <thread>
#include <atomic>
#include <chrono>
#include <log/rdma_log_writer.h>
#include <log/nic_log_writer.h>

#include "nova_mem_server.h"
#include "nova_msg_callback.h"
#include "nova_rdma_store.h"
#include "nova_common.h"
#include "linked_list.h"
#include "nova_mem_config.h"
#include "mc/nova_mem_manager.h"
#include "leveldb/db.h"
#include "nova_async_worker.h"


namespace nova {

    class NovaMemServer;

    void event_handler(int fd, short which, void *arg);

    void rdma_timer_event_handler(int fd, short event, void *arg);

    struct Stats {
        uint64_t nreqs = 0;
        uint64_t nreads = 0;
        uint64_t nreadsagain = 0;
        uint64_t nwrites = 0;
        uint64_t nwritesagain = 0;
        uint64_t service_time = 0;
        uint64_t read_service_time = 0;
        uint64_t write_service_time = 0;

        uint64_t ngets = 0;
        uint64_t nget_hits = 0;
        uint64_t nget_lc = 0;
        uint64_t nget_lc_hits = 0;

        uint64_t nget_rdma = 0;
        uint64_t nget_rdma_stale = 0;
        uint64_t nget_rdma_invalid = 0;

        uint64_t ngetindex_rdma = 0;
        uint64_t ngetindex_rdma_invalid = 0;
        uint64_t ngetindex_rdma_indirect = 0;

        uint64_t nputs = 0;
        uint64_t nput_lc = 0;

        uint64_t nranges = 0;

        uint64_t nreqs_to_poll_rdma = 0;

        Stats diff(const Stats &other) {
            Stats diff{};
            diff.nreqs = nreqs - other.nreqs;
            diff.nreads = nreads - other.nreads;
            diff.nreadsagain = nreadsagain - other.nreadsagain;
            diff.nwrites = nwrites - other.nwrites;
            diff.nwritesagain = nwritesagain - other.nwritesagain;
            diff.ngets = ngets - other.ngets;
            diff.nget_hits = nget_hits - other.nget_hits;
            diff.nget_lc = nget_lc - other.nget_lc;
            diff.nget_lc_hits = nget_lc_hits - other.nget_lc_hits;
            diff.nget_rdma = nget_rdma - other.nget_rdma;
            diff.nget_rdma_stale = nget_rdma_stale - other.nget_rdma_stale;
            diff.nget_rdma_invalid =
                    nget_rdma_invalid - other.nget_rdma_invalid;
            diff.ngetindex_rdma = ngetindex_rdma - other.ngetindex_rdma;
            diff.ngetindex_rdma_invalid =
                    ngetindex_rdma_invalid - other.ngetindex_rdma_invalid;
            diff.ngetindex_rdma_indirect =
                    ngetindex_rdma_indirect - other.ngetindex_rdma_indirect;
            diff.nputs = nputs - other.nputs;
            diff.nput_lc = nput_lc - other.nput_lc;
            diff.nranges = nranges - other.nranges;
            return diff;
        }
    };

    class NovaMemWorker : public NovaMsgCallback {
    public:
        NovaMemWorker(int store_id, int thread_id, NovaMemServer *server)
                : store_id_(store_id),
                  thread_id_(thread_id), mem_server_(server) {
            nservers = NovaConfig::config->servers.size();
            RDMA_LOG(INFO) << "memstore[" << thread_id << "]: " << "create "
                           << store_id << ":" << thread_id << ":";
            int fd[2];
            pipe(fd);
            async_queue_.read_fd = fd[0];
            async_queue_.write_fd = fd[1];
        }

        void Start();

        void ProcessRDMAWC(ibv_wc_opcode type, int remote_server_id,
                           char *buf) override;

        void ProcessRDMAREAD(int remote_server_id, char *buf);

        void ProcessRDMAGETResponse(uint64_t to_sock_fd,
                                    DataEntry *entry, bool fetch_from_origin);

        void
        PostRDMAGETRequest(int fd, char *key, uint64_t nkey, int home_server,
                           uint64_t remote_offset, uint64_t remote_size);

        void
        PostRDMAGETIndexRequest(int fd, char *key, uint64_t nkey,
                                int home_server,
                                uint64_t remote_addr);

        void set_rdma_store(NovaRDMAStore *rdma_store) {
            rdma_store_ = rdma_store;
        };

        void set_mem_manager(NovaMemManager *mem_manager) {
            mem_manager_ = mem_manager;
        };

        void set_dbs(const std::vector<leveldb::DB *> &dbs) {
            dbs_ = dbs;
        }

        timeval start{};
        timeval read_start{};
        timeval write_start{};
        int store_id_ = 0;
        int thread_id_ = 0;
        int listen_fd_ = -1;            /* listener descriptor      */
        int epoll_fd_ = -1;      /* used for all notification*/
        int rr_server_redirect_reqs = 0;
        std::mutex mutex_;

        NovaMemServer *mem_server_ = nullptr;

        NovaMemManager *mem_manager_ = nullptr;

        std::vector<leveldb::DB *> dbs_;

        NovaRDMAStore *rdma_store_ = nullptr;
        struct event_base *base = nullptr;

        leveldb::log::RDMALogWriter *rdma_log_writer_ = nullptr;
        LogFileManager *log_manager_ = nullptr;

        NovaAsyncWorker *async_worker_ = nullptr;
        NovaAsyncCompleteQueue async_queue_;
        leveldb::log::NICLogWriter *nic_log_writer_ = nullptr;
        std::vector<NovaClientSock *> socks_;

        int on_new_conn_send_fd = 0;
        int on_new_conn_recv_fd = 0;
        int nconns = 0;

        int nservers = 0;
        mutex conn_mu;
        vector<int> conn_queue;
        vector<Connection *> conns;
        Stats stats;
        Stats prev_stats;
    };
}

#endif //RLIB_NOVA_MEM_STORE_H
