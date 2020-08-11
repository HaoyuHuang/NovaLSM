
//
// Created by Haoyu Huang on 3/28/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
// Client request worker based on libevent.

#ifndef CLIENT_REQ_WORKER_H
#define CLIENT_REQ_WORKER_H


#include <event.h>
#include <cstring>
#include <thread>
#include <atomic>
#include <chrono>

#include "rdma/rdma_msg_callback.h"
#include "rdma/nova_rdma_broker.h"
#include "common/nova_common.h"
#include "common/nova_config.h"
#include "common/nova_mem_manager.h"
#include "leveldb/db.h"
#include "rdma_msg_handler.h"
#include "log/logc_log_writer.h"


namespace nova {

    void event_handler(int fd, short which, void *arg);

    struct Stats {
        uint64_t nreqs = 0;
        uint64_t nresponses = 0;
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

        uint64_t nscans = 0;

        uint64_t nreplicate_log_records = 0;

        uint64_t nremove_log_records = 0;

        uint64_t nreqs_to_poll_rdma = 0;

        Stats diff(const Stats &other) {
            Stats diff{};
            diff.nreqs = nreqs - other.nreqs;
            diff.nresponses = nresponses - other.nresponses;
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
            diff.nscans = nscans - other.nscans;
            return diff;
        }
    };

    struct DBAsyncWorkers {
        std::vector<RDMAMsgHandler *> workers;
    };

    class NICClientReqWorker {
    public:
        NICClientReqWorker(int thread_id)
                :
                thread_id_(thread_id) {
            NOVA_LOG(INFO) << "memstore[" << thread_id << "]: "
                           << "create conn thread :" << thread_id;
            rand_seed = thread_id;
            replicate_log_record_states = new leveldb::StoCReplicateLogRecordState[nova::NovaConfig::config->servers.size()];
            ResetReplicateState();

            request_buf = (char *) malloc(NovaConfig::config->max_msg_size);
            buf = (char *) malloc(NovaConfig::config->max_msg_size);
            NOVA_ASSERT(request_buf != NULL);
            NOVA_ASSERT(buf != NULL);

            memset(request_buf, 0, NovaConfig::config->max_msg_size);
            memset(buf, 0, NovaConfig::config->max_msg_size);

        }

        void Start();

        void ResetReplicateState() {
            for (int i = 0; i < nova::NovaConfig::config->servers.size(); i++) {
                replicate_log_record_states[i].cfgid = 0;
                replicate_log_record_states[i].result = leveldb::StoCReplicateLogRecordResult::REPLICATE_LOG_RECORD_NONE;
                replicate_log_record_states[i].rdma_wr_id = -1;
            }
        }

        timeval start{};
        timeval read_start{};
        timeval write_start{};
        int thread_id_ = 0;
        int listen_fd_ = -1;            /* listener descriptor      */
        int epoll_fd_ = -1;      /* used for all notification*/
        std::mutex mutex_;

        struct event_base *base = nullptr;
        rdmaio::RdmaCtrl *ctrl_;
        std::vector<nova::RDMAMsgCallback *> rdma_threads;
        leveldb::StocPersistentFileManager *stoc_file_manager_;
        std::vector<DBMigration *> db_migration_threads_;

        leveldb::StoCBlockClient *stoc_client_;
        NovaMemManager *mem_manager_;

        int nconns = 0;

        mutex conn_mu;
        vector<int> conn_queue;
        vector<Connection *> conns;
        Stats stats;
        Stats prev_stats;
        unsigned int rand_seed = 0;

        char *rdma_backing_mem = nullptr;
        uint32_t rdma_backing_mem_size = 0;
        leveldb::StoCReplicateLogRecordState *replicate_log_record_states;
        char *request_buf = nullptr;
        char *buf = nullptr;
        uint32_t req_ind = 0;
    };
}

#endif //CLIENT_REQ_WORKER_H
