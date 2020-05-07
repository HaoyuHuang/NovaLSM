
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_MEM_SERVER_H
#define RLIB_NOVA_MEM_SERVER_H

#include "mc/nova_sstable.h"
#include "leveldb/db_types.h"
#include "mc/nova_mem_manager.h"
#include "nova_cc_conn_worker.h"
#include "nova/nova_config.h"
#include "nova/nova_rdma_store.h"
#include "nova/nova_rdma_rc_store.h"
#include "cc/nova_rdma_cc.h"
#include "leveldb/db.h"
#include "nova_cc.h"
#include "cc/db_compaction_thread.h"
#include "mc/nova_mc_wb_worker.h"
#include "nova_cc_stat_thread.h"

namespace nova {
    class NovaCCConnWorker;

    class NovaCCLoadThread {
    public:
        NovaCCLoadThread(std::vector<leveldb::DB *> &dbs,
                         std::vector<NovaRDMAComputeComponent *> &async_workers,
                         NovaMemManager *mem_manager,
                         std::set<uint32_t> &assigned_dbids, uint32_t tid);

        void Start();

        uint64_t throughput = 0;

        void VerifyLoad();

    private:
        uint64_t LoadDataWithRangePartition();

        std::vector<NovaRDMAComputeComponent *> async_workers_;
        NovaMemManager *mem_manager_;
        std::vector<leveldb::DB *> dbs_;
        uint32_t tid_;
        std::set<uint32_t> assigned_dbids_;
    };


    class NovaCCNICServer {
    public:
        NovaCCNICServer(RdmaCtrl *rdma_ctrl, char *rdmabuf, int nport);

        void Start();

        void SetupListener();

        void LoadData();

        int nport_;
        int listen_fd_ = -1;            /* listener descriptor      */

        std::vector<leveldb::DB *> dbs_;
        NovaMemManager *mem_manager;
        InMemoryLogFileManager *log_manager;

        std::vector<NovaCCConnWorker *> conn_workers;
        std::vector<NovaRDMAComputeComponent *> async_workers;
        std::vector<NovaRDMAComputeComponent *> async_compaction_workers;

        std::vector<NovaCCServerAsyncWorker *> cc_server_workers;
        std::vector<leveldb::EnvBGThread *> bgs;
        std::vector<leveldb::EnvBGThread *> reorg_bgs;
        std::vector<leveldb::EnvBGThread *> compaction_coord_bgs;

        NovaStatThread *stat_thread_;
        vector<std::thread> stats_t_;

        struct event_base *base;
        int current_conn_worker_id_;
        vector<thread> conn_worker_threads;
        vector<thread> cc_workers;
        vector<thread> compaction_workers;
        vector<thread> reorg_workers;
        vector<thread> compaction_coord_workers;
        std::vector<std::thread> cc_server_async_workers;
    };
}

#endif //RLIB_NOVA_MEM_SERVER_H
