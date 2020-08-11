
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef NIC_SERVER_H
#define NIC_SERVER_H

#include "leveldb/db_types.h"
#include "common/nova_mem_manager.h"
#include "client_req_worker.h"
#include "common/nova_config.h"
#include "rdma/nova_rdma_broker.h"
#include "rdma/nova_rdma_rc_broker.h"
#include "rdma_msg_handler.h"
#include "leveldb/db.h"
#include "ltc/stoc_file_client_impl.h"
#include "ltc/compaction_thread.h"
#include "ltc/stat_thread.h"
#include "ltc/db_migration.h"
#include "lsm_tree_cleaner.h"

namespace nova {
    class NICClientReqWorker;

    class LoadThread {
    public:
        LoadThread(std::vector<RDMAMsgHandler *> &async_workers,
                   NovaMemManager *mem_manager,
                   std::set<uint32_t> &assigned_frags, uint32_t tid);

        void Start();

        uint64_t throughput = 0;

        void VerifyLoad();

    private:
        uint64_t LoadDataWithRangePartition();

        std::vector<RDMAMsgHandler *> async_workers_;
        NovaMemManager *mem_manager_;
        uint32_t tid_;
        std::set<uint32_t> assigned_frags_;
    };


    class NICServer {
    public:
        NICServer(RdmaCtrl *rdma_ctrl, char *rdmabuf, int nport);

        void Start();

        void SetupListener();

        void LoadData();

        int nport_;
        int listen_fd_ = -1;            /* listener descriptor      */

        std::vector<leveldb::DB *> dbs_;
        NovaMemManager *mem_manager;
        StoCInMemoryLogFileManager *log_manager;

        std::vector<NICClientReqWorker *> conn_workers;
        std::vector<RDMAMsgHandler *> fg_rdma_msg_handlers;
        std::vector<RDMAMsgHandler *> bg_rdma_msg_handlers;
        leveldb::LSMTreeCleaner *lsm_tree_cleaner_;

        std::vector<StorageWorker *> fg_storage_workers;
        std::vector<StorageWorker *> bg_storage_workers;
        std::vector<StorageWorker *> compaction_storage_workers;
        std::vector<leveldb::EnvBGThread *> bg_compaction_threads;
        std::vector<leveldb::EnvBGThread *> bg_flush_memtable_threads;
        std::vector<DBMigration *> db_migration_threads;

        NovaStatThread *stat_thread_;

        vector<std::thread> stats_t_;
        struct event_base *base;
        int current_conn_worker_id_;
        vector<thread> conn_worker_threads;
        vector<thread> fg_rdma_workers;
        vector<thread> compaction_workers;
        vector<thread> reorg_workers;
        vector<thread> compaction_coord_workers;
        vector<thread> db_migrate_workers;
        std::vector<std::thread> storage_worker_threads;
    };
}
#endif //NIC_SERVER_H
