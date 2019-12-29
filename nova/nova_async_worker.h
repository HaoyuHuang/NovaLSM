
//
// Created by Haoyu Huang on 12/25/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_ASYNC_WORKER_H
#define LEVELDB_NOVA_ASYNC_WORKER_H

#include <string>
#include <port/port_stdcxx.h>
#include <leveldb/db.h>
#include "leveldb/options.h"
#include "nova_common.h"
#include "nova_msg_callback.h"
#include "nova_rdma_store.h"
#include <semaphore.h>
#include <list>
#include "log/nova_log.h"
#include "log/nic_log_writer.h"
#include "log/rdma_log_writer.h"

namespace nova {

#define RDMA_POLL_MIN_TIMEOUT_US 1000
#define RDMA_POLL_MAX_TIMEOUT_US 10000

    struct NovaAsyncTask {
        std::string key;
        std::string value;
        int sock_fd;
        Connection *conn;
    };

    struct NovaAsyncCompleteTask {
        int sock_fd;
        Connection *conn;
    };

    struct NovaAsyncCompleteQueue {
        std::list<NovaAsyncCompleteTask> queue;
        leveldb::port::Mutex mutex;
        int read_fd;
        int write_fd;
        struct event readevent;
    };

    class NovaAsyncWorker : public NovaMsgCallback {
    public:
        NovaAsyncWorker(const std::vector<leveldb::DB *> &dbs,
                        NovaAsyncCompleteQueue *cq) : dbs_(dbs), cq_(cq) {
            sem_init(&sem_, 0, 0);
        }

        void ProcessRDMAWC(ibv_wc_opcode type, int remote_server_id,
                           char *buf) override;

        void Start();

        void AddTask(const NovaAsyncTask &task);

        int size();

        void set_rdma_store(NovaRDMAStore *rdma_store) {
            rdma_store_ = rdma_store;
        };

        leveldb::log::RDMALogWriter *rdma_log_writer_ = nullptr;
        leveldb::log::NICLogWriter *nic_log_writer_ = nullptr;
        LogFileManager *log_manager_ = nullptr;
    private:
        int ProcessQueue();

//        void ProcessRDMAREAD(int remote_server_id, char *buf);

//        void ProcessRDMAGETResponse(uint64_t to_sock_fd,
//                                    DataEntry *entry, bool fetch_from_origin);
//
//        void
//        PostRDMAGETRequest(int fd, char *key, uint64_t nkey, int home_server,
//                           uint64_t remote_offset, uint64_t remote_size);
//
//        void
//        PostRDMAGETIndexRequest(int fd, char *key, uint64_t nkey,
//                                int home_server,
//                                uint64_t remote_addr);

        NovaRDMAStore *rdma_store_ = nullptr;

        bool is_running_ = true;

        sem_t sem_;
        std::vector<leveldb::DB *> dbs_;
        leveldb::port::Mutex mutex_;
        std::list<NovaAsyncTask> queue_;
        NovaAsyncCompleteQueue *cq_;
    };
}


#endif //LEVELDB_NOVA_ASYNC_WORKER_H
