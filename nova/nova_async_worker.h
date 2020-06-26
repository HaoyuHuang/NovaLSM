
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
#include "nova_rdma_broker.h"
#include <semaphore.h>
#include <list>
#include "log/nova_log.h"
#include "log/nic_log_writer.h"
#include "log/rdma_log_writer.h"

namespace nova {

#define RDMA_POLL_MIN_TIMEOUT_US 10
#define RDMA_POLL_MAX_TIMEOUT_US 100

    struct NovaAsyncTask {
        RequestType type;
        int conn_worker_id;
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
                        NovaAsyncCompleteQueue **cqs) : dbs_(dbs), cqs_(cqs) {
            sem_init(&sem_, 0, 0);
            conn_workers_ = new bool[NovaConfig::config->num_conn_workers];
        }

        bool IsInitialized();

        void ProcessRDMAWC(ibv_wc_opcode type, int remote_server_id,
                           char *buf) override;

        void Start();

        void AddTask(const NovaAsyncTask &task);

        int size();

        void set_rdma_store(NovaRDMABroker *rdma_store) {
            rdma_store_ = rdma_store;
        };

        leveldb::log::RDMALogWriter *rdma_log_writer_ = nullptr;
        leveldb::log::NICLogWriter *nic_log_writer_ = nullptr;
        LogFileManager *log_manager_ = nullptr;
        std::vector<NovaClientSock *> socks_;

    private:
        void ProcessGet(const NovaAsyncTask &task);

        void ProcessPut(const NovaAsyncTask &task);

        void ProcessReplicateLogRecords(const NovaAsyncTask &task);

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

        NovaRDMABroker *rdma_store_ = nullptr;

        bool is_running_ = false;


        sem_t sem_;
        std::vector<leveldb::DB *> dbs_;
        leveldb::port::Mutex mutex_;
        std::list<NovaAsyncTask> queue_;
        NovaAsyncCompleteQueue **cqs_;
        bool *conn_workers_;
    };
}


#endif //LEVELDB_NOVA_ASYNC_WORKER_H
