
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
#include <semaphore.h>
#include <list>

namespace nova {
    struct NovaAsyncTask {
        std::string key;
        std::string value;
        leveldb::WriteOptions option;
        int sock_fd;
        Connection *conn;
    };

    struct NovaAsyncCompleteTask {
        Connection *conn;
    };

    struct NovaAsyncCompleteQueue {
        std::list<NovaAsyncCompleteTask> queue;
        leveldb::port::Mutex mutex;
        int read_fd;
        int write_fd;
        struct event readevent;
    };

    class NovaAsyncWorker {
    public:
        NovaAsyncWorker(const std::vector<leveldb::DB *> &dbs,
                        NovaAsyncCompleteQueue *cq) : dbs_(dbs), cq_(cq) {
            sem_init(&sem_, 0, 0);
        }

        void Start();

        void AddTask(const NovaAsyncTask &task);

    private:
        sem_t sem_;
        std::vector<leveldb::DB *> dbs_;
        leveldb::port::Mutex mutex_;
        std::list<NovaAsyncTask> queue_;
        NovaAsyncCompleteQueue *cq_;
    };
}


#endif //LEVELDB_NOVA_ASYNC_WORKER_H
