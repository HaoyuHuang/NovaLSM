
//
// Created by Haoyu Huang on 12/15/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_WORKER_H
#define LEVELDB_WORKER_H

#include <vector>

#include "port/port.h"
#include "port/thread_annotations.h"

namespace nova {

    class Request {

    };

    class Worker {
    public:
        Worker() : worker_signal_(&mutex_) {

        }

        void AddRequest(Request *request);

        void WakeupForWork();

        void Run();

        virtual void DoWork(Request *request) = 0;

    private:
        leveldb::port::Mutex request_queue_mutex_;
        std::vector<Request *> request_queue_ GUARDED_BY(request_queue_mutex_);

        leveldb::port::CondVar worker_signal_;
        leveldb::port::Mutex mutex_;
    };
}

#endif //LEVELDB_WORKER_H
