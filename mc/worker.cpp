
//
// Created by Haoyu Huang on 12/15/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "worker.h"

namespace nova {

    void Worker::AddRequest(Request *request) {
        request_queue_mutex_.Lock();
        request_queue_.push_back(request);
        request_queue_mutex_.Unlock();
    }

    void Worker::WakeupForWork() {
        worker_signal_.SignalAll();
    }

    void Worker::Run() {
        while (true) {
            worker_signal_.Wait();

            request_queue_mutex_.Lock();
            std::vector<Request *> request_queue = request_queue_;
            request_queue_mutex_.Unlock();

            for (auto it = request_queue.begin();
                 it != request_queue.end(); it++) {
                DoWork(*it);
                delete *it;
            }

            request_queue_mutex_.Lock();
            request_queue_.erase(request_queue_.begin(),
                                 request_queue_.begin() + request_queue.size());
            request_queue_mutex_.Unlock();
        }
    }

}