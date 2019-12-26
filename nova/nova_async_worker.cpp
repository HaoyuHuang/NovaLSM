
//
// Created by Haoyu Huang on 12/25/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova_async_worker.h"
#include "nova_mem_config.h"
#include "nova_common.h"

namespace nova {
    void NovaAsyncWorker::AddTask(const NovaAsyncTask &task) {
        mutex_.Lock();
        queue_.push_back(task);
        mutex_.Unlock();
        semaphore_.notify();
    }

    void NovaAsyncWorker::Start() {
        RDMA_LOG(INFO) << "Async worker started";

        while (true) {
            semaphore_.wait();
            mutex_.Lock();
            std::list<NovaAsyncTask> queue(queue_.begin(), queue_.end());
            mutex_.Unlock();

            if (queue.empty()) {
                continue;
            }

            for (const NovaAsyncTask &task : queue) {
                uint64_t hv = NovaConfig::keyhash(task.key.data(),
                                                  task.key.size());
                Fragment *frag = NovaConfig::home_fragment(hv);
                leveldb::Status status = dbs_[frag->db_ids[0]]->Put(task.option,
                                                                    task.key,
                                                                    task.value);
                RDMA_LOG(DEBUG) << "############### Async worker process task "
                                << task.sock_fd
                                << ":" << task.key;
                RDMA_ASSERT(status.ok()) << status.ToString();
                char *response_buf = task.conn->buf;
                int nlen = 1;
                int len = int_to_str(response_buf, nlen);
                task.conn->response_buf = task.conn->buf;
                task.conn->response_size = len + nlen;
                RDMA_ASSERT(
                        socket_write_handler(task.sock_fd, task.conn) ==
                        COMPLETE);
                write_socket_complete(task.sock_fd, task.conn);
            }
//            RDMA_LOG(DEBUG) << "Async worker processed " << queue.size();
            mutex_.Lock();
            auto begin = queue_.begin();
            auto end = queue_.begin();
            std::advance(end, queue.size());
            queue_.erase(begin, end);
            mutex_.Unlock();
        }
    }
}