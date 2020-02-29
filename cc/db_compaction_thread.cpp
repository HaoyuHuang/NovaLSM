
//
// Created by Haoyu Huang on 2/27/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "db_compaction_thread.h"

namespace leveldb {

    NovaCCCompactionThread::NovaCCCompactionThread(MemManager *mem_manager)
            : mem_manager_(mem_manager) {
        sem_init(&signal, 0, 0);
    }

    void NovaCCCompactionThread::Schedule(const CompactionTask &task) {
        background_work_mutex_.Lock();
        background_work_queue_.emplace(task);
        num_tasks_ += 1;
        background_work_mutex_.Unlock();
        sem_post(&signal);
    }

    bool NovaCCCompactionThread::IsInitialized() {
        background_work_mutex_.Lock();
        bool is_running = is_running_;
        background_work_mutex_.Unlock();
        return is_running;
    }

    uint32_t NovaCCCompactionThread::num_running_tasks() {
        return num_tasks_;
    }

    void NovaCCCompactionThread::Start() {
        nova::NovaConfig::config->add_tid_mapping();

        background_work_mutex_.Lock();
        is_running_ = true;
        background_work_mutex_.Unlock();

        RDMA_LOG(rdmaio::INFO) << "Compaction workers started";
        while (is_running_) {
            sem_wait(&signal);

            background_work_mutex_.Lock();
            RDMA_ASSERT(!background_work_queue_.empty());

            auto task = background_work_queue_.front();
            background_work_queue_.pop();
            background_work_mutex_.Unlock();
            auto db = reinterpret_cast<DB *>(task.db);
            db->PerformCompaction(this);

            background_work_mutex_.Lock();
            num_tasks_ -= 1;
            background_work_mutex_.Unlock();
        }
    }
}