
//
// Created by Haoyu Huang on 2/27/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "compaction_thread.h"

namespace leveldb {

    LTCCompactionThread::LTCCompactionThread(MemManager *mem_manager)
            : mem_manager_(mem_manager) {
        sem_init(&signal, 0, 0);
        for (int i = 0; i < BUCKET_SIZE; i++) {
            memtable_size[i] = 0;
        }
    }

    bool LTCCompactionThread::Schedule(const EnvBGTask &task) {
        background_work_mutex_.Lock();
        background_work_queue_.push_back(task);
        background_work_mutex_.Unlock();
        sem_post(&signal);
        return true;
    }

    bool LTCCompactionThread::IsInitialized() {
        background_work_mutex_.Lock();
        bool is_running = is_running_;
        background_work_mutex_.Unlock();
        return is_running;
    }

    uint32_t LTCCompactionThread::num_running_tasks() {
        return num_tasks_;
    }


    void LTCCompactionThread::Start() {
        nova::NovaConfig::config->add_tid_mapping();

        background_work_mutex_.Lock();
        is_running_ = true;
        background_work_mutex_.Unlock();

        rand_seed_ = thread_id_ + 100000;

        if (db_) {
            leveldb::DB *db = reinterpret_cast<leveldb::DB *>(db_);
            db->CoordinateMajorCompaction();
        }

        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("{} Compaction worker started.", thread_id_);
        while (is_running_) {
            sem_wait(&signal);

            background_work_mutex_.Lock();
            if (background_work_queue_.empty()) {
                background_work_mutex_.Unlock();
                continue;
            }

            std::vector<EnvBGTask> tasks(background_work_queue_);
            background_work_queue_.clear();
            background_work_mutex_.Unlock();

            num_tasks_ += tasks.size();

            bool reorg = false;
            for (auto &task : tasks) {
                if (task.memtable == nullptr &&
                    task.compaction_task == nullptr &&
                    !task.delete_obsolete_files) {
                    auto db = reinterpret_cast<DB *>(tasks[0].db);
                    db->PerformSubRangeReorganization();
                    reorg = true;
                    continue;
                }
            }

            if (reorg) {
                continue;
            }

            std::unordered_map<void *, std::vector<EnvBGTask>> db_tasks;
            for (auto &task : tasks) {
                db_tasks[task.db].push_back(task);
                if (task.memtable) {
                    memtable_size[task.memtable_size_mb] += 1;
                }
            }

            for (auto &it : db_tasks) {
                auto db = reinterpret_cast<DB *>(it.first);
                db->PerformCompaction(this, it.second);
            }
        }
    }
}