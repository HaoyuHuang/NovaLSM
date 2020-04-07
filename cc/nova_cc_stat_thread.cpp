
//
// Created by Haoyu Huang on 3/30/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_cc_stat_thread.h"

namespace nova {
    void NovaStatThread::Start() {
        std::vector<uint32_t> foreground_rdma_tasks;
        std::vector<uint32_t> bg_rdma_tasks;

        struct StorageWorkerStats {
            uint32_t tasks = 0;
            uint64_t read_bytes = 0;
            uint64_t write_bytes = 0;
        };

        std::vector<StorageWorkerStats> stats;
        std::vector<uint32_t> compaction_stats;

        for (int i = 0; i < async_workers_.size(); i++) {
            foreground_rdma_tasks.push_back(async_workers_[i]->stat_tasks_);
        }

        for (int i = 0; i < async_compaction_workers_.size(); i++) {
            bg_rdma_tasks.push_back(async_compaction_workers_[i]->stat_tasks_);
        }

        for (int i = 0; i < bgs_.size(); i++) {
            compaction_stats.push_back(bgs_[i]->num_running_tasks());
        }

        for (int i = 0; i < cc_server_workers_.size(); i++) {
            StorageWorkerStats s = {};
            s.tasks = cc_server_workers_[i]->stat_tasks_;
            s.read_bytes = cc_server_workers_[i]->stat_read_bytes_;
            s.write_bytes = cc_server_workers_[i]->stat_write_bytes_;
            stats.push_back(s);
        }

        std::string output;
        int flushed_memtable_size[BUCKET_SIZE];
        while (true) {
            usleep(10000000);
            output = "frdma:";
            for (int i = 0; i < foreground_rdma_tasks.size(); i++) {
                uint32_t tasks = async_workers_[i]->stat_tasks_;
                output += std::to_string(tasks - foreground_rdma_tasks[i]);
                output += ",";
                foreground_rdma_tasks[i] = tasks;
            }
            output += "\n";

            output += "brdma:";
            for (int i = 0; i < bg_rdma_tasks.size(); i++) {
                uint32_t tasks = async_compaction_workers_[i]->stat_tasks_;
                output += std::to_string(tasks - bg_rdma_tasks[i]);
                output += ",";
                bg_rdma_tasks[i] = tasks;
            }
            output += "\n";

            output += "compaction:";
            for (int i = 0; i < compaction_stats.size(); i++) {
                uint32_t tasks = bgs_[i]->num_running_tasks();
                output += std::to_string(tasks - compaction_stats[i]);
                output += ",";
                compaction_stats[i] = tasks;
            }
            output += "\n";

            output += "storage:";
            for (int i = 0; i < cc_server_workers_.size(); i++) {
                uint32_t tasks = cc_server_workers_[i]->stat_tasks_;
                output += std::to_string(tasks - stats[i].tasks);
                output += ",";
                stats[i].tasks = tasks;
            }
            output += "\n";

            output += "storage-read:";
            for (int i = 0; i < cc_server_workers_.size(); i++) {
                uint32_t tasks = cc_server_workers_[i]->stat_read_bytes_;
                output += std::to_string(tasks - stats[i].read_bytes);
                output += ",";
                stats[i].read_bytes = tasks;
            }
            output += "\n";

            output += "storage-write:";
            for (int i = 0; i < cc_server_workers_.size(); i++) {
                uint32_t tasks = cc_server_workers_[i]->stat_write_bytes_;
                output += std::to_string(tasks - stats[i].write_bytes);
                output += ",";
                stats[i].write_bytes = tasks;
            }
            output += "\n";

            output += "active-memtables:";
            for (int i = 0; i < dbs_.size(); i++) {
                output += std::to_string(dbs_[i]->number_of_active_memtables_);
                output += ",";
            }
            output += "\n";

            output += "immutable-memtables:";
            for (int i = 0; i < dbs_.size(); i++) {
                output += std::to_string(
                        dbs_[i]->number_of_immutable_memtables_);
                output += ",";
            }
            output += "\n";

            output += "steals:";
            for (int i = 0; i < dbs_.size(); i++) {
                output += std::to_string(dbs_[i]->number_of_steals_);
                output += ",";
            }
            output += "\n";

            output += "puts:";
            for (int i = 0; i < dbs_.size(); i++) {
                output += std::to_string(dbs_[i]->processed_writes_);
                output += ",";
            }
            output += "\n";

            output += "wait-due-to-contention:";
            for (int i = 0; i < dbs_.size(); i++) {
                output += std::to_string(
                        dbs_[i]->number_of_wait_due_to_contention_);
                output += ",";
            }
            output += "\n";

            output += "gets:";
            for (int i = 0; i < dbs_.size(); i++) {
                output += std::to_string(dbs_[i]->number_of_gets_);
                output += ",";
            }
            output += "\n";

            output += "hits:";
            for (int i = 0; i < dbs_.size(); i++) {
                output += std::to_string(dbs_[i]->number_of_memtable_hits_);
                output += ",";
            }
            output += "\n";

            for (int j = 0; j < BUCKET_SIZE; j++) {
                flushed_memtable_size[j] = 0;
            }
            output += "memtable-hist:";
            for (int i = 0; i < bgs_.size(); i++) {
                for (int j = 0; j < BUCKET_SIZE; j++) {
                    flushed_memtable_size[j] += bgs_[i]->memtable_size[j];
                }
            }

            for (int j = 0; j < BUCKET_SIZE; j++) {
                output += std::to_string(flushed_memtable_size[j]);
                output += ",";
            }
            output += "\n";

            output += "puts-no-wait:";
            for (int i = 0; i < dbs_.size(); i++) {
                output += std::to_string(dbs_[i]->number_of_puts_no_wait_);
                output += ",";
            }
            output += "\n";

            output += "puts-wait:";
            for (int i = 0; i < dbs_.size(); i++) {
                output += std::to_string(dbs_[i]->number_of_puts_wait_);
                output += ",";
            }
            output += "\n";

            RDMA_LOG(INFO) << fmt::format("stats: \n{}", output);
            output.clear();
        }
    }
}