
//
// Created by Haoyu Huang on 3/30/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "stat_thread.h"

namespace nova {
    namespace {
        struct Stats {
            uint32_t min = UINT32_MAX;
            uint32_t max = 0;
            uint32_t median = 0;

            std::string DebugString() {
                return fmt::format("{},{},{}", min, max, median);
            }
        };

        Stats
        compute_stats(const std::vector<leveldb::OverlappingStats> &data) {
            Stats stats = {};
            if (data.empty()) {
                stats.min = 0;
            }

            for (int i = 0; i < data.size(); i++) {
                stats.min = std::min(data[i].num_overlapping_tables, stats.min);
                stats.max = std::max(data[i].num_overlapping_tables, stats.max);
            }
            if (!data.empty()) {
                stats.median = data[data.size() / 2].num_overlapping_tables;
            }
            return stats;
        }
    }

    void NovaStatThread::Initialize(
            std::vector<nova::NovaStatThread::StorageWorkerStats> *storage_stats,
            const std::vector<nova::StorageWorker *> &storage_workers) {
        for (int i = 0; i < storage_workers.size(); i++) {
            StorageWorkerStats s = {};
            s.tasks = storage_workers[i]->stat_tasks_;
            s.read_bytes = storage_workers[i]->stat_read_bytes_;
            s.write_bytes = storage_workers[i]->stat_write_bytes_;
            storage_stats->push_back(s);
        }
    }

    void NovaStatThread::OutputStats(const std::string &prefix,
                                     std::string *output,
                                     std::vector<nova::NovaStatThread::StorageWorkerStats> *storage_stats,
                                     const std::vector<nova::StorageWorker *> &storage_workers) {
        output->append(prefix + "-storage,");
        for (int i = 0; i < storage_workers.size(); i++) {
            uint32_t tasks = storage_workers[i]->stat_tasks_;
            output->append(std::to_string(tasks - (*storage_stats)[i].tasks));
            output->append(",");
            (*storage_stats)[i].tasks = tasks;
        }
        output->append("\n");
        output->append(prefix + "-storage-read,");
        for (int i = 0; i < storage_workers.size(); i++) {
            uint32_t tasks = storage_workers[i]->stat_read_bytes_;
            output->append(
                    std::to_string(tasks - (*storage_stats)[i].read_bytes));
            output->append(",");
            (*storage_stats)[i].read_bytes = tasks;
        }
        output->append("\n");
        output->append(prefix + "-storage-write,");
        for (int i = 0; i < storage_workers.size(); i++) {
            uint32_t tasks = storage_workers[i]->stat_write_bytes_;
            output->append(
                    std::to_string(tasks - (*storage_stats)[i].write_bytes));
            output->append(",");
            (*storage_stats)[i].write_bytes = tasks;
        }
        output->append("\n");
    }

    void NovaStatThread::Start() {
        std::vector<uint32_t> foreground_rdma_tasks;
        std::vector<uint32_t> bg_rdma_tasks;

        std::vector<StorageWorkerStats> fg_storage_stats;
        std::vector<StorageWorkerStats> bg_storage_stats;
        std::vector<StorageWorkerStats> compaction_storage_stats;
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
        Initialize(&fg_storage_stats, fg_storage_workers_);
        Initialize(&bg_storage_stats, bg_storage_workers_);
        Initialize(&compaction_storage_stats, compaction_storage_workers_);

        std::string output;
        int flushed_memtable_size[BUCKET_SIZE];
        while (true) {
            sleep(10);

            std::vector<leveldb::DB *> dbs;
            Configuration *cfg = NovaConfig::config->cfgs[NovaConfig::config->current_cfg_id];
            for (auto frag : cfg->fragments) {
                auto db = reinterpret_cast<leveldb::DB *>(frag->db);
                if (db) {
                    dbs.push_back(db);
                }
            }

            output = "frdma,";
            for (int i = 0; i < foreground_rdma_tasks.size(); i++) {
                uint32_t tasks = async_workers_[i]->stat_tasks_;
                output += std::to_string(tasks - foreground_rdma_tasks[i]);
                output += ",";
                foreground_rdma_tasks[i] = tasks;
            }
            output += "\n";

            output += "brdma,";
            for (int i = 0; i < bg_rdma_tasks.size(); i++) {
                uint32_t tasks = async_compaction_workers_[i]->stat_tasks_;
                output += std::to_string(tasks - bg_rdma_tasks[i]);
                output += ",";
                bg_rdma_tasks[i] = tasks;
            }
            output += "\n";

            output += "compaction,";
            for (int i = 0; i < compaction_stats.size(); i++) {
                uint32_t tasks = bgs_[i]->num_running_tasks();
                output += std::to_string(tasks - compaction_stats[i]);
                output += ",";
                compaction_stats[i] = tasks;
            }
            output += "\n";

            OutputStats("fg", &output, &fg_storage_stats, fg_storage_workers_);
            OutputStats("bg", &output, &bg_storage_stats, bg_storage_workers_);
            OutputStats("c", &output, &compaction_storage_stats,
                        compaction_storage_workers_);

            output += "active-memtables,";
            for (int i = 0; i < dbs.size(); i++) {
                output += std::to_string(dbs[i]->number_of_active_memtables_);
                output += ",";
            }
            output += "\n";

            output += "immutable-memtables,";
            for (int i = 0; i < dbs.size(); i++) {
                output += std::to_string(
                        dbs[i]->number_of_immutable_memtables_);
                output += ",";
            }
            output += "\n";

            output += "steals,";
            for (int i = 0; i < dbs.size(); i++) {
                output += std::to_string(dbs[i]->number_of_steals_);
                output += ",";
            }
            output += "\n";

            output += "puts,";
            for (int i = 0; i < dbs.size(); i++) {
                output += std::to_string(dbs[i]->processed_writes_);
                output += ",";
            }
            output += "\n";

            output += "wait-due-to-contention,";
            for (int i = 0; i < dbs.size(); i++) {
                output += std::to_string(
                        dbs[i]->number_of_wait_due_to_contention_);
                output += ",";
            }
            output += "\n";

            output += "gets,";
            for (int i = 0; i < dbs.size(); i++) {
                output += std::to_string(dbs[i]->number_of_gets_);
                output += ",";
            }
            output += "\n";

            output += "hits,";
            for (int i = 0; i < dbs.size(); i++) {
                output += std::to_string(dbs[i]->number_of_memtable_hits_);
                output += ",";
            }
            output += "\n";

            output += "scans,";
            for (int i = 0; i < dbs.size(); i++) {
                output += fmt::format("[{}]",
                                      dbs[i]->scan_stats.DebugString());
                output += ",";
            }
            output += "\n";

            output += "searched_file_per_miss,";
            for (int i = 0; i < dbs.size(); i++) {
                double miss = dbs[i]->number_of_gets_ -
                              dbs[i]->number_of_memtable_hits_;
                double files = dbs[i]->number_of_files_to_search_for_get_;
                output += std::to_string(files / miss);
                output += ",";
            }
            output += "\n";

            for (int j = 0; j < BUCKET_SIZE; j++) {
                flushed_memtable_size[j] = 0;
            }
            output += "memtable-hist,";
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

            output += "puts-no-wait,";
            for (int i = 0; i < dbs.size(); i++) {
                output += std::to_string(dbs[i]->number_of_puts_no_wait_);
                output += ",";
            }
            output += "\n";

            output += "puts-wait,";
            for (int i = 0; i < dbs.size(); i++) {
                output += std::to_string(dbs[i]->number_of_puts_wait_);
                output += ",";
            }
            output += "\n";

            // report overlapping sstables.
            leveldb::DBStats aggregated_stats = {};
            uint32_t size_dist[BUCKET_SIZE];
            aggregated_stats.sstable_size_dist = size_dist;

            for (int j = 0; j < BUCKET_SIZE; j++) {
                size_dist[j] = 0;
            }

            for (int i = 0; i < dbs.size(); i++) {
                output += "db-overlapping-sstable-stats-" + std::to_string(i) +
                          ",";
                leveldb::DBStats stats = {};
                uint32_t size_dist[BUCKET_SIZE];
                for (int j = 0; j < BUCKET_SIZE; j++) {
                    size_dist[j] = 0;
                }
                stats.sstable_size_dist = size_dist;
                dbs[i]->QueryDBStats(&stats);

                aggregated_stats.dbsize += stats.dbsize;
                aggregated_stats.num_l0_sstables += stats.num_l0_sstables;
                output += std::to_string(stats.num_l0_sstables);
                output += ",";
                uint32_t ideal_nsstables = stats.num_l0_sstables;
                if (nova::NovaConfig::config->enable_subrange) {
                    ideal_nsstables = stats.num_l0_sstables /
                                      nova::NovaConfig::config->num_memtable_partitions;
                    ideal_nsstables = std::max(ideal_nsstables, (uint32_t) 1);
                }
                output += std::to_string(ideal_nsstables);
                output += ",";

                Stats ostats = compute_stats(
                        stats.num_overlapping_sstables_per_table);
                output += ostats.DebugString();
                output += ",";

                uint32_t ideal_nsstables_since_last_query = stats.new_l0_sstables_since_last_query;
                if (nova::NovaConfig::config->enable_subrange) {
                    ideal_nsstables_since_last_query =
                            stats.new_l0_sstables_since_last_query /
                            nova::NovaConfig::config->num_memtable_partitions;
                    ideal_nsstables_since_last_query = std::max(
                            ideal_nsstables_since_last_query, (uint32_t) 1);
                }
                output += std::to_string(
                        stats.new_l0_sstables_since_last_query);
                output += ",";
                output += std::to_string(ideal_nsstables_since_last_query);
                output += ",";

                ostats = compute_stats(
                        stats.num_overlapping_sstables_per_table_since_last_query);
                output += ostats.DebugString();
                output += ",";
                output += std::to_string(
                        stats.load_imbalance.maximum_load_imbalance);
                output += ",";
                output += std::to_string(stats.load_imbalance.stdev);
                output += ",";
                // ideal load imbalance is 0.
                output += std::to_string(0);
                output += ",";
                output += std::to_string(stats.num_major_reorgs);
                output += ",";
                output += std::to_string(stats.num_skipped_major_reorgs);
                output += ",";
                output += std::to_string(stats.num_minor_reorgs);
                output += ",";
                output += std::to_string(stats.num_minor_reorgs_samples);
                output += ",";
                output += std::to_string(stats.num_minor_reorgs_for_dup);
                output += ",";
                output += std::to_string(stats.num_skipped_minor_reorgs);

                output += "\n";

                output += "db-size-stats-" + std::to_string(i) + ",";
                output += std::to_string(stats.dbsize / 1024 / 1024);
                output += ",";
                output += std::to_string(stats.num_l0_sstables);
                output += ",";
                for (int j = 0; j < BUCKET_SIZE; j++) {
                    size_dist[j] += stats.sstable_size_dist[j];
                    output += std::to_string(stats.sstable_size_dist[j]);
                    output += ",";
                }
                output += "\n";

                output += "db-overlap-overall-" + std::to_string(i) + ",";
                for (auto &overlap : stats.num_overlapping_sstables) {
                    output += overlap.DebugString();
                    output += ",";
                }
                output += "\n";

                output += "db-overlap-" + std::to_string(i) + ",";
                for (auto &overlap : stats.num_overlapping_sstables_since_last_query) {
                    output += overlap.DebugString();
                    output += ",";
                }
                output += "\n";
            }
            output += "db," + std::to_string(
                    aggregated_stats.dbsize / 1024 / 1024);
            output += ",";
            output += std::to_string(aggregated_stats.num_l0_sstables);
            output += ",";
            output += std::to_string(
                    nova::NovaGlobalVariables::global.generated_memtable_sizes);
            output += ",";
            output += std::to_string(
                    nova::NovaGlobalVariables::global.written_memtable_sizes);
            output += ",";
            output += std::to_string(
                    nova::NovaGlobalVariables::global.total_disk_reads);
            output += ",";
            output += std::to_string(
                    nova::NovaGlobalVariables::global.total_disk_writes);
            output += ",";
            for (int j = 0; j < BUCKET_SIZE; j++) {
                output += std::to_string(aggregated_stats.sstable_size_dist[j]);
                output += ",";
            }
            output += "\n";
            NOVA_LOG(INFO) << fmt::format("stats: \n{}", output);
            output.clear();
        }
    }
}