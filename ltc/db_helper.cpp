
//
// Created by Haoyu Huang on 6/21/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "db_helper.h"

#include <fmt/core.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/syscall.h>
#include "leveldb/cache.h"

#include "leveldb/write_batch.h"
#include "db/filename.h"
#include "ltc/stoc_file_client_impl.h"
#include "util/env_posix.h"

#include "leveldb/db_types.h"
#include "common/nova_mem_manager.h"
#include "common/nova_config.h"
#include "rdma/nova_rdma_broker.h"
#include "rdma/nova_rdma_rc_broker.h"
#include "leveldb/db.h"
#include "ltc/stoc_file_client_impl.h"
#include "ltc/compaction_thread.h"
#include "ltc/stat_thread.h"


namespace leveldb {
    leveldb::Options
    BuildDBOptions(int cfg_id, int db_index, leveldb::Cache *cache,
                   leveldb::MemTablePool *memtable_pool,
                   leveldb::MemManager *mem_manager,
                   leveldb::StoCClient *stoc_client,
                   const std::vector<leveldb::EnvBGThread *> &bg_compaction_threads,
                   const std::vector<leveldb::EnvBGThread *> &bg_flush_memtable_threads,
                   leveldb::EnvBGThread *reorg_thread,
                   leveldb::EnvBGThread *compaction_coord_thread,
                   leveldb::Env *env) {
        leveldb::Options options;
        options.enable_detailed_stats = nova::NovaConfig::config->enable_detailed_db_stats;
        options.block_cache = cache;
        options.memtable_pool = memtable_pool;
        if (nova::NovaConfig::config->memtable_size_mb > 0) {
            options.write_buffer_size = (uint64_t) (nova::NovaConfig::config->memtable_size_mb) * 1024 * 1024;
        }
        if (nova::NovaConfig::config->sstable_size > 0) {
            options.max_file_size = nova::NovaConfig::config->sstable_size;
        }
        if (nova::NovaConfig::config->client_access_pattern == "uniform") {
            options.client_access_pattern = ClientAccessPattern::kClientAccessUniform;
        } else {
            options.client_access_pattern = ClientAccessPattern::kClientAccessSkewed;
        }
        options.mem_manager = mem_manager;
        options.stoc_client = stoc_client;
        options.num_memtable_partitions = nova::NovaConfig::config->num_memtable_partitions;
        options.num_memtables = nova::NovaConfig::config->num_memtables;
        options.l0bytes_start_compaction_trigger = nova::NovaConfig::config->l0_start_compaction_mb * 1024 * 1024;
        options.l0bytes_stop_writes_trigger = nova::NovaConfig::config->l0_stop_write_mb * 1024 * 1024;
        options.max_open_files = 100000;
        options.enable_lookup_index = nova::NovaConfig::config->enable_lookup_index;
        options.enable_range_index = nova::NovaConfig::config->enable_range_index;
        options.num_recovery_thread = nova::NovaConfig::config->number_of_recovery_threads;
        options.num_compaction_threads = bg_flush_memtable_threads.size();
        options.max_stoc_file_size = std::max(options.write_buffer_size, options.max_file_size) +
                                     LEVELDB_TABLE_PADDING_SIZE_MB * 1024 * 1024;
        options.env = env;
        options.create_if_missing = true;
        options.compression = leveldb::kNoCompression;
        options.filter_policy = leveldb::NewBloomFilterPolicy(10);
        options.bg_compaction_threads = bg_compaction_threads;
        options.bg_flush_memtable_threads = bg_flush_memtable_threads;
        options.enable_tracing = false;
        options.comparator = new YCSBKeyComparator();
        if (nova::NovaConfig::config->memtable_type == "pool") {
            options.memtable_type = leveldb::MemTableType::kMemTablePool;
        } else {
            options.memtable_type = leveldb::MemTableType::kStaticPartition;
        }
        options.enable_subranges = nova::NovaConfig::config->enable_subrange;
        options.subrange_reorg_sampling_ratio = 1.0;
        options.reorg_thread = reorg_thread;
        options.compaction_coordinator_thread = compaction_coord_thread;
        options.enable_flush_multiple_memtables = nova::NovaConfig::config->enable_flush_multiple_memtables;
        options.max_num_sstables_in_nonoverlapping_set = nova::NovaConfig::config->major_compaction_max_tables_in_a_set;
        options.max_num_coordinated_compaction_nonoverlapping_sets = nova::NovaConfig::config->major_compaction_max_parallism;
        options.enable_subrange_reorg = nova::NovaConfig::config->enable_subrange_reorg;
        options.level = nova::NovaConfig::config->level;
        if (nova::NovaConfig::config->major_compaction_type == "no") {
            options.major_compaction_type = leveldb::MajorCompactionType::kMajorDisabled;
        } else if (nova::NovaConfig::config->major_compaction_type == "st") {
            options.major_compaction_type = leveldb::MajorCompactionType::kMajorSingleThreaded;
        } else if (nova::NovaConfig::config->major_compaction_type == "lc") {
            options.major_compaction_type = leveldb::MajorCompactionType::kMajorCoordinated;
        } else if (nova::NovaConfig::config->major_compaction_type == "sc") {
            options.major_compaction_type = leveldb::MajorCompactionType::kMajorCoordinatedStoC;
        } else {
            options.major_compaction_type = leveldb::MajorCompactionType::kMajorDisabled;
        }
        options.subrange_no_flush_num_keys = nova::NovaConfig::config->subrange_num_keys_no_flush;
        options.lower_key = nova::NovaConfig::config->cfgs[0]->fragments[db_index]->range.key_start;
        options.upper_key = nova::NovaConfig::config->cfgs[0]->fragments[db_index]->range.key_end;
        auto cfg = nova::NovaConfig::config->cfgs[0];
        if (nova::NovaConfig::config->use_local_disk) {
            options.manifest_stoc_ids.push_back(nova::NovaConfig::config->my_server_id);
        } else {
            uint32_t stocid = db_index % cfg->stoc_servers.size();
            for (int i = 0; i < nova::NovaConfig::config->number_of_manifest_replicas; i++) {
                stocid = (stocid + i) % cfg->stoc_servers.size();
                NOVA_LOG(rdmaio::INFO) << fmt::format("Manifest stoc id: {}", cfg->stoc_servers[stocid]);
                options.manifest_stoc_ids.push_back(cfg->stoc_servers[stocid]);
            }
        }
        options.num_tiny_ranges_per_subrange = nova::NovaConfig::config->num_tinyranges_per_subrange;
        return options;
    }

    leveldb::Options BuildStorageOptions(leveldb::MemManager *mem_manager, leveldb::Env *env) {
        leveldb::Options options;
        options.block_cache = nullptr;
        options.memtable_pool = nullptr;
        if (nova::NovaConfig::config->memtable_size_mb > 0) {
            options.write_buffer_size = (uint64_t) (nova::NovaConfig::config->memtable_size_mb) * 1024 * 1024;
        }
        if (nova::NovaConfig::config->sstable_size > 0) {
            options.max_file_size = nova::NovaConfig::config->sstable_size;
        }
        options.mem_manager = mem_manager;
        options.stoc_client = nullptr;
        options.num_memtable_partitions = nova::NovaConfig::config->num_memtable_partitions;
        options.num_memtables = nova::NovaConfig::config->num_memtables;
        options.max_open_files = 100000;
        options.enable_lookup_index = nova::NovaConfig::config->enable_lookup_index;
        options.num_recovery_thread = nova::NovaConfig::config->number_of_recovery_threads;
        options.level = nova::NovaConfig::config->level;
        options.max_stoc_file_size = std::max(options.write_buffer_size, options.max_file_size) +
                                     LEVELDB_TABLE_PADDING_SIZE_MB * 1024 * 1024;
        options.env = env;
        options.create_if_missing = true;
        options.compression = leveldb::kNoCompression;
        leveldb::InternalFilterPolicy *filter = new leveldb::InternalFilterPolicy(leveldb::NewBloomFilterPolicy(10));
        options.filter_policy = filter;
        options.enable_tracing = false;
        options.comparator = new YCSBKeyComparator();
        if (nova::NovaConfig::config->memtable_type == "pool") {
            options.memtable_type = leveldb::MemTableType::kMemTablePool;
        } else {
            options.memtable_type = leveldb::MemTableType::kStaticPartition;
        }
        options.enable_subranges = nova::NovaConfig::config->enable_subrange;
        options.subrange_reorg_sampling_ratio = 1.0;
        options.enable_flush_multiple_memtables = nova::NovaConfig::config->enable_flush_multiple_memtables;
        options.max_num_sstables_in_nonoverlapping_set = 15;
        return options;
    }

    leveldb::DB *CreateDatabase(int cfg_id, int db_index, leveldb::Cache *cache,
                                leveldb::MemTablePool *memtable_pool,
                                leveldb::MemManager *mem_manager,
                                leveldb::StoCClient *stoc_client,
                                const std::vector<leveldb::EnvBGThread *> &bg_compaction_threads,
                                const std::vector<leveldb::EnvBGThread *> &bg_flush_memtable_threads,
                                leveldb::EnvBGThread *reorg_thread,
                                leveldb::EnvBGThread *compaction_coord_thread) {
        leveldb::EnvOptions env_option;
        env_option.sstable_mode = leveldb::NovaSSTableMode::SSTABLE_MEM;
        leveldb::PosixEnv *env = new leveldb::PosixEnv;
        env->set_env_option(env_option);
        leveldb::DB *db;
        leveldb::Options options = BuildDBOptions(cfg_id, db_index, cache,
                                                  memtable_pool,
                                                  mem_manager,
                                                  stoc_client,
                                                  bg_compaction_threads,
                                                  bg_flush_memtable_threads,
                                                  reorg_thread,
                                                  compaction_coord_thread,
                                                  env);
        leveldb::Logger *log = nullptr;
        std::string db_path = nova::DBName(nova::NovaConfig::config->db_path, db_index);
        nova::mkdirs(db_path.c_str());
        NOVA_ASSERT(env->NewLogger(db_path + "/LOG-" + std::to_string(db_index), &log).ok());
        options.info_log = log;
        leveldb::Status status = leveldb::DB::Open(options, db_path, &db);
        NOVA_ASSERT(status.ok()) << "Open leveldb failed " << status.ToString();
        return db;
    }
}