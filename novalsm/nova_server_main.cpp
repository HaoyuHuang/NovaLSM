
//
// Created by Haoyu Huang on 2/20/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
// NovaLSM main class.


#include "rdma/rdma_ctrl.hpp"
#include "common/nova_common.h"
#include "common/nova_config.h"
#include "nic_server.h"
#include "leveldb/db.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "ltc/storage_selector.h"
#include "ltc/db_migration.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <gflags/gflags.h>
#include "db/version_set.h"

using namespace std;
using namespace rdmaio;
using namespace nova;

DEFINE_string(db_path, "/tmp/db", "level db path");
DEFINE_string(stoc_files_path, "/tmp/stoc", "StoC files path");

DEFINE_string(all_servers, "localhost:11211", "A list of servers");
DEFINE_int64(server_id, -1, "Server id.");
DEFINE_int64(number_of_ltcs, 0, "The first n are LTCs and the rest are StoCs.");

DEFINE_uint64(mem_pool_size_gb, 0, "Memory pool size in GB.");
DEFINE_uint64(use_fixed_value_size, 0, "Fixed value size.");

DEFINE_uint64(rdma_port, 0, "The port used by RDMA.");
DEFINE_uint64(rdma_max_msg_size, 0, "The maximum message size used by RDMA.");
DEFINE_uint64(rdma_max_num_sends, 0,
              "The maximum number of pending RDMA sends. This includes READ/WRITE/SEND. We also post the same number of RECV events. ");
DEFINE_uint64(rdma_doorbell_batch_size, 0, "The doorbell batch size.");
DEFINE_bool(enable_rdma, false, "Enable RDMA.");
DEFINE_bool(enable_load_data, false, "Enable loading data.");

DEFINE_string(ltc_config_path, "/tmp/uniform-3-32-10000000-frags.txt",
              "The path that stores the configuration.");
DEFINE_uint64(ltc_num_client_workers, 0, "Number of client worker threads.");
DEFINE_uint32(num_rdma_fg_workers, 0,
              "Number of RDMA foreground worker threads.");
DEFINE_uint32(num_compaction_workers, 0,
              "Number of compaction worker threads.");
DEFINE_uint32(num_rdma_bg_workers, 0,
              "Number of RDMA background worker threads.");

DEFINE_uint32(num_storage_workers, 0,
              "Number of storage worker threads.");
DEFINE_uint32(ltc_num_stocs_scatter_data_blocks, 0,
              "Number of StoCs to scatter data blocks of an SSTable.");

DEFINE_uint64(block_cache_mb, 0, "block cache size in mb");
DEFINE_uint64(row_cache_mb, 0, "row cache size in mb. Not supported");

DEFINE_uint32(num_memtables, 0, "Number of memtables.");
DEFINE_uint32(num_memtable_partitions, 0,
              "Number of memtable partitions. One active memtable per partition.");
DEFINE_bool(enable_lookup_index, false, "Enable lookup index.");
DEFINE_bool(enable_range_index, false, "Enable range index.");

DEFINE_uint32(l0_start_compaction_mb, 0,
              "Level-0 size to start compaction in MB.");
DEFINE_uint32(l0_stop_write_mb, 0, "Level-0 size to stall writes in MB.");
DEFINE_int32(level, 2, "Number of levels.");

DEFINE_uint64(memtable_size_mb, 0, "memtable size in mb");
DEFINE_uint64(sstable_size_mb, 0, "sstable size in mb");
DEFINE_uint32(cc_log_buf_size, 0,
              "log buffer size. Not supported. Same as memtable size.");
DEFINE_uint32(max_stoc_file_size_mb, 0, "Max StoC file size in MB");
DEFINE_bool(use_local_disk, false,
            "Enable LTC to write data to its local disk.");
DEFINE_string(scatter_policy, "random",
              "Policy to scatter an SSTable, i.e., random/power_of_two");
DEFINE_string(log_record_mode, "none",
              "Policy for LogC to replicate log records, i.e., none/rdma");
DEFINE_uint32(num_log_replicas, 0, "Number of replicas for a log record.");
DEFINE_string(memtable_type, "", "Memtable type, i.e., pool/static_partition");

DEFINE_bool(recover_dbs, false, "Enable recovery");
DEFINE_uint32(num_recovery_threads, 32, "Number of recovery threads");

DEFINE_bool(enable_subrange, false, "Enable subranges");
DEFINE_bool(enable_subrange_reorg, false, "Enable subrange reorganization.");
DEFINE_double(sampling_ratio, 1,
              "Sampling ratio on memtables for subrange reorg. A value between 0 and 1.");
DEFINE_string(zipfian_dist_ref_counts, "/tmp/zipfian",
              "Zipfian ref count file used to report load imbalance across subranges.");
DEFINE_string(client_access_pattern, "uniform",
              "Client access pattern used to report load imbalance across subranges.");
DEFINE_uint32(num_tinyranges_per_subrange, 10,
              "Number of tiny ranges per subrange.");

DEFINE_bool(enable_detailed_db_stats, false,
            "Enable detailed stats. It will report stats such as number of overlapping SSTables between Level-0 and Level-1.");
DEFINE_bool(enable_flush_multiple_memtables, false,
            "Enable a compaction thread to compact mulitple memtables at the same time.");
DEFINE_uint32(subrange_no_flush_num_keys, 100,
              "A subrange merges memtables into new a memtable if its contained number of unique keys is less than this threshold.");
DEFINE_string(major_compaction_type, "no",
              "Major compaction type: i.e., no/lc/sc");
DEFINE_uint32(major_compaction_max_parallism, 1,
              "The maximum compaction parallelism.");
DEFINE_uint32(major_compaction_max_tables_in_a_set, 15,
              "The maximum number of SSTables in a compaction job.");
DEFINE_uint32(num_sstable_replicas, 1, "Number of replicas for SSTables.");
DEFINE_uint32(num_sstable_metadata_replicas, 1, "Number of replicas for meta blocks of SSTables.");
DEFINE_bool(use_parity_for_sstable_data_blocks, false, "");
DEFINE_uint32(num_manifest_replicas, 1, "Number of replicas for manifest file.");

DEFINE_int32(fail_stoc_id, -1, "The StoC to fail.");
DEFINE_int32(exp_seconds_to_fail_stoc, -1,
             "Number of seconds elapsed to fail the stoc.");
DEFINE_int32(failure_duration, -1, "Failure duration");
DEFINE_int32(num_migration_threads, 1, "Number of migration threads");
DEFINE_string(ltc_migration_policy, "base", "immediate/base");
DEFINE_bool(use_ordered_flush, false, "use ordered flush");

NovaConfig *NovaConfig::config;
std::atomic_int_fast32_t leveldb::EnvBGThread::bg_flush_memtable_thread_id_seq;
std::atomic_int_fast32_t nova::StorageWorker::storage_file_number_seq;
// Sequence id to assign tasks to a thread in a round-robin manner.
std::atomic_int_fast32_t nova::RDMAServerImpl::compaction_storage_worker_seq_id_;
std::atomic_int_fast32_t leveldb::EnvBGThread::bg_compaction_thread_id_seq;
std::atomic_int_fast32_t nova::RDMAServerImpl::fg_storage_worker_seq_id_;
std::atomic_int_fast32_t nova::RDMAServerImpl::bg_storage_worker_seq_id_;
std::atomic_int_fast32_t leveldb::StoCBlockClient::rdma_worker_seq_id_;
std::atomic_int_fast32_t nova::DBMigration::migration_seq_id_;
std::atomic_int_fast32_t leveldb::StorageSelector::stoc_for_compaction_seq_id;

std::unordered_map<uint64_t, leveldb::FileMetaData *> leveldb::Version::last_fnfile;
std::atomic<nova::Servers *> leveldb::StorageSelector::available_stoc_servers;
NovaGlobalVariables NovaGlobalVariables::global;

void StartServer() {
    RdmaCtrl *rdma_ctrl = new RdmaCtrl(NovaConfig::config->my_server_id,
                                       NovaConfig::config->rdma_port);
//    if (NovaConfig::config->my_server_id < FLAGS_number_of_ltcs) {
//        NovaConfig::config->mem_pool_size_gb = 10;
//    }
    int port = NovaConfig::config->servers[NovaConfig::config->my_server_id].port;
    uint64_t nrdmatotal = nrdma_buf_server();
    uint64_t ntotal = nrdmatotal;
    ntotal += NovaConfig::config->mem_pool_size_gb * 1024 * 1024 * 1024;
    NOVA_LOG(INFO) << "Allocated buffer size in bytes: " << ntotal;

    auto *buf = (char *) malloc(ntotal);
    memset(buf, 0, ntotal);
    NovaConfig::config->nova_buf = buf;
    NovaConfig::config->nnovabuf = ntotal;
    NOVA_ASSERT(buf != NULL) << "Not enough memory";

    if (!FLAGS_recover_dbs) {
        int ret = system(fmt::format("exec rm -rf {}/*",
                           NovaConfig::config->db_path).data());
        ret = system(fmt::format("exec rm -rf {}/*",
                           NovaConfig::config->stoc_files_path).data());
    }
    mkdirs(NovaConfig::config->stoc_files_path.data());
    mkdirs(NovaConfig::config->db_path.data());
    auto *mem_server = new NICServer(rdma_ctrl, buf, port);
    mem_server->Start();
}

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    int i;
    const char **methods = event_get_supported_methods();
    printf("Starting Libevent %s.  Available methods are:\n",
           event_get_version());
    for (i = 0; methods[i] != NULL; ++i) {
        printf("    %s\n", methods[i]);
    }
    if (FLAGS_server_id == -1) {
        exit(0);
    }
    std::vector<gflags::CommandLineFlagInfo> flags;
    gflags::GetAllFlags(&flags);
    for (const auto &flag : flags) {
        printf("%s=%s\n", flag.name.c_str(),
               flag.current_value.c_str());
    }

    NovaConfig::config = new NovaConfig;
    NovaConfig::config->stoc_files_path = FLAGS_stoc_files_path;

    NovaConfig::config->mem_pool_size_gb = FLAGS_mem_pool_size_gb;
    NovaConfig::config->load_default_value_size = FLAGS_use_fixed_value_size;
    // RDMA
    NovaConfig::config->rdma_port = FLAGS_rdma_port;
    NovaConfig::config->max_msg_size = FLAGS_rdma_max_msg_size;
    NovaConfig::config->rdma_max_num_sends = FLAGS_rdma_max_num_sends;
    NovaConfig::config->rdma_doorbell_batch_size = FLAGS_rdma_doorbell_batch_size;

    NovaConfig::config->block_cache_mb = FLAGS_block_cache_mb;
    NovaConfig::config->memtable_size_mb = FLAGS_memtable_size_mb;

    NovaConfig::config->db_path = FLAGS_db_path;
    NovaConfig::config->enable_rdma = FLAGS_enable_rdma;
    NovaConfig::config->enable_load_data = FLAGS_enable_load_data;
    NovaConfig::config->major_compaction_type = FLAGS_major_compaction_type;
    NovaConfig::config->enable_flush_multiple_memtables = FLAGS_enable_flush_multiple_memtables;
    NovaConfig::config->major_compaction_max_parallism = FLAGS_major_compaction_max_parallism;
    NovaConfig::config->major_compaction_max_tables_in_a_set = FLAGS_major_compaction_max_tables_in_a_set;

    NovaConfig::config->number_of_recovery_threads = FLAGS_num_recovery_threads;
    NovaConfig::config->recover_dbs = FLAGS_recover_dbs;
    NovaConfig::config->number_of_sstable_data_replicas = FLAGS_num_sstable_replicas;
    NovaConfig::config->number_of_sstable_metadata_replicas = FLAGS_num_sstable_metadata_replicas;
    NovaConfig::config->number_of_manifest_replicas = FLAGS_num_manifest_replicas;
    NovaConfig::config->use_parity_for_sstable_data_blocks = FLAGS_use_parity_for_sstable_data_blocks;

    NovaConfig::config->servers = convert_hosts(FLAGS_all_servers);
    NovaConfig::config->my_server_id = FLAGS_server_id;
    NovaConfig::config->num_conn_workers = FLAGS_ltc_num_client_workers;
    NovaConfig::config->num_fg_rdma_workers = FLAGS_num_rdma_fg_workers;
    NovaConfig::config->num_storage_workers = FLAGS_num_storage_workers;
    NovaConfig::config->num_compaction_workers = FLAGS_num_compaction_workers;
    NovaConfig::config->num_bg_rdma_workers = FLAGS_num_rdma_bg_workers;
    NovaConfig::config->num_memtables = FLAGS_num_memtables;
    NovaConfig::config->num_memtable_partitions = FLAGS_num_memtable_partitions;
    NovaConfig::config->enable_subrange = FLAGS_enable_subrange;
    NovaConfig::config->memtable_type = FLAGS_memtable_type;

    NovaConfig::config->num_stocs_scatter_data_blocks = FLAGS_ltc_num_stocs_scatter_data_blocks;
    NovaConfig::config->max_stoc_file_size = FLAGS_max_stoc_file_size_mb * 1024;
    NovaConfig::config->manifest_file_size = NovaConfig::config->max_stoc_file_size * 4;
    NovaConfig::config->sstable_size = FLAGS_sstable_size_mb * 1024 * 1024;
    NovaConfig::config->use_local_disk = FLAGS_use_local_disk;
    NovaConfig::config->num_tinyranges_per_subrange = FLAGS_num_tinyranges_per_subrange;

    if (FLAGS_scatter_policy == "random") {
        NovaConfig::config->scatter_policy = ScatterPolicy::RANDOM;
    } else if (FLAGS_scatter_policy == "power_of_two") {
        NovaConfig::config->scatter_policy = ScatterPolicy::POWER_OF_TWO;
    } else if (FLAGS_scatter_policy == "power_of_three") {
        NovaConfig::config->scatter_policy = ScatterPolicy::POWER_OF_THREE;
    } else if (FLAGS_scatter_policy == "local") {
        NovaConfig::config->scatter_policy = ScatterPolicy::LOCAL;
        NOVA_ASSERT(NovaConfig::config->num_stocs_scatter_data_blocks == 1);
    } else {
        NovaConfig::config->scatter_policy = ScatterPolicy::SCATTER_DC_STATS;
    }

    if (FLAGS_log_record_mode == "none") {
        NovaConfig::config->log_record_mode = NovaLogRecordMode::LOG_NONE;
    } else if (FLAGS_log_record_mode == "rdma") {
        NovaConfig::config->log_record_mode = NovaLogRecordMode::LOG_RDMA;
    }

    NovaConfig::config->enable_lookup_index = FLAGS_enable_lookup_index;
    NovaConfig::config->enable_range_index = FLAGS_enable_range_index;
    NovaConfig::config->subrange_sampling_ratio = FLAGS_sampling_ratio;
    NovaConfig::config->zipfian_dist_file_path = FLAGS_zipfian_dist_ref_counts;
    NovaConfig::config->ReadZipfianDist();
    NovaConfig::config->client_access_pattern = FLAGS_client_access_pattern;
    NovaConfig::config->enable_detailed_db_stats = FLAGS_enable_detailed_db_stats;
    NovaConfig::config->subrange_num_keys_no_flush = FLAGS_subrange_no_flush_num_keys;
    NovaConfig::config->l0_stop_write_mb = FLAGS_l0_stop_write_mb;
    NovaConfig::config->l0_start_compaction_mb = FLAGS_l0_start_compaction_mb;
    NovaConfig::config->level = FLAGS_level;
    NovaConfig::config->enable_subrange_reorg = FLAGS_enable_subrange_reorg;
    NovaConfig::config->num_migration_threads = FLAGS_num_migration_threads;
    NovaConfig::config->use_ordered_flush = FLAGS_use_ordered_flush;

    if (FLAGS_ltc_migration_policy == "immediate") {
        NovaConfig::config->ltc_migration_policy = LTCMigrationPolicy::IMMEDIATE;
    } else {
        NovaConfig::config->ltc_migration_policy = LTCMigrationPolicy::PROCESS_UNTIL_MIGRATION_COMPLETE;
    }

    NovaConfig::ReadFragments(FLAGS_ltc_config_path);
    if (FLAGS_num_log_replicas > 0) {
        for (int i = 0; i < NovaConfig::config->cfgs.size(); i++) {
            NOVA_ASSERT(FLAGS_num_log_replicas <= NovaConfig::config->cfgs[i]->stoc_servers.size());
        }
        NovaConfig::ComputeLogReplicaLocations(FLAGS_num_log_replicas);
    }
    NOVA_LOG(INFO) << fmt::format("{} configurations", NovaConfig::config->cfgs.size());
    for (auto c : NovaConfig::config->cfgs) {
        NOVA_LOG(INFO) << c->DebugString();
    }

    leveldb::EnvBGThread::bg_flush_memtable_thread_id_seq = 0;
    leveldb::EnvBGThread::bg_compaction_thread_id_seq = 0;
    nova::RDMAServerImpl::bg_storage_worker_seq_id_ = 0;
    leveldb::StoCBlockClient::rdma_worker_seq_id_ = 0;
    nova::StorageWorker::storage_file_number_seq = 0;
    nova::RDMAServerImpl::compaction_storage_worker_seq_id_ = 0;
    nova::DBMigration::migration_seq_id_ = 0;
    leveldb::StorageSelector::stoc_for_compaction_seq_id = nova::NovaConfig::config->my_server_id;
    nova::NovaGlobalVariables::global.Initialize();
    auto available_stoc_servers = new Servers;
    available_stoc_servers->servers = NovaConfig::config->cfgs[0]->stoc_servers;
    for (int i = 0; i < available_stoc_servers->servers.size(); i++) {
        available_stoc_servers->server_ids.insert(available_stoc_servers->servers[i]);
    }
    leveldb::StorageSelector::available_stoc_servers.store(available_stoc_servers);

    // Sanity checks.
    if (NovaConfig::config->number_of_sstable_data_replicas > 1) {
        NOVA_ASSERT(NovaConfig::config->number_of_sstable_data_replicas ==
                    NovaConfig::config->number_of_sstable_metadata_replicas);
        NOVA_ASSERT(NovaConfig::config->num_stocs_scatter_data_blocks == 1);
        NOVA_ASSERT(!NovaConfig::config->use_parity_for_sstable_data_blocks);
    }

    if (NovaConfig::config->use_parity_for_sstable_data_blocks) {
        NOVA_ASSERT(NovaConfig::config->number_of_sstable_data_replicas == 1);
        NOVA_ASSERT(NovaConfig::config->num_stocs_scatter_data_blocks > 1);
        NOVA_ASSERT(NovaConfig::config->num_stocs_scatter_data_blocks +
                    NovaConfig::config->number_of_sstable_metadata_replicas + 1 <=
                    NovaConfig::config->cfgs[0]->stoc_servers.size());
    }
    for (int i = 0; i < NovaConfig::config->cfgs.size(); i++) {
        auto cfg = NovaConfig::config->cfgs[i];
        NOVA_ASSERT(FLAGS_ltc_num_stocs_scatter_data_blocks <= cfg->stoc_servers.size()) << fmt::format(
                    "Not enough stoc to scatter. Scatter width: {} Num StoCs: {}",
                    FLAGS_ltc_num_stocs_scatter_data_blocks, cfg->stoc_servers.size());
        NOVA_ASSERT(FLAGS_num_sstable_replicas <= cfg->stoc_servers.size()) << fmt::format(
                    "Not enough stoc to replicate sstables. Replication factor: {} Num StoCs: {}",
                    FLAGS_num_sstable_replicas, cfg->stoc_servers.size());
    }
    StartServer();
    return 0;
}
