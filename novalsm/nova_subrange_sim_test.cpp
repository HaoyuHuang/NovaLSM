
//
// Created by Haoyu Huang on 4/22/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "rdma/rdma_ctrl.hpp"
#include "common/nova_common.h"
#include "common/nova_config.h"
#include "nic_server.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "db/filename.h"
#include "util/env_posix.h"
#include "db/version_set.h"
#include "ltc/storage_selector.h"

#include "util/generator.h"
#include "util/uniform_generator.h"
#include "util/zipfian_generator.h"

#include <stdlib.h>
#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <thread>
#include <assert.h>
#include <csignal>
#include <gflags/gflags.h>


using namespace std;
using namespace rdmaio;
using namespace nova;

DEFINE_uint32(cc_num_memtables, 16, "");
DEFINE_uint32(cc_num_memtable_partitions, 8, "");
DEFINE_uint64(cc_iterations, 10000000, "");
DEFINE_double(cc_sampling_ratio, 1, "");
DEFINE_string(cc_zipfian_dist, "/tmp/zipfian", "");
DEFINE_string(cc_client_access_pattern, "zipfian", "");
DEFINE_string(test_filename, "035350.ldb", "");

namespace {
    class YCSBKeyComparator : public leveldb::Comparator {
    public:
        //   if a < b: negative result
        //   if a > b: positive result
        //   else: zero result
        int
        Compare(const leveldb::Slice &a, const leveldb::Slice &b) const {
            uint64_t ai = 0;
            str_to_int(a.data(), &ai, a.size());
            uint64_t bi = 0;
            str_to_int(b.data(), &bi, b.size());

            if (ai < bi) {
                return -1;
            } else if (ai > bi) {
                return 1;
            }
            return 0;
        }

        // Ignore the following methods for now:
        const char *Name() const { return "YCSBKeyComparator"; }

        void
        FindShortestSeparator(std::string *,
                              const leveldb::Slice &) const {}

        void FindShortSuccessor(std::string *) const {}
    };

    leveldb::DB *CreateDatabase(int db_index, leveldb::Cache *cache,
                                leveldb::MemManager *mem_manager,
                                leveldb::MemTablePool *memtable_pool,
                                std::vector<leveldb::EnvBGThread *> bg_threads,
                                leveldb::EnvBGThread *reorg_thread) {
        leveldb::EnvOptions env_option;
        env_option.sstable_mode = leveldb::NovaSSTableMode::SSTABLE_MEM;
        leveldb::PosixEnv *env = new leveldb::PosixEnv;
        env->set_env_option(env_option);
        leveldb::DB *db;
        leveldb::Options options;
        options.mem_manager = mem_manager;
        options.block_cache = cache;
        options.debug = true;
        options.memtable_pool = memtable_pool;
        options.subrange_reorg_sampling_ratio = FLAGS_cc_sampling_ratio;
        options.zipfian_dist_file_path = FLAGS_cc_zipfian_dist;

        if (NovaConfig::config->memtable_size_mb > 0) {
            options.write_buffer_size =
                    (uint64_t) (
                            NovaConfig::config->memtable_size_mb) *
                    1024 * 1024;
        }
        if (NovaConfig::config->sstable_size > 0) {
            options.max_file_size = NovaConfig::config->sstable_size;
        }
        options.num_compaction_threads = bg_threads.size();
        options.num_memtable_partitions = NovaConfig::config->num_memtable_partitions;
        options.num_memtables = NovaConfig::config->num_memtables;
        options.l0bytes_stop_writes_trigger = 0;
        options.max_open_files = 50000;
        options.enable_lookup_index = NovaConfig::config->enable_lookup_index;
        options.max_stoc_file_size =
                std::max(options.write_buffer_size, options.max_file_size) +
                LEVELDB_TABLE_PADDING_SIZE_MB * 1024 * 1024;
        options.env = env;
        options.create_if_missing = true;
        options.compression = leveldb::kNoCompression;
        options.filter_policy = leveldb::NewBloomFilterPolicy(10);
        options.bg_flush_memtable_threads = bg_threads;
        options.enable_tracing = false;
        options.comparator = new YCSBKeyComparator();
        options.memtable_type = leveldb::MemTableType::kStaticPartition;
        options.enable_subranges = NovaConfig::config->enable_subrange;
        options.subrange_reorg_sampling_ratio = 1.0;
        options.reorg_thread = reorg_thread;
        options.num_tiny_ranges_per_subrange = 10;
        options.enable_subranges = true;
        options.max_num_sstables_in_nonoverlapping_set = 0;
        options.max_num_coordinated_compaction_nonoverlapping_sets = 0;
        options.enable_flush_multiple_memtables = false;
        options.major_compaction_type = leveldb::MajorCompactionType::kMajorDisabled;

        leveldb::Logger *log = nullptr;
        std::string db_path = DBName(NovaConfig::config->db_path, db_index);
        mkdirs(db_path.c_str());

        NOVA_ASSERT(env->NewLogger(
                db_path + "/LOG-" + std::to_string(db_index), &log).ok());
        options.info_log = log;
        leveldb::Status status = leveldb::DB::Open(options, db_path, &db);
        NOVA_ASSERT(status.ok()) << "Open leveldb failed "
                                 << status.ToString();

        uint32_t index = 0;
        std::string logname = leveldb::LogFileName(db_path, 1111);
        ParseDBIndexFromLogFileName(logname, &index);
        NOVA_ASSERT(index == db_index);
        return db;
    }
}

NovaConfig *NovaConfig::config;
std::atomic_int_fast32_t leveldb::EnvBGThread::bg_flush_memtable_thread_id_seq;
std::atomic_int_fast32_t leveldb::EnvBGThread::bg_compaction_thread_id_seq;
std::atomic_int_fast32_t nova::RDMAServerImpl::fg_storage_worker_seq_id_;
std::atomic_int_fast32_t nova::RDMAServerImpl::bg_storage_worker_seq_id_;
std::atomic_int_fast32_t nova::RDMAServerImpl::compaction_storage_worker_seq_id_;
std::atomic_int_fast32_t leveldb::StoCBlockClient::rdma_worker_seq_id_;
std::atomic_int_fast32_t nova::StorageWorker::storage_file_number_seq;
std::atomic_int_fast32_t nova::DBMigration::migration_seq_id_;
std::unordered_map<uint64_t, leveldb::FileMetaData *> leveldb::Version::last_fnfile;
std::atomic<nova::Servers *> leveldb::StorageSelector::available_stoc_servers;
std::atomic_int_fast32_t leveldb::StorageSelector::stoc_for_compaction_seq_id;

NovaGlobalVariables NovaGlobalVariables::global;

void start(NICServer *server) {
    server->Start();
}

void TestSubRanges() {
    uint64_t nrdmatotal = nrdma_buf_server();
    uint64_t ntotal = nrdmatotal;
    ntotal += NovaConfig::config->mem_pool_size_gb * 1024 * 1024 * 1024;
    NOVA_LOG(INFO) << "Allocated buffer size in bytes: " << ntotal;

    auto *buf = (char *) malloc(ntotal);
    memset(buf, 0, ntotal);
    NovaConfig::config->nova_buf = buf;
    NovaConfig::config->nnovabuf = ntotal;
    NOVA_ASSERT(buf != NULL) << "Not enough memory";
    int ret = system(fmt::format("exec rm -rf {}/*", NovaConfig::config->db_path).data());
    ret = system(fmt::format("exec rm -rf {}/*",
                       NovaConfig::config->stoc_files_path).data());

    mkdirs(NovaConfig::config->stoc_files_path.data());
    mkdirs(NovaConfig::config->db_path.data());

    std::vector<leveldb::EnvBGThread *> bgs;
    for (int i = 0; i < FLAGS_cc_num_memtable_partitions; i++) {
        leveldb::LTCNoopCompactionThread *bg = new leveldb::LTCNoopCompactionThread;
        bgs.push_back(bg);
    }

    leveldb::LTCCompactionThread *reorg = new leveldb::LTCCompactionThread(
            nullptr);
    std::thread t(&leveldb::LTCCompactionThread::Start, reorg);
    leveldb::MemManager *mem_manager = new NovaMemManager(buf, 1, 1, 18);

    leveldb::DB *db = CreateDatabase(0, nullptr, mem_manager, nullptr, bgs,
                                     reorg);
    for (int i = 0; i < FLAGS_cc_num_memtable_partitions; i++) {
        auto bg = reinterpret_cast<leveldb::LTCNoopCompactionThread *> (bgs[i]);
        bg->db = db;
    }

    auto stat_thread = new NovaStatThread;
    stat_thread->bg_storage_workers_ = {};
    stat_thread->bgs_ = bgs;
    stat_thread->async_workers_ = {};
    stat_thread->async_compaction_workers_ = {};
    std::thread stats_thread(&NovaStatThread::Start, stat_thread);

    // load data.
    timeval start{};
    gettimeofday(&start, nullptr);
    uint64_t loaded_keys = 0;
    std::vector<LTCFragment *> &frags = NovaConfig::config->cfgs[0]->fragments;
    leveldb::StoCReplicateLogRecordState *state = new leveldb::StoCReplicateLogRecordState[NovaConfig::config->servers.size()];
    for (int i = 0; i < NovaConfig::config->servers.size(); i++) {
        state[i].rdma_wr_id = -1;
        state[i].result = leveldb::StoCReplicateLogRecordResult::REPLICATE_LOG_RECORD_NONE;
    }

    unsigned int rand_seed = 0;
    uint32_t records = 10000000;
    ycsbc::Generator<uint64_t> *gen = nullptr;
    if (FLAGS_cc_client_access_pattern == "uniform") {
        gen = new ycsbc::UniformGenerator(0, records);
    } else {
        gen = new ycsbc::ZipfianGenerator(0, records);
    }

    for (uint64_t j = 0; j < FLAGS_cc_iterations; j++) {
        uint32_t rid = gen->Next();
        auto v = static_cast<char>((rid % 10) + 'a');

        std::string key(std::to_string(rid));
        std::string val(
                NovaConfig::config->load_default_value_size, v);

        for (int i = 0; i < NovaConfig::config->servers.size(); i++) {
            state[i].rdma_wr_id = -1;
            state[i].result = leveldb::StoCReplicateLogRecordResult::REPLICATE_LOG_RECORD_NONE;
        }
        leveldb::WriteOptions option;
        option.hash = rid;
        option.rand_seed = &rand_seed;
        option.thread_id = 0;
        option.local_write = true;
        option.replicate_log_record_states = state;
        option.is_loading_db = false;
        leveldb::Status s = db->Put(option, key, val);
        NOVA_ASSERT(s.ok());
    }

    t.join();
    stats_thread.join();
}

leveldb::ReadWriteFile *f;
char *backing_mem;

void Read(uint32_t id) {
    leveldb::StoCBlockHandle h = {};
    uint64_t size = 4096;
    if (id == 0) {
        size = 15l * 1024 * 1024;
    }
    leveldb::Slice result;
    NOVA_LOG(rdmaio::INFO) << fmt::format("{}:read {} start", id, size);
    f->Read(h, 0, size, &result, backing_mem);
    NOVA_LOG(rdmaio::INFO) << fmt::format("{}:read {} end", id, size);
}


int main(int argc, char *argv[]) {

    {
        uint32_t db = 0;
        uint32_t mid = 0;
        ParseDBIndexFromLogFileName("0-100", &db, &mid);
        NOVA_LOG(rdmaio::INFO) << fmt::format("!!!!!!!!!!! {}-{}", db, mid);
    }

    NovaConfig::config = new NovaConfig;
    NovaConfig::config->stoc_files_path = "/tmp/rtables";

    NovaConfig::config->mem_pool_size_gb = 1;

    NovaConfig::config->load_default_value_size = 1024;
    // RDMA
    NovaConfig::config->rdma_port = 11211;
    NovaConfig::config->max_msg_size = 1024;
    NovaConfig::config->rdma_max_num_sends = 256;
    NovaConfig::config->rdma_doorbell_batch_size = 8;

    NovaConfig::config->block_cache_mb = 0;
    NovaConfig::config->memtable_size_mb = 16;

    NovaConfig::config->db_path = "/tmp/db";
    NovaConfig::config->enable_rdma = false;
    NovaConfig::config->enable_load_data = true;

    NovaConfig::config->servers = convert_hosts("localhost:11222");
//    for (int i = 0; i < NovaConfig::config->servers.size(); i++) {
//        if (i < 1) {
//            NovaConfig::config->ltc_servers.push_back(
//                    NovaConfig::config->servers[i]);
//        } else {
//            NovaConfig::config->stoc_servers.push_back(
//                    NovaConfig::config->servers[i]);
//        }
//    }

//    for (int i = 0; i < NovaConfig::config->ltc_servers.size(); i++) {
//        Host host = NovaConfig::config->ltc_servers[i];
//        NOVA_LOG(INFO)
//            << fmt::format("ltc: {}:{}:{}", host.server_id, host.ip, host.port);
//    }
//    for (int i = 0; i < NovaConfig::config->stoc_servers.size(); i++) {
//        Host host = NovaConfig::config->stoc_servers[i];
//        NOVA_LOG(INFO)
//            << fmt::format("dc: {}:{}:{}", host.server_id, host.ip, host.port);
//    }

    NovaConfig::config->my_server_id = 0;

    NovaConfig::ReadFragments(
            "/tmp/rdma-shared-ltc-nrecords-10000000-nccservers-1-nlogreplicas-1-nranges-1");
    uint32_t start_stoc_id = 0;
    for (int i = 0; i < NovaConfig::config->cfgs[0]->fragments.size(); i++) {
        NovaConfig::config->cfgs[0]->fragments[i]->log_replica_stoc_ids.clear();
        for (int r = 0; r < 0; r++) {
            NovaConfig::config->cfgs[0]->fragments[i]->log_replica_stoc_ids.push_back(start_stoc_id);
            start_stoc_id = (start_stoc_id + 1) % NovaConfig::config->cfgs[0]->stoc_servers.size();
        }
    }

    NovaConfig::config->num_conn_workers = 1;
    NovaConfig::config->num_fg_rdma_workers = 1;
    NovaConfig::config->num_storage_workers = 1;
    NovaConfig::config->num_compaction_workers = 1;
    NovaConfig::config->num_bg_rdma_workers = 1;
    NovaConfig::config->num_memtables = FLAGS_cc_num_memtables;
    NovaConfig::config->num_memtable_partitions = FLAGS_cc_num_memtable_partitions;
    NovaConfig::config->l0_stop_write_mb = 0;
    NovaConfig::config->enable_subrange = true;
    NovaConfig::config->memtable_type = "static_partition";

    NovaConfig::config->num_stocs_scatter_data_blocks = 1;
    NovaConfig::config->max_stoc_file_size = 18 * 1024 * 1024;
    NovaConfig::config->sstable_size = 18 * 1024 * 1024;
    NovaConfig::config->use_local_disk = false;
    NovaConfig::config->scatter_policy = ScatterPolicy::RANDOM;
    NovaConfig::config->log_record_mode = NovaLogRecordMode::LOG_NONE;

    NovaConfig::config->enable_lookup_index = true;
    leveldb::EnvBGThread::bg_flush_memtable_thread_id_seq = 0;
    leveldb::EnvBGThread::bg_compaction_thread_id_seq = 0;
    nova::RDMAServerImpl::bg_storage_worker_seq_id_ = 0;
    leveldb::StoCBlockClient::rdma_worker_seq_id_ = 0;
    NovaConfig::config->use_local_disk = false;
    nova::RDMAServerImpl::compaction_storage_worker_seq_id_ = 0;
    NovaConfig::config->subrange_sampling_ratio = FLAGS_cc_sampling_ratio;
    NovaConfig::config->zipfian_dist_file_path = FLAGS_cc_zipfian_dist;
    NovaConfig::config->ReadZipfianDist();
    NovaConfig::config->client_access_pattern = FLAGS_cc_client_access_pattern;
    NovaConfig::config->level = 6;

    char *edit_memory = (char *) malloc(10240);
    leveldb::VersionEdit edit;
    {
        std::vector<leveldb::Range> tiny_ranges;
        leveldb::Range r = {};
        r.lower = "0";
        r.upper = "1111";
        r.lower_inclusive = true;
        r.upper_inclusive = false;
        tiny_ranges.push_back(r);
        edit.UpdateSubRange(0, tiny_ranges, 0);
    }
    {
        std::vector<leveldb::Range> tiny_ranges;
        leveldb::Range r;
        r.lower = "2222";
        r.upper = "33333";
        r.lower_inclusive = true;
        r.upper_inclusive = false;
        tiny_ranges.push_back(r);
        edit.UpdateSubRange(1, tiny_ranges, 1);
    }
    edit.DeleteFile(0, 123);
    leveldb::InternalKey smallest("haoyu", 4567,
                                  leveldb::ValueType::kTypeValue);
    leveldb::InternalKey largest("lulu", 4568, leveldb::ValueType::kTypeValue);
    leveldb::StoCBlockHandle meta_handle = {};
    meta_handle.server_id = 1;
    meta_handle.stoc_file_id = 2;
    meta_handle.offset = 2323;
    meta_handle.size = 92323;
    leveldb::StoCBlockHandle data_handle = {};
    data_handle.server_id = 3;
    data_handle.stoc_file_id = 3;
    data_handle.offset = 3434;
    data_handle.size = 222;

    leveldb::StoCBlockHandle data_handle2 = {};
    data_handle2.server_id = 4;
    data_handle2.stoc_file_id = 4;
    data_handle2.offset = 5555;
    data_handle2.size = 111;
    {
        std::vector<leveldb::FileReplicaMetaData> replicas;
        leveldb::FileReplicaMetaData replica;
        replica.meta_block_handle = meta_handle;
        replica.data_block_group_handles.push_back(data_handle);
        replicas.push_back(replica);
        edit.AddFile(0, {4}, 333, 102400, 10240, 99999, smallest, largest,
                     replicas, {});
    }
    {
        std::vector<leveldb::FileReplicaMetaData> replicas;
        leveldb::FileReplicaMetaData replica;
        replica.meta_block_handle = meta_handle;
        replica.data_block_group_handles.push_back(data_handle2);
        replicas.push_back(replica);
        edit.AddFile(0, {5}, 444, 232323, 45464, 32341, smallest, largest,
                     replicas, {});
    }
    edit.SetNextFile(45555);
    edit.SetLastSequence(9999999);
    uint32_t size = edit.EncodeTo(edit_memory);

    leveldb::VersionEdit new_edit;
    NOVA_ASSERT(new_edit.DecodeFrom(leveldb::Slice(edit_memory, size)).ok());
    NOVA_LOG(rdmaio::INFO) << new_edit.DebugString();
//    TestSubRanges();

    for (int j = 0; j < 64; j++) {
        for (int k = 0; k < 100000; k++) {
            uint32_t sid = 0;
            uint32_t dbid = 0;
            ParseDBIndexFromLogFileName(LogFileName(j, k), &dbid);
//                RDMA_LOG(INFO) << fmt::format("{} {}", sid, dbid);
            NOVA_ASSERT(dbid == j);
        }
    }


//    std::string fname = FLAGS_test_filename;
//    leveldb::Env* posix = new leveldb::PosixEnv;
//    leveldb::EnvFileMetadata meta;
//    meta.level = 0;
//    RDMA_LOG(rdmaio::INFO) << fname;
//    auto s = posix->NewReadWriteFile(fname, meta, &f);
//    RDMA_ASSERT(s.ok()) << s.ToString();
//
//    backing_mem = (char*) malloc(15 * 1024 * 1024);
//    std::vector<std::thread> threads;
//    for (int i = 0; i < 64; i++) {
//        threads.push_back(std::thread(Read, i));
//    }
//    for (int i = 0; i < 64; i++) {
//        threads[i].join();
//    }
    return 0;
}
