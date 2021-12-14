// Copyright (c) 2011 The leveldb Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifdef NUMA
#include <numa.h>
#include <numaif.h>
#endif
#include <sys/types.h>
#include <gflags/gflags.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <ltc/storage_selector.h>
#include <db/version_set.h>
//#include "common/nova_config.h"
#include "leveldb/cache.h"
#include "db/table_cache.h"
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#include "rdma/rdma_ctrl.hpp"
#include "common/nova_common.h"
#include "common/nova_config.h"
#include "novalsm/local_server.h"
#include "novalsm/nic_server.h"
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
nova::NovaConfig *nova::NovaConfig::config = nullptr;
std::atomic_int_fast32_t leveldb::EnvBGThread::bg_flush_memtable_thread_id_seq;
std::atomic_int_fast32_t leveldb::EnvBGThread::bg_compaction_thread_id_seq ;
nova::NovaGlobalVariables nova::NovaGlobalVariables::global;
std::atomic<nova::Servers *> leveldb::StorageSelector::available_stoc_servers;
std::unordered_map<uint64_t, leveldb::FileMetaData *> leveldb::Version::last_fnfile;
std::atomic_int_fast32_t leveldb::StorageSelector::stoc_for_compaction_seq_id;

std::atomic_int_fast32_t nova::StorageWorker::storage_file_number_seq;
// Sequence id to assign tasks to a thread in a round-robin manner.
std::atomic_int_fast32_t nova::RDMAServerImpl::compaction_storage_worker_seq_id_;

std::atomic_int_fast32_t nova::RDMAServerImpl::fg_storage_worker_seq_id_;
std::atomic_int_fast32_t nova::RDMAServerImpl::bg_storage_worker_seq_id_;
std::atomic_int_fast32_t leveldb::StoCBlockClient::rdma_worker_seq_id_;
std::atomic_int_fast32_t nova::DBMigration::migration_seq_id_;

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
// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      seekordered   -- N ordered seeks
//      open          -- cost of opening a DB
//      crc32c        -- repeated crc32c of 4K of data
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)
//static const char* FLAGS_benchmarks =
//        "fillseq,"
//        "fillsync,"
//        "fillrandom,"
//        "overwrite,"
//        "readrandom,"
//        "readrandom,"  // Extra run to allow previous compactions to quiesce
//        "readseq,"
//        "readreverse,"
//        "compact,"
//        "readrandom,"
//        "readseq,"
//        "readreverse,"
//        "fill100K,"
//        "crc32c,"
//        "snappycomp,"
//        "snappyuncomp,";
DEFINE_string(benchmarks, "fillrandom,readrandom",
             "Comma-separated list of operations to run in the specified order");
// Number of key/values to place in database
DEFINE_int32(num, 1000000,
              "Number of key/values to place in database");
//static int FLAGS_num = 1000000;

// Number of read operations to do.  If negative, do FLAGS_num reads.
DEFINE_int32(reads, -1,
             "Number of read operations to do");
//static int FLAGS_reads = -1;

// Number of concurrent threads to run.
DEFINE_int32(threads, 1,
             "Number of concurrent threads to run.");
//static int FLAGS_threads = 1;


// Size of each value
DEFINE_int32(value_size, 412,
             "Size of each value");
//static int FLAGS_value_size = 400;
// Size of each key
//static int FLAGS_key_size = 20;
// Arrange to generate values that shrink to this fraction of
// their original size after compression
DEFINE_double(compression_ratio, 0.5,
             "Arrange to generate values that shrink to this fraction of their original size after compression");
//static double FLAGS_compression_ratio = 0.5;

// Print histogram of operation timings
DEFINE_bool(histogram, false,
              "Print histogram of operation timings");
//static bool FLAGS_histogram = false;

// Count the number of string comparisons performed
DEFINE_bool(comparisons, false,
            "Count the number of string comparisons performed");
//static bool FLAGS_comparisons = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
DEFINE_int32(write_buffer_size, 0,
            "Number of bytes to buffer in memtable before compacting");
//static int FLAGS_write_buffer_size = 0;

// Number of bytes written to each file.
// (initialized to default value by "main")
DEFINE_int32(max_file_size, 0,
             "Number of bytes written to each file.");
//static int FLAGS_max_file_size = 0;

// Approximate size of user data packed per block (before compression.
// (initialized to default value by "main")
DEFINE_int32(block_size, 0,
             "Approximate size of user data packed per block (before compression.");
//static int FLAGS_block_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
DEFINE_int32(cache_size, -1,
             "Number of bytes written to each file.");
//static int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
DEFINE_int32(open_files, 0,
             "Number of bytes written to each file.");
//static int FLAGS_open_files = 0;
DEFINE_int32(block_restart_interval, 16,
             "Number of bytes written to each file.");
//static int FLAGS_block_restart_interval = 16;
// Bloom filter bits per key.
// Negative means use default settings.
DEFINE_int32(bloom_bits, 10,
             "bloom filter bits per key.");
//static int FLAGS_bloom_bits = 10;

// Common key prefix length.
DEFINE_int32(key_prefix, 0,
             "Common key prefix length.");
//static int FLAGS_key_prefix = 0;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
DEFINE_bool(use_existing_db, false,
             "Common key prefix length.");
//static bool FLAGS_use_existing_db = false;
// whether the writer threads aware of the NUMA archetecture.
DEFINE_bool(enable_numa, false,
            "Common key prefix length.");
//static bool FLAGS_enable_numa = false;
// If true, reuse existing log/MANIFEST files when re-opening a database.
//static bool FLAGS_reuse_logs = false;

// Use the db with the following name.
DEFINE_string(db, "",
            "db name.");
//static const char* FLAGS_db = nullptr;

namespace leveldb {

    namespace {
        leveldb::Env* g_env = nullptr;

        class CountComparator : public Comparator {
        public:
            CountComparator(const Comparator* wrapped) : wrapped_(wrapped) {}
            ~CountComparator() override {}
            int Compare(const Slice& a, const Slice& b) const override {
                count_.fetch_add(1, std::memory_order_relaxed);
                return wrapped_->Compare(a, b);
            }
            const char* Name() const override { return wrapped_->Name(); }
            void FindShortestSeparator(std::string* start,
                                       const Slice& limit) const override {
                wrapped_->FindShortestSeparator(start, limit);
            }

            void FindShortSuccessor(std::string* key) const override {
                return wrapped_->FindShortSuccessor(key);
            }

            size_t comparisons() const { return count_.load(std::memory_order_relaxed); }

            void reset() { count_.store(0, std::memory_order_relaxed); }

        private:
            mutable std::atomic<size_t> count_{0};
            const Comparator* const wrapped_;
        };

// Helper for quickly generating random data.
        class RandomGenerator {
        private:
            std::string data_;
            int pos_;

        public:
            RandomGenerator() {
                // We use a limited amount of data over and over again and ensure
                // that it is larger than the compression window (32KB), and also
                // large enough to serve all typical value sizes we want to write.
                Random rnd(301);
                std::string piece;
                while (data_.size() < 1048576) {
                    // Add a short fragment that is as compressible as specified
                    // by FLAGS_compression_ratio.
                    test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
                    data_.append(piece);
                }
                pos_ = 0;
            }

            Slice Generate(size_t len) {
                if (pos_ + len > data_.size()) {
                    pos_ = 0;
                    assert(len < data_.size());
                }
                pos_ += len;
                return Slice(data_.data() + pos_ - len, len);
            }
        };

        class KeyBuffer {
        public:
            KeyBuffer() {
                assert(FLAGS_key_prefix < sizeof(buffer_));
                memset(buffer_, 'a', FLAGS_key_prefix);
            }
            KeyBuffer& operator=(KeyBuffer& other) = delete;
            KeyBuffer(KeyBuffer& other) = delete;

            void Set(int k) {
                std::snprintf(buffer_ + FLAGS_key_prefix,
                              sizeof(buffer_) - FLAGS_key_prefix, "%020d", k); //%016d means preceeding with 0s
            }

            Slice slice() const { return Slice(buffer_, FLAGS_key_prefix + 20); }

        private:
            char buffer_[1024];
        };

#if defined(__linux)
        static Slice TrimSpace(Slice s) {
            size_t start = 0;
            while (start < s.size() && isspace(s[start])) {
                start++;
            }
            size_t limit = s.size();
            while (limit > start && isspace(s[limit - 1])) {
                limit--;
            }
            return Slice(s.data() + start, limit - start);
        }
#endif

        static void AppendWithSpace(std::string* str, Slice msg) {
            if (msg.empty()) return;
            if (!str->empty()) {
                str->push_back(' ');
            }
            str->append(msg.data(), msg.size());
        }

        class Stats {
        private:
            double start_;
            double finish_;
            double seconds_;
            int done_;
            int next_report_;
            int64_t bytes_;
            double last_op_finish_;
            Histogram hist_;
            std::string message_;

        public:
            Stats() { Start(); }

            void Start() {
                next_report_ = 100;
                hist_.Clear();
                done_ = 0;
                bytes_ = 0;
                seconds_ = 0;
                message_.clear();
                start_ = finish_ = last_op_finish_ = g_env->NowMicros();
            }

            void Merge(const Stats& other) {
                hist_.Merge(other.hist_);
                done_ += other.done_;
                bytes_ += other.bytes_;
                seconds_ += other.seconds_;
                if (other.start_ < start_) start_ = other.start_;
                if (other.finish_ > finish_) finish_ = other.finish_;

                // Just keep the messages from one thread
                if (message_.empty()) message_ = other.message_;
            }

            void Stop() {
                finish_ = g_env->NowMicros();
                seconds_ = (finish_ - start_) * 1e-6;
            }

            void AddMessage(Slice msg) { AppendWithSpace(&message_, msg); }

            void FinishedSingleOp() {
                if (FLAGS_histogram) {
                    double now = g_env->NowMicros();
                    double micros = now - last_op_finish_;
                    hist_.Add(micros);
                    if (micros > 20000) {
                        std::fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
                        std::fflush(stderr);
                    }
                    last_op_finish_ = now;
                }

                done_++;
                if (done_ >= next_report_) {
                    if (next_report_ < 1000)
                        next_report_ += 100;
                    else if (next_report_ < 5000)
                        next_report_ += 500;
                    else if (next_report_ < 10000)
                        next_report_ += 1000;
                    else if (next_report_ < 50000)
                        next_report_ += 5000;
                    else if (next_report_ < 100000)
                        next_report_ += 10000;
                    else if (next_report_ < 500000)
                        next_report_ += 50000;
                    else
                        next_report_ += 100000;
                    std::fprintf(stderr, "... finished %d ops%30s\r", done_, "");
                    std::fflush(stderr);
                }
            }

            void AddBytes(int64_t n) { bytes_ += n; }

            void Report(const Slice& name) {
                // Pretend at least one op was done in case we are running a benchmark
                // that does not call FinishedSingleOp().
                if (done_ < 1) done_ = 1;
                double elapsed = (finish_ - start_) * 1e-6;
                std::string extra;
                if (bytes_ > 0) {
                    // Rate is computed on actual elapsed time, not the sum of per-thread
                    // elapsed times.

                    char rate[100];
                    std::snprintf(rate, sizeof(rate), "%6.1f MB/s",
                                  (bytes_ / 1048576.0) / elapsed);
                    extra = rate;
                }
                AppendWithSpace(&extra, message_);

                std::fprintf(stdout, "%-12s : %11.3f micros/op; %ld ops/sec;%s%s\n",
                             name.ToString().c_str(), seconds_ * 1e6 / done_, (long)(done_/elapsed),
                             (extra.empty() ? "" : " "), extra.c_str());

                if (FLAGS_histogram) {
                    std::fprintf(stdout, "Microseconds per op:\n%s\n",
                                 hist_.ToString().c_str());
                }
                std::fflush(stdout);
            }
        };

// State shared by all concurrent executions of the same benchmark.
        struct SharedState {
            port::Mutex mu;
            port::CondVar cv GUARDED_BY(mu);
            int total GUARDED_BY(mu);

            // Each thread goes through the following states:
            //    (1) initializing
            //    (2) waiting for others to be initialized
            //    (3) running
            //    (4) done

            int num_initialized GUARDED_BY(mu);
            int num_done GUARDED_BY(mu);
            bool start GUARDED_BY(mu);

            SharedState(int total)
                    : cv(&mu), total(total), num_initialized(0), num_done(0), start(false) {}
        };

// Per-thread state for concurrent executions of the same benchmark.
        struct ThreadState {
            int tid;      // 0..n-1 when running in n threads
            Random64 rand;  // Has different seeds for different threads
//  Random rand;
            Stats stats;
            SharedState* shared;

            ThreadState(int index, int seed) : tid(index), rand(seed), shared(nullptr) {}
        };

    }  // namespace
    void StartServer(DB **db, LocalServer **local_server) {
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
//    auto *mem_server = new NICServer(rdma_ctrl, buf, port);
        *local_server = new LocalServer(rdma_ctrl, buf);
        *db =  (*local_server)->Start();
    }
    void nova_config_process_code(DB **db, LocalServer **local_server) {

        int i;
//    const char **methods = event_get_supported_methods();
//    printf("Starting Libevent %s.  Available methods are:\n",
//           event_get_version());
//    for (i = 0; methods[i] != NULL; ++i) {
//        printf("    %s\n", methods[i]);
//    }
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
        StartServer(db, local_server);
    }
    class Benchmark {
    private:
        Cache* cache_;
        const FilterPolicy* filter_policy_;
        DB* db_;
        LocalServer* local_s;
        int num_;
        int value_size_;
        int entries_per_batch_;
        WriteOptions write_options_;
        int reads_;
        int heap_counter_;
        CountComparator count_comparator_;
        int total_thread_count_;
        std::vector<std::string> validation_keys;

        void PrintHeader() {
            const int kKeySize = 16 + FLAGS_key_prefix;
            PrintEnvironment();
            std::fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
            std::fprintf(
                    stdout, "Values:     %d bytes each (%d bytes after compression)\n",
                    FLAGS_value_size,
                    static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
            std::fprintf(stdout, "Entries:    %d\n", num_);
            std::fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
                         ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_) /
                          1048576.0));
            std::fprintf(
                    stdout, "FileSize:   %.1f MB (estimated)\n",
                    (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_) /
                     1048576.0));
            PrintWarnings();
            std::fprintf(stdout, "------------------------------------------------\n");
        }

        void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
            std::fprintf(
                    stdout,
                    "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
            std::fprintf(
                    stdout,
                    "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

            // See if snappy is working by attempting to compress a compressible string
            const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
            std::string compressed;
            if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
                std::fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
            } else if (compressed.size() >= sizeof(text)) {
                std::fprintf(stdout, "WARNING: Snappy compression is not effective\n");
            }
        }

        void PrintEnvironment() {
            std::fprintf(stderr, "leveldb:    version %d.%d\n", kMajorVersion,
                         kMinorVersion);

#if defined(__linux)
            time_t now = time(nullptr);
            std::fprintf(stderr, "Date:       %s",
                         ctime(&now));  // ctime() adds newline

            FILE* cpuinfo = std::fopen("/proc/cpuinfo", "r");
            if (cpuinfo != nullptr) {
                char line[1000];
                int num_cpus = 0;
                std::string cpu_type;
                std::string cache_size;
                while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
                    const char* sep = strchr(line, ':');
                    if (sep == nullptr) {
                        continue;
                    }
                    Slice key = TrimSpace(Slice(line, sep - 1 - line));
                    Slice val = TrimSpace(Slice(sep + 1));
                    if (key == "model name") {
                        ++num_cpus;
                        cpu_type = val.ToString();
                    } else if (key == "cache size") {
                        cache_size = val.ToString();
                    }
                }
                std::fclose(cpuinfo);
                std::fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
                std::fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
            }
#endif
        }

    public:
        Benchmark()
                : cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : nullptr),
                  filter_policy_(FLAGS_bloom_bits >= 0
                                 ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                                 : nullptr),
                  db_(nullptr),
                  num_(FLAGS_num),
                  value_size_(FLAGS_value_size),
                  entries_per_batch_(1),
                  reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
                  heap_counter_(0),
                  count_comparator_(BytewiseComparator()),
                  total_thread_count_(0) {
            std::vector<std::string> files;
            g_env->GetChildren(FLAGS_db, &files);
            for (size_t i = 0; i < files.size(); i++) {
                if (Slice(files[i]).starts_with("heap-")) {
                    g_env->DeleteFile(std::string(FLAGS_db) + "/" + files[i]);
                }
            }
            if (!FLAGS_use_existing_db) {
                DestroyDB(FLAGS_db, Options());
            }
        }

        ~Benchmark() {
            delete db_;
            delete cache_;
            delete filter_policy_;
        }
        Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
            // Hardcoded as 10 bytes.
            char* data = new char[10];
            const char* const_data = data;
            key_guard->reset(const_data);
            return Slice(key_guard->get(), sizeof(int));
        }
        Slice AllocateKey(std::unique_ptr<const char[]>* key_guard, size_t key_size) {
            char* data = new char[key_size];
            const char* const_data = data;
            key_guard->reset(const_data);
            return Slice(key_guard->get(), key_size);
        }
//        void GenerateKeyFromInt(uint64_t v, int64_t num_keys, Slice* key) {
//
//            char* start = const_cast<char*>(key->data());
//            char* pos = start;
////    if (keys_per_prefix_ > 0) {
////      int64_t num_prefix = num_keys / keys_per_prefix_;
////      int64_t prefix = v % num_prefix;
////      int bytes_to_fill = std::min(prefix_size_, 8);
////      if (port::kLittleEndian) {
////        for (int i = 0; i < bytes_to_fill; ++i) {
////          pos[i] = (prefix >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
////        }
////      } else {
////        memcpy(pos, static_cast<void*>(&prefix), bytes_to_fill);
////      }
////      if (prefix_size_ > 8) {
////        // fill the rest with 0s
////        memset(pos + 8, '0', prefix_size_ - 8);
////      }
////      pos += prefix_size_;
////    }
//
//            int bytes_to_fill = std::min(FLAGS_key_size, 8);
//            if (port::kLittleEndian) {
//                for (int i = 0; i < bytes_to_fill; ++i) {
//                    pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
//                }
//            } else {
//                memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
//            }
//            pos += bytes_to_fill;
//            if (FLAGS_key_size > pos - start) {
//                memset(pos, '0', FLAGS_key_size - (pos - start));
//            }
//        }
        void Run() {

            PrintHeader();
            Open();

            const char* benchmarks = FLAGS_benchmarks.c_str();
//    Validation_Write();
            while (benchmarks != nullptr) {

                const char* sep = strchr(benchmarks, ',');
                Slice name;
                if (sep == nullptr) {
                    name = benchmarks;
                    benchmarks = nullptr;
                } else {
                    name = Slice(benchmarks, sep - benchmarks);
                    benchmarks = sep + 1;
                }

                // Reset parameters that may be overridden below
                num_ = FLAGS_num;
                reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
                value_size_ = FLAGS_value_size;
                entries_per_batch_ = 1;
                write_options_ = WriteOptions();

                void (Benchmark::*method)(ThreadState*) = nullptr;
                bool fresh_db = false;
                int num_threads = FLAGS_threads;

                if (name == Slice("open")) {
                    method = &Benchmark::OpenBench;
                    num_ /= 10000;
                    if (num_ < 1) num_ = 1;
                } else if (name == Slice("fillseq")) {
                    fresh_db = true;
                    method = &Benchmark::WriteSeq;
                } else if (name == Slice("fillbatch")) {
                    fresh_db = true;
                    entries_per_batch_ = 1000;
                    method = &Benchmark::WriteSeq;
                } else if (name == Slice("fillrandom")) {
                    fresh_db = true;
                    method = &Benchmark::WriteRandom;
                } else if (name == Slice("overwrite")) {
                    fresh_db = false;
                    method = &Benchmark::WriteRandom;
                } else if (name == Slice("fillsync")) {
                    fresh_db = true;
                    num_ /= 1000;
//                    write_options_.sync = true;
                    method = &Benchmark::WriteRandom;
                } else if (name == Slice("fill100K")) {
                    fresh_db = true;
                    num_ /= 1000;
                    value_size_ = 100 * 1000;
                    method = &Benchmark::WriteRandom;
                } else if (name == Slice("readseq")) {
                    method = &Benchmark::ReadSequential;
                } else if (name == Slice("readreverse")) {
                    method = &Benchmark::ReadReverse;
                } else if (name == Slice("readrandom")) {
                    method = &Benchmark::ReadRandom;
                } else if (name == Slice("readmissing")) {
                    method = &Benchmark::ReadMissing;
                } else if (name == Slice("seekrandom")) {
                    method = &Benchmark::SeekRandom;
                } else if (name == Slice("seekordered")) {
                    method = &Benchmark::SeekOrdered;
                } else if (name == Slice("readhot")) {
                    method = &Benchmark::ReadHot;
                } else if (name == Slice("readrandomsmall")) {
                    reads_ /= 1000;
                    method = &Benchmark::ReadRandom;
                } else if (name == Slice("deleteseq")) {
                    method = &Benchmark::DeleteSeq;
                } else if (name == Slice("deleterandom")) {
                    method = &Benchmark::DeleteRandom;
                } else if (name == Slice("readwhilewriting")) {
                    num_threads++;  // Add extra thread for writing
                    method = &Benchmark::ReadWhileWriting;
//                } else if (name == Slice("compact")) {
//                    method = &Benchmark::Compact;
                } else if (name == Slice("crc32c")) {
                    method = &Benchmark::Crc32c;
                } else if (name == Slice("snappycomp")) {
                    method = &Benchmark::SnappyCompress;
                } else if (name == Slice("snappyuncomp")) {
                    method = &Benchmark::SnappyUncompress;
                } else if (name == Slice("heapprofile")) {
                    HeapProfile();
                } else if (name == Slice("stats")) {
                    PrintStats("leveldb.stats");
                } else if (name == Slice("sstables")) {
                    PrintStats("leveldb.sstables");
                } else {
                    if (!name.empty()) {  // No error message for empty name
                        std::fprintf(stderr, "unknown benchmark '%s'\n",
                                     name.ToString().c_str());
                    }
                }

                if (fresh_db) {
                    if (FLAGS_use_existing_db) {
                        std::fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                                     name.ToString().c_str());
                        method = nullptr;
                    } else {
//                        delete db_;
//                        db_ = nullptr;
//                        DestroyDB(FLAGS_db, Options());
//                        Open();
//                        DEBUG("The second open finished.\n");
                    }
                }

                if (method != nullptr) {
//#ifdef PROCESSANALYSIS
//                    if (method == &Benchmark::ReadRandom || method == &Benchmark::ReadWhileWriting){
//          TableCache::CleanAll();
//        }
//#endif
//                    DEBUG("The benchmark start.\n");
                    RunBenchmark(num_threads, name, method);
//                    DEBUG("Benchmark finished\n");
                    if (method == &Benchmark::WriteRandom){
                        // Wait until there are no SSTables at L0.
                        while (NovaConfig::config->major_compaction_type != "no") {
                            uint32_t l0tables = 0;
                            uint32_t nmemtables = 0;
                            bool needs_compaction = false;
                            {
                                leveldb::DBStats stats;
                                stats.sstable_size_dist = new uint32_t[20];
                                db_->QueryDBStats(&stats);
                                if (!needs_compaction) {
                                    needs_compaction = stats.needs_compaction;
                                }
                                l0tables += stats.num_l0_sstables;
                                nmemtables += db_->FlushMemTables(true);
                                delete stats.sstable_size_dist;
                            }
                            NOVA_LOG(rdmaio::INFO) << fmt::format(
                                        "Waiting for {} L0 tables and {} memtables to go to L1 Needs compaction:{}",
                                        l0tables, nmemtables, needs_compaction);
                            if (l0tables == 0 && nmemtables == 0) {
                                break;
                            }
                            sleep(1);
                        }
                    }
//
                }
            }
//            Validation_Read();
        }

    private:
        struct ThreadArg {
            Benchmark* bm;
            SharedState* shared;
            ThreadState* thread;
            void (Benchmark::*method)(ThreadState*);
        };

        static void ThreadBody(void* v) {
            ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
            SharedState* shared = arg->shared;
            ThreadState* thread = arg->thread;
            printf("Wait for thread start\n");
            {
                MutexLock l(&shared->mu);
                shared->num_initialized++;
                if (shared->num_initialized >= shared->total) {
                    shared->cv.SignalAll();
                }
                while (!shared->start) {
                    shared->cv.Wait();
                }
            }
            printf("Threads start to run\n");
            thread->stats.Start();
            (arg->bm->*(arg->method))(thread);
            thread->stats.Stop();

            {
                MutexLock l(&shared->mu);
                shared->num_done++;
                if (shared->num_done >= shared->total) {
                    shared->cv.SignalAll();
                }
            }
        }

        void RunBenchmark(int n, Slice name,
                          void (Benchmark::*method)(ThreadState*)) {
//    printf("Bechmark start\n");
//            if (name.ToString() == "fillrandom")
//                Validation_Write();
//    if (name.ToString() == "readrandom"){
//    }
            SharedState shared(n);

            ThreadArg* arg = new ThreadArg[n];
            for (int i = 0; i < n; i++) {
#ifdef NUMA
                if (FLAGS_enable_numa) {
        // Performs a local allocation of memory to threads in numa node.
        int n_nodes = numa_num_task_nodes();  // Number of nodes in NUMA.
        numa_exit_on_error = 1;
        int numa_node = i % n_nodes;
        bitmask* nodes = numa_allocate_nodemask();
        numa_bitmask_clearall(nodes);
        numa_bitmask_setbit(nodes, numa_node);
        // numa_bind() call binds the process to the node and these
        // properties are passed on to the thread that is created in
        // StartThread method called later in the loop.
        numa_bind(nodes);
        numa_set_strict(1);
        numa_free_nodemask(nodes);
      }
#endif
                arg[i].bm = this;
                arg[i].method = method;
                arg[i].shared = &shared;
                ++total_thread_count_;
                // Seed the thread's random state deterministically based upon thread
                // creation across all benchmarks. This ensures that the seeds are unique
                // but reproducible when rerunning the same set of benchmarks.
                arg[i].thread = new ThreadState(i, /*seed=*/1000 + total_thread_count_);
                arg[i].thread->shared = &shared;
                printf("start front-end threads\n");
                g_env->StartThread(ThreadBody, &arg[i]);
            }

            shared.mu.Lock();
            while (shared.num_initialized < n) {
                shared.cv.Wait();
            }

            shared.start = true;
            shared.cv.SignalAll();
            while (shared.num_done < n) {
                shared.cv.Wait();
            }
            shared.mu.Unlock();

            for (int i = 1; i < n; i++) {
                arg[0].thread->stats.Merge(arg[i].thread->stats);
            }
            arg[0].thread->stats.Report(name);
            if (FLAGS_comparisons) {
                fprintf(stdout, "Comparisons: %zu\n", count_comparator_.comparisons());
                count_comparator_.reset();
                fflush(stdout);
            }

            for (int i = 0; i < n; i++) {
                delete arg[i].thread;
            }
            delete[] arg;
//            db_->WaitforAllbgtasks();
    if (method == &Benchmark::WriteRandom)
      sleep(60); // wait for SSTable digestion
        }

        void Crc32c(ThreadState* thread) {
            // Checksum about 500MB of data total
            const int size = 4096;
            const char* label = "(4K per op)";
            std::string data(size, 'x');
            int64_t bytes = 0;
            uint32_t crc = 0;
            while (bytes < 500 * 1048576) {
                crc = crc32c::Value(data.data(), size);
                thread->stats.FinishedSingleOp();
                bytes += size;
            }
            // Print so result is not dead
            std::fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

            thread->stats.AddBytes(bytes);
            thread->stats.AddMessage(label);
        }

        void SnappyCompress(ThreadState* thread) {
            RandomGenerator gen;
            Slice input = gen.Generate(Options().block_size);
            int64_t bytes = 0;
            int64_t produced = 0;
            bool ok = true;
            std::string compressed;
            while (ok && bytes < 1024 * 1048576) {  // Compress 1G
                ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
                produced += compressed.size();
                bytes += input.size();
                thread->stats.FinishedSingleOp();
            }

            if (!ok) {
                thread->stats.AddMessage("(snappy failure)");
            } else {
                char buf[100];
                std::snprintf(buf, sizeof(buf), "(output: %.1f%%)",
                              (produced * 100.0) / bytes);
                thread->stats.AddMessage(buf);
                thread->stats.AddBytes(bytes);
            }
        }

        void SnappyUncompress(ThreadState* thread) {
            RandomGenerator gen;
            Slice input = gen.Generate(Options().block_size);
            std::string compressed;
            bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
            int64_t bytes = 0;
            char* uncompressed = new char[input.size()];
            while (ok && bytes < 1024 * 1048576) {  // Compress 1G
                ok = port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                             uncompressed);
                bytes += input.size();
                thread->stats.FinishedSingleOp();
            }
            delete[] uncompressed;

            if (!ok) {
                thread->stats.AddMessage("(snappy failure)");
            } else {
                thread->stats.AddBytes(bytes);
            }
        }

        void Open() {
            assert(db_ == nullptr);
            nova_config_process_code(&db_, &local_s);
//            Status s = DB::Open(options, FLAGS_db, &db_);
//            if (!s.ok()) {
//                std::fprintf(stderr, "open error: %s\n", s.ToString().c_str());
//                std::exit(1);
//            }
        }

        void OpenBench(ThreadState* thread) {
            for (int i = 0; i < num_; i++) {
                delete db_;
                Open();
                thread->stats.FinishedSingleOp();
            }
        }

        void WriteSeq(ThreadState* thread) { DoWrite(thread, true); }

        void WriteRandom(ThreadState* thread) { DoWrite(thread, false); }
//        void Validation_Write() {
//            Random64 rand(123);
//            RandomGenerator gen;
//            Status s;
//            std::unique_ptr<const char[]> key_guard;
//            WriteBatch batch;
//            Slice key = AllocateKey(&key_guard, FLAGS_key_size+1);
//            for (int i = 0; i < 1000; i++) {
//                batch.Clear();
////      //The key range should be adjustable.
//////        const int k = seq ? i + j : thread->rand.Uniform(FLAGS_num*FLAGS_threads);
////      const int k = rand.Next()%(FLAGS_num*FLAGS_threads);
//                GenerateKeyFromInt(i, FLAGS_num, &key);
//                key.Reset(key.data(), key.size()-1);
//                char to_be_append = 'v';// add an extra char to make key different from write bench.
//                assert(key.size() == FLAGS_key_size);
//                key.append(&to_be_append, 1);
//////      batch.Put(key, gen.Generate(value_size_));
//                batch.Put(key, key);
//
//                s = db_->Write(write_options_, &batch);
//                validation_keys.push_back(key.ToString());
//            }
//            printf("validation write finished\n");
//        }
//        void Validation_Read() {
//            ReadOptions options;
//            //TODO(ruihong): specify the cache option.
//            std::string value;
//            int not_found = 0;
////    KeyBuffer key;
//            std::unique_ptr<const char[]> key_guard;
//            Slice key = AllocateKey(&key_guard);
//            for (int i = 0; i < 1000; i++) {
//                key = validation_keys[i];
//                if (db_->Get(options, key, &value).ok()) {
//
//                }else{
////        printf("Validation failed\n");
//                    not_found++;
////        assert(false);
//                }
//            }
//            printf("validation read finished, not found num %d\n", not_found);
//        }
        void DoWrite(ThreadState* thread, bool seq) {
            if (num_ != FLAGS_num) {
                char msg[100];
                std::snprintf(msg, sizeof(msg), "(%d ops)", num_);
                thread->stats.AddMessage(msg);
            }
            auto worker = local_s->conn_workers[thread->tid];
            write_options_.stoc_client = worker->stoc_client_;
            write_options_.local_write = false;
            write_options_.thread_id = worker->thread_id_;
            write_options_.rand_seed = &worker->rand_seed;

//            write_options_.total_writes = total_writes.fetch_add(1, std::memory_order_relaxed) + 1;
            write_options_.replicate_log_record_states = worker->replicate_log_record_states;
            write_options_.rdma_backing_mem = worker->rdma_backing_mem;
            write_options_.rdma_backing_mem_size = worker->rdma_backing_mem_size;
            write_options_.is_loading_db = false;
            RandomGenerator gen;
            WriteBatch batch;
            Status s;
            int64_t bytes = 0;
//    KeyBuffer key;
//            std::unique_ptr<const char[]> key_guard;
//            Slice key = AllocateKey(&key_guard);
            char* key_b = new char[10];
            for (int i = 0; i < num_; i += entries_per_batch_) {
                batch.Clear();
                for (int j = 0; j < entries_per_batch_; j++) {
                    //The key range should be adjustable.
//        const int k = seq ? i + j : thread->rand.Uniform(FLAGS_num*FLAGS_threads);
                    const uint64_t k = seq ? i + j : thread->rand.Next()%(FLAGS_num*FLAGS_threads);
                    size_t len = nova::int_to_str(key_b, k);
                    Slice key =  Slice(key_b, len);
//        key.Set(k);
//                    GenerateKeyFromInt(k, FLAGS_num, &key);
//                    memcpy((void *) key.data(), (void*)(&k), sizeof(int));
//                    std::cout<< key.ToString() <<std::endl;
                    write_options_.hash = k;
//        batch.Put(key.slice(), gen.Generate(value_size_));
                    db_->Put(write_options_, key, gen.Generate(value_size_));

//        bytes += value_size_ + key.slice().size();
                    bytes += value_size_ + key.size();
                    thread->stats.FinishedSingleOp();
                }
//                s = db_->Write(write_options_, &batch);
                if (!s.ok()) {
                    std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                    std::exit(1);
                }
            }
            thread->stats.AddBytes(bytes);
        }

        void ReadSequential(ThreadState* thread) {
            Iterator* iter = db_->NewIterator(ReadOptions());
            int i = 0;
            int64_t bytes = 0;
            for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
                bytes += iter->key().size() + iter->value().size();
                thread->stats.FinishedSingleOp();
                ++i;
            }
            delete iter;
            thread->stats.AddBytes(bytes);
        }

        void ReadReverse(ThreadState* thread) {
            Iterator* iter = db_->NewIterator(ReadOptions());
            int i = 0;
            int64_t bytes = 0;
            for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
                bytes += iter->key().size() + iter->value().size();
                thread->stats.FinishedSingleOp();
                ++i;
            }
            delete iter;
            thread->stats.AddBytes(bytes);
        }

        void ReadRandom(ThreadState* thread) {
            ReadOptions options;
//            leveldb::ReadOptions read_options;
//            options.hash = int_key;
            // tid is the thread number start from 0
            auto worker = local_s->conn_workers[thread->tid];
            options.stoc_client = worker->stoc_client_;
//            uint32_t scid = local_s->mem_manager->slabclassid(0, MAX_BLOCK_SIZE);
            options.mem_manager = worker->mem_manager_;
//            options.thread_id = worker->thread_id_;
            options.rdma_backing_mem = worker->rdma_backing_mem;
            options.rdma_backing_mem_size = worker->rdma_backing_mem_size;
            options.cfg_id = NovaConfig::config->current_cfg_id;
            //TODO(ruihong): specify the cache option.
            std::string value;
            int found = 0;
//    KeyBuffer key;
//            std::unique_ptr<const char[]> key_guard;
//            Slice key = AllocateKey(&key_guard);
            char* key_b = new char[10];
            for (int i = 0; i < reads_; i++) {
//      const int k = thread->rand.Uniform(FLAGS_num*FLAGS_threads);// make it uniform as write.
                const int k = thread->rand.Next()%(FLAGS_num*FLAGS_threads);
                size_t len = nova::int_to_str(key_b, k);
                Slice key =  Slice(key_b, len);
//            key.Set(k);
//                GenerateKeyFromInt(k, FLAGS_num, &key);
//                memcpy((void *) key.data(), (void*)(&k), sizeof(int));
                options.hash = k;
//      if (db_->Get(options, key.slice(), &value).ok()) {
//        found++;
//      }
                if (db_->Get(options, key, &value).ok()) {
                    NOVA_ASSERT(value.size() == 416)
                        << fmt::format("value size is not correct {}", value.size());
                    found++;
                }
                thread->stats.FinishedSingleOp();
            }
            char msg[100];
            std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
            thread->stats.AddMessage(msg);
        }

        void ReadMissing(ThreadState* thread) {
            ReadOptions options;
            std::string value;
            KeyBuffer key;
            for (int i = 0; i < reads_; i++) {
                const int k = thread->rand.Uniform(FLAGS_num);
                key.Set(k);
                Slice s = Slice(key.slice().data(), key.slice().size() - 1);
                db_->Get(options, s, &value);
                thread->stats.FinishedSingleOp();
            }
        }

        void ReadHot(ThreadState* thread) {
            ReadOptions options;
            std::string value;
            const int range = (FLAGS_num + 99) / 100;
            KeyBuffer key;
            for (int i = 0; i < reads_; i++) {
                const int k = thread->rand.Uniform(range);
                key.Set(k);
                db_->Get(options, key.slice(), &value);
                thread->stats.FinishedSingleOp();
            }
        }

        void SeekRandom(ThreadState* thread) {
            ReadOptions options;
            int found = 0;
            KeyBuffer key;
            for (int i = 0; i < reads_; i++) {
                Iterator* iter = db_->NewIterator(options);
                const int k = thread->rand.Uniform(FLAGS_num);
                key.Set(k);
                iter->Seek(key.slice());
                if (iter->Valid() && iter->key() == key.slice()) found++;
                delete iter;
                thread->stats.FinishedSingleOp();
            }
            char msg[100];
            snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
            thread->stats.AddMessage(msg);
        }

        void SeekOrdered(ThreadState* thread) {
            ReadOptions options;
            Iterator* iter = db_->NewIterator(options);
            int found = 0;
            int k = 0;
            KeyBuffer key;
            for (int i = 0; i < reads_; i++) {
                k = (k + (thread->rand.Uniform(100))) % FLAGS_num;
                key.Set(k);
                iter->Seek(key.slice());
                if (iter->Valid() && iter->key() == key.slice()) found++;
                thread->stats.FinishedSingleOp();
            }
            delete iter;
            char msg[100];
            std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
            thread->stats.AddMessage(msg);
        }

        void DoDelete(ThreadState* thread, bool seq) {
            RandomGenerator gen;
//            WriteBatch batch;
            Status s;
            KeyBuffer key;
//            WriteOptions o;
            for (int i = 0; i < num_; i += entries_per_batch_) {
//                batch.Clear();
                for (int j = 0; j < entries_per_batch_; j++) {
                    const int k = seq ? i + j : (thread->rand.Uniform(FLAGS_num));
                    key.Set(k);
                    db_->Delete(write_options_,key.slice());
                    thread->stats.FinishedSingleOp();
                }
//                s = db_->Write(write_options_, &batch);
                if (!s.ok()) {
                    std::fprintf(stderr, "del error: %s\n", s.ToString().c_str());
                    std::exit(1);
                }
            }
        }

        void DeleteSeq(ThreadState* thread) { DoDelete(thread, true); }

        void DeleteRandom(ThreadState* thread) { DoDelete(thread, false); }

        void ReadWhileWriting(ThreadState* thread) {
            if (thread->tid > 0) {
                ReadRandom(thread);
            } else {
                // Special thread that keeps writing until other threads are done.
                RandomGenerator gen;
                KeyBuffer key;
                while (true) {
                    {
                        MutexLock l(&thread->shared->mu);
                        if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
                            // Other threads have finished
                            break;
                        }
                    }

                    const int k = thread->rand.Uniform(FLAGS_num*FLAGS_threads);
                    key.Set(k);
                    Status s =
                            db_->Put(write_options_, key.slice(), gen.Generate(value_size_));
                    if (!s.ok()) {
                        std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                        std::exit(1);
                    }
                }

                // Do not count any of the preceding work/delay in stats.
                thread->stats.Start();
            }
        }

//        void Compact(ThreadState* thread) { db_->CompactRange(nullptr, nullptr); }

        void PrintStats(const char* key) {
            std::string stats;
            if (!db_->GetProperty(key, &stats)) {
                stats = "(failed)";
            }
            std::fprintf(stdout, "\n%s\n", stats.c_str());
        }

        static void WriteToFile(void* arg, const char* buf, int n) {
            reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
        }

        void HeapProfile() {
            char fname[100];
            std::snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db.c_str(),
                          ++heap_counter_);
            WritableFile* file;
            Status s = g_env->NewWritableFile(fname, {},&file);
            if (!s.ok()) {
                std::fprintf(stderr, "%s\n", s.ToString().c_str());
                return;
            }
            bool ok = port::GetHeapProfile(WriteToFile, file);
            delete file;
            if (!ok) {
                std::fprintf(stderr, "heap profiling not supported\n");
                g_env->DeleteFile(fname);
            }
        }
    };





}  // namespace leveldb

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
    FLAGS_max_file_size = leveldb::Options().max_file_size;
    FLAGS_block_size = leveldb::Options().block_size;
    FLAGS_open_files = leveldb::Options().max_open_files;
    std::string default_db_path;




    leveldb::g_env = leveldb::Env::Default();

    // Choose a location for the test database if none given with --db=<path>
    if (FLAGS_db.empty()) {
        leveldb::g_env->GetTestDirectory(&default_db_path);
        default_db_path += "/dbbench";
        string &FLAGS_db = default_db_path;
    }

    leveldb::Benchmark benchmark;
    benchmark.Run();
    return 0;
}