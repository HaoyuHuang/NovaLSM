
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <netinet/tcp.h>
#include <signal.h>
#include <fmt/core.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/syscall.h>
#include "leveldb/cache.h"

#include "leveldb/write_batch.h"
#include "db/filename.h"
#include "nova_cc_nic_server.h"
#include "nova_cc.h"
#include "util/env_posix.h"

namespace nova {
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
                                    leveldb::MemTablePool *memtable_pool,
                                    leveldb::MemManager *mem_manager,
                                    leveldb::CCClient *cc_client,
                                    std::vector<leveldb::EnvBGThread *> &bg_threads,
                                    leveldb::EnvBGThread *reorg_thread) {
            leveldb::EnvOptions env_option;
            env_option.sstable_mode = leveldb::NovaSSTableMode::SSTABLE_MEM;
            leveldb::PosixEnv *env = new leveldb::PosixEnv;
            env->set_env_option(env_option);
            leveldb::DB *db;
            leveldb::Options options;
            options.block_cache = cache;
            options.memtable_pool = memtable_pool;
            if (NovaConfig::config->write_buffer_size_mb > 0) {
                options.write_buffer_size =
                        (uint64_t) (
                                NovaConfig::config->write_buffer_size_mb) *
                        1024 * 1024;
            }
            if (NovaConfig::config->sstable_size > 0) {
                options.max_file_size = NovaConfig::config->sstable_size;
            }
            options.mem_manager = mem_manager;
            options.dc_client = cc_client;
            options.num_memtable_partitions = NovaConfig::config->num_memtable_partitions;
            options.l0_consolidate_group_size = 8;
            options.num_memtables = NovaConfig::config->num_memtables;
            options.l0_stop_writes_trigger = NovaConfig::config->cc_l0_stop_write;
            options.max_open_files = 50000;
            options.enable_table_locator = NovaConfig::config->enable_table_locator;
            options.num_recovery_thread = NovaConfig::config->number_of_recovery_threads;

            if (NovaConfig::config->cc_l0_stop_write == 0) {
                options.l0_stop_writes_trigger = UINT32_MAX;
            }
            options.max_dc_file_size =
                    std::max(options.write_buffer_size, options.max_file_size) +
                    LEVELDB_TABLE_PADDING_SIZE_MB * 1024 * 1024;
            options.env = env;
            options.create_if_missing = true;
            options.compression = leveldb::kNoCompression;
            options.filter_policy = leveldb::NewBloomFilterPolicy(10);
            options.bg_threads = bg_threads;
            options.enable_tracing = false;
            options.comparator = new YCSBKeyComparator();
            if (NovaConfig::config->memtable_type == "pool") {
                options.memtable_type = leveldb::MemTableType::kMemTablePool;
            } else {
                options.memtable_type = leveldb::MemTableType::kStaticPartition;
            }
            options.enable_subranges = NovaConfig::config->enable_subrange;
            options.subrange_reorg_sampling_ratio = 1.0;
            options.reorg_thread = reorg_thread;
            options.enable_major = NovaConfig::config->enable_major_compaction;

            uint32_t stocid = db_index % NovaConfig::config->dc_servers.size();
            options.manifest_stoc_id = NovaConfig::config->dc_servers[stocid].server_id;

            leveldb::Logger *log = nullptr;
            std::string db_path = DBName(NovaConfig::config->db_path,
                                         NovaConfig::config->my_server_id,
                                         db_index);
            mkdirs(db_path.c_str());

            RDMA_ASSERT(env->NewLogger(
                    db_path + "/LOG-" + std::to_string(db_index), &log).ok());
            options.info_log = log;
            leveldb::Status status = leveldb::DB::Open(options, db_path, &db);
            RDMA_ASSERT(status.ok()) << "Open leveldb failed "
                                     << status.ToString();

            uint32_t index = 0;
            uint32_t sid = 0;
            std::string logname = leveldb::LogFileName(db_path, 1111);
            ParseDBIndexFromFile(logname, &sid, &index);
            RDMA_ASSERT(index == db_index);
            RDMA_ASSERT(NovaConfig::config->my_server_id == sid);
            return db;
        }
    }


    void start(NovaCCConnWorker *store) {
        store->Start();
    }

    NovaCCLoadThread::NovaCCLoadThread(std::vector<leveldb::DB *> &dbs,
                                       std::vector<nova::NovaRDMAComputeComponent *> &async_workers,
                                       nova::NovaMemManager *mem_manager,
                                       std::set<uint32_t> &assigned_dbids,
                                       uint32_t tid) : dbs_(dbs),
                                                       async_workers_(
                                                               async_workers),
                                                       mem_manager_(
                                                               mem_manager),
                                                       assigned_dbids_(
                                                               assigned_dbids),
                                                       tid_(tid) {

    }

    uint64_t NovaCCLoadThread::LoadDataWithRangePartition() {
        // load data.
        timeval start{};
        gettimeofday(&start, nullptr);
        uint64_t loaded_keys = 0;
        std::vector<CCFragment *> &frags = NovaConfig::config->fragments;
        leveldb::WriteState *state = new leveldb::WriteState[NovaConfig::config->servers.size()];
        for (int i = 0; i < NovaConfig::config->servers.size(); i++) {
            state[i].rdma_wr_id = -1;
            state[i].result = leveldb::WriteResult::REPLICATE_LOG_RECORD_NONE;
        }

        unsigned int rand_seed = tid_;
        auto client = new leveldb::NovaBlockCCClient(tid_);
        client->ccs_ = async_workers_;

        int pivot = 0;
        int i = pivot;
        int loaded_frags = 0;
        while (loaded_frags < frags.size()) {
            if (frags[i]->cc_server_id !=
                NovaConfig::config->my_server_id) {
                loaded_frags++;
                i = (i + 1) % frags.size();
                continue;
            }

            uint32_t dbid = frags[i]->dbid;
            if (assigned_dbids_.find(dbid) == assigned_dbids_.end()) {
                loaded_frags++;
                i = (i + 1) % frags.size();
                continue;
            }

            // Insert cold keys first so that hot keys will be at the top level.
            leveldb::DB *db = dbs_[frags[i]->dbid];
            RDMA_LOG(INFO) << fmt::format("t[{}] Insert range {} to {}", tid_,
                                          frags[i]->range.key_start,
                                          frags[i]->range.key_end);
            for (uint64_t j = frags[i]->range.key_end;
                 j >= frags[i]->range.key_start; j--) {
                auto v = static_cast<char>((j % 10) + 'a');

                std::string key(std::to_string(j));
                std::string val(
                        NovaConfig::config->load_default_value_size, v);

                for (int i = 0; i < NovaConfig::config->servers.size(); i++) {
                    state[i].rdma_wr_id = -1;
                    state[i].result = leveldb::WriteResult::REPLICATE_LOG_RECORD_NONE;
                }
                leveldb::WriteOptions option;
                option.sync = true;
                option.hash = j;
                option.rand_seed = &rand_seed;
                option.dc_client = client;
                option.thread_id = tid_;
                option.local_write = true;
                option.replicate_log_record_states = state;
                // DO NOT update subranges since this is not the actual workload.
                option.update_subranges = false;

                leveldb::Status s = db->Put(option, key, val);
                RDMA_ASSERT(s.ok());
                loaded_keys++;
                if (loaded_keys % 100000 == 0) {
                    timeval now{};
                    gettimeofday(&now, nullptr);
                    RDMA_LOG(INFO)
                        << fmt::format("t[{}]: Load {} entries took {}", tid_,
                                       loaded_keys,
                                       (now.tv_sec - start.tv_sec));
                }

                if (j == frags[i]->range.key_start) {
                    break;
                }
            }

            loaded_frags++;
            i = (i + 1) % frags.size();
        }
        RDMA_LOG(INFO)
            << fmt::format("t[{}]: Completed loading data {}", tid_,
                           loaded_keys);
        return loaded_keys;
    }

    void NovaCCLoadThread::VerifyLoad() {
        auto client = new leveldb::NovaBlockCCClient(tid_);
        client->ccs_ = async_workers_;
        leveldb::ReadOptions read_options = {};
        read_options.mem_manager = mem_manager_;
        read_options.dc_client = client;

        read_options.thread_id = tid_;
        read_options.verify_checksums = false;
        std::vector<CCFragment *> &frags = NovaConfig::config->fragments;
        for (int i = 0; i < frags.size(); i++) {
            if (frags[i]->cc_server_id !=
                NovaConfig::config->my_server_id) {
                continue;
            }
            leveldb::DB *db = dbs_[frags[i]->dbid];

            RDMA_LOG(INFO) << fmt::format("t[{}] Verify range {} to {}", tid_,
                                          frags[i]->range.key_start,
                                          frags[i]->range.key_end);

            for (uint64_t j = frags[i]->range.key_end;
                 j >= frags[i]->range.key_start; j--) {
                auto v = static_cast<char>((j % 10) + 'a');
                std::string key = std::to_string(j);
                std::string expected_val(
                        NovaConfig::config->load_default_value_size, v
                );
                std::string value;
                leveldb::Status s = db->Get(read_options, key, &value);
                RDMA_ASSERT(s.ok()) << s.ToString();

                leveldb::Status status = db->Get(read_options, key, &value);
                RDMA_ASSERT(status.ok())
                    << fmt::format("key:{} status:{}", key, status.ToString());
                RDMA_ASSERT(expected_val.compare(value) == 0) << value;

                if (j == frags[i]->range.key_start) {
                    break;
                }
            }
            RDMA_LOG(INFO)
                << fmt::format("t[{}]: Success: Verified range {} to {}", tid_,
                               frags[i]->range.key_start,
                               frags[i]->range.key_end);
        }
    }

    void NovaCCLoadThread::Start() {
        timeval start{};
        gettimeofday(&start, nullptr);

        uint64_t puts = 0;
        int iter = 2;
        if (NovaConfig::config->num_mem_partitions == 1) {
            iter = 1;
        }
        for (int i = 0; i < iter; i++) {
            puts += LoadDataWithRangePartition();
        }

        timeval end{};
        gettimeofday(&end, nullptr);

        throughput = puts / std::max((int) (end.tv_sec - start.tv_sec), 1);
    }

    void NovaCCNICServer::LoadData() {
        if (!NovaConfig::config->use_multiple_disks) {
            for (int i = 0;
                 i < NovaConfig::config->dc_servers.size(); i++) {
                if (NovaConfig::config->my_server_id ==
                    NovaConfig::config->dc_servers[i].server_id) {
                    return;
                }
            }
        }

        uint32_t nloading_threads = std::min(1, (int) dbs_.size());
        RDMA_ASSERT(dbs_.size() >= nloading_threads &&
                    dbs_.size() % nloading_threads == 0);
        uint32_t ndb_per_thread = dbs_.size() / nloading_threads;
        uint32_t current_db_id = 0;

        RDMA_LOG(INFO)
            << fmt::format("{} dbs. {} dbs per load thread.", dbs_.size(),
                           ndb_per_thread);

        std::vector<std::thread> load_threads;
        std::vector<NovaCCLoadThread *> ts;
        for (int i = 0; i < nloading_threads; i++) {
            std::set<uint32_t> dbids;
            for (int i = 0; i < ndb_per_thread; i++) {
                dbids.insert(current_db_id);
                current_db_id += 1;
            }
            NovaCCLoadThread *t = new NovaCCLoadThread(dbs_, async_workers,
                                                       mem_manager, dbids, i);
            ts.push_back(t);
            load_threads.emplace_back(std::thread(&NovaCCLoadThread::Start, t));
        }

        timeval start{};
        gettimeofday(&start, nullptr);

        for (int i = 0; i < nloading_threads; i++) {
            load_threads[i].join();
        }

        timeval end{};
        gettimeofday(&end, nullptr);

        RDMA_LOG(INFO)
            << fmt::format("!!!!!!!!!!!!!!!!!!!!!!!Complete Load took {}",
                           (end.tv_sec - start.tv_sec));

        uint64_t thpt = 0;
        for (int i = 0; i < nloading_threads; i++) {
            RDMA_LOG(INFO)
                << fmt::format("t[{}],Throughput,{}", i, ts[i]->throughput);
            thpt += ts[i]->throughput;
        }
        RDMA_LOG(INFO) << fmt::format("Total throughput: {}", thpt);

//        ts.clear();
//        load_threads.clear();
//        for (int i = 0; i < nloading_threads; i++) {
//            std::set<uint32_t> dbids;
//            for (int i = 0; i < ndb_per_thread; i++) {
//                dbids.insert(current_db_id);
//                current_db_id += 1;
//            }
//            NovaCCLoadThread *t = new NovaCCLoadThread(dbs_, async_workers,
//                                                       mem_manager, dbids, i);
//            load_threads.emplace_back(
//                    std::thread(&NovaCCLoadThread::VerifyLoad, t));
//        }
//
//        for (int i = 0; i < nloading_threads; i++) {
//            load_threads[i].join();
//        }

        for (int i = 0; i < dbs_.size(); i++) {
            RDMA_LOG(INFO) << "Database " << i;
            std::string value;
            dbs_[i]->GetProperty("leveldb.sstables", &value);
            RDMA_LOG(INFO) << "\n" << value;
            value.clear();
            dbs_[i]->GetProperty("leveldb.approximate-memory-usage", &value);
            RDMA_LOG(INFO) << "\n" << "leveldb memory usage " << value;
        }

//        RDMA_ASSERT(false)
//            << fmt::format("Loading complete {}", (end.tv_sec - start.tv_sec));
    }

    NovaCCNICServer::NovaCCNICServer(RdmaCtrl *rdma_ctrl,
                                     char *rdmabuf, int nport)
            : nport_(nport) {
        std::map<uint32_t, std::set<uint32_t >> dbids = NovaConfig::ReadDatabases(
                NovaConfig::config->fragments);
        for (auto sid : dbids) {
            for (auto dbid : sid.second) {
                std::string db_path = DBName(NovaConfig::config->db_path,
                                             sid.first, dbid);
                mkdir(db_path.c_str(), 0777);
            }
        }
        int ndbs = NovaConfig::ParseNumberOfDatabases(
                NovaConfig::config->fragments,
                &NovaConfig::config->db_fragment,
                NovaConfig::config->my_server_id);

        char *buf = rdmabuf;
        char *cache_buf = buf + nrdma_buf_cc();

        uint32_t num_mem_partitions = 1;
        NovaConfig::config->num_mem_partitions = num_mem_partitions;
        uint64_t slab_size_mb = std::max(
                NovaConfig::config->sstable_size / 1024 / 1024,
                NovaConfig::config->write_buffer_size_mb) +
                                LEVELDB_TABLE_PADDING_SIZE_MB;

        mem_manager = new NovaMemManager(cache_buf,
                                         num_mem_partitions,
                                         NovaConfig::config->mem_pool_size_gb,
                                         slab_size_mb);
        log_manager = new InMemoryLogFileManager(mem_manager);

        NovaConfig::config->add_tid_mapping();
        int bg_thread_id = 0;
        for (int i = 0;
             i < NovaConfig::config->num_compaction_workers; i++) {
            auto bg = new leveldb::NovaCCCompactionThread(mem_manager);
            bgs.push_back(bg);
        }

        leveldb::Cache *block_cache = nullptr;
        leveldb::Cache *row_cache = nullptr;
        if (NovaConfig::config->block_cache_mb > 0) {
            uint64_t cache_size =
                    (uint64_t) (NovaConfig::config->block_cache_mb) *
                    1024 * 1024;
            block_cache = leveldb::NewLRUCache(cache_size);

            RDMA_LOG(INFO)
                << fmt::format("Block cache size {}. Configured size {} MB",
                               block_cache->TotalCapacity(),
                               NovaConfig::config->block_cache_mb);
        }
        if (NovaConfig::config->row_cache_mb > 0) {
            uint64_t row_cache_size =
                    (uint64_t) (NovaConfig::config->row_cache_mb) * 1024 *
                    1024;
            row_cache = leveldb::NewLRUCache(row_cache_size);
        }
        leveldb::MemTablePool *pool = new leveldb::MemTablePool;
        pool->num_available_memtables_ =
                NovaConfig::config->num_memtables;
        pool->range_cond_vars_ = new leveldb::port::CondVar *[ndbs];

        std::vector<nova::NovaMsgCallback *> rdma_threads;
        std::vector<leveldb::NovaBlockCCClient *> db_clients;
        for (int db_index = 0; db_index < ndbs; db_index++) {
            auto reorg = new leveldb::NovaCCCompactionThread(mem_manager);
            reorg_bgs.push_back(reorg);
            auto client = new leveldb::NovaBlockCCClient(db_index);
            dbs_.push_back(
                    CreateDatabase(db_index, block_cache, pool, mem_manager,
                                   client, bgs, reorg));
            db_clients.push_back(client);
        }
        for (int db_index = 0; db_index < ndbs; db_index++) {
            dbs_[db_index]->dbs_ = dbs_;
        }

        leveldb::EnvOptions env_option;
        env_option.sstable_mode = leveldb::NovaSSTableMode::SSTABLE_DISK;
        leveldb::PosixEnv *env = new leveldb::PosixEnv;
        env->set_env_option(env_option);
        std::vector<std::string> dbnames;
        for (auto sid : dbids) {
            for (auto dbid : sid.second) {
                dbnames.push_back(DBName(NovaConfig::config->db_path,
                                         sid.first, dbid));
            }
        }

        for (auto &dbname : dbnames) {
            mkdirs(dbname.c_str());
        }

        uint32_t nranges = NovaConfig::config->fragments.size() /
                           NovaConfig::config->cc_servers.size();
        leveldb::NovaRTableManager *rtable_manager = new leveldb::NovaRTableManager(
                env, mem_manager, NovaConfig::config->rtable_path,
                NovaConfig::config->rtable_size,
                NovaConfig::config->servers.size(), nranges);
        int worker_id = 0;

        uint32_t max_req_id = UINT32_MAX - 1;
        uint32_t range_per_server =
                max_req_id / NovaConfig::config->cc_servers.size();
        uint32_t lower_client_req_id =
                1 + (NovaConfig::config->my_server_id * range_per_server);
        uint32 upper_client_req_id = lower_client_req_id + range_per_server;

        RDMA_LOG(INFO)
            << fmt::format("Request Id range {}:{}", lower_client_req_id,
                           upper_client_req_id);
        std::vector<NovaCCServer *> cc_servers;
        for (worker_id = 0;
             worker_id <
             NovaConfig::config->num_conn_async_workers; worker_id++) {
            NovaRDMAComputeComponent *cc = new NovaRDMAComputeComponent(
                    rdma_ctrl,
                    mem_manager,
                    dbs_);
            rdma_threads.push_back(cc);
            async_workers.push_back(cc);
            NovaRDMAStore *store = nullptr;
            std::vector<QPEndPoint> endpoints;
            for (int i = 0;
                 i < NovaConfig::config->servers.size(); i++) {
                if (i == NovaConfig::config->my_server_id) {
                    continue;
                }

                QPEndPoint qp;
                qp.host = NovaConfig::config->servers[i];
                qp.thread_id = worker_id;
                qp.server_id = i;
                endpoints.push_back(qp);
            }

            if (NovaConfig::config->enable_rdma) {
                store = new NovaRDMARCStore(buf, worker_id, endpoints,
                                            NovaConfig::config->rdma_max_num_sends,
                                            NovaConfig::config->max_msg_size,
                                            NovaConfig::config->rdma_doorbell_batch_size,
                                            NovaConfig::config->my_server_id,
                                            NovaConfig::config->nova_buf,
                                            NovaConfig::config->nnovabuf,
                                            NovaConfig::config->rdma_port,
                                            async_workers[worker_id]);
            } else {
                store = new NovaRDMANoopStore();
            }

            // Log writers.
            nova::NovaCCServer *cc_server = new nova::NovaCCServer(rdma_ctrl,
                                                                   mem_manager,
                                                                   rtable_manager,
                                                                   log_manager,
                                                                   worker_id,
                                                                   false);
            leveldb::CCClient *dc_client = new leveldb::NovaCCClient(worker_id,
                                                                     store,
                                                                     mem_manager,
                                                                     rtable_manager,
                                                                     new leveldb::RDMALogWriter(
                                                                             store,
                                                                             mem_manager,
                                                                             log_manager),
                                                                     lower_client_req_id,
                                                                     upper_client_req_id,
                                                                     cc_server);

            cc_servers.push_back(cc_server);
            cc_server->rdma_store_ = store;

            cc->thread_id_ = worker_id;
            cc->rdma_store_ = store;
            cc->cc_client_ = dc_client;
            cc->cc_server_ = cc_server;

            buf += nrdma_buf_unit() *
                   NovaConfig::config->servers.size();
        }

        for (int i = 0;
             i < NovaConfig::config->num_rdma_compaction_workers; i++) {
            NovaRDMAComputeComponent *cc = new NovaRDMAComputeComponent(
                    rdma_ctrl,
                    mem_manager,
                    dbs_);
            rdma_threads.push_back(cc);
            async_compaction_workers.push_back(cc);

            NovaRDMAStore *store = nullptr;
            std::vector<QPEndPoint> endpoints;
            for (int j = 0;
                 j < NovaConfig::config->servers.size(); j++) {
                if (j == NovaConfig::config->my_server_id) {
                    continue;
                }

                QPEndPoint qp;
                qp.host = NovaConfig::config->servers[j];
                qp.thread_id = worker_id;
                qp.server_id = j;
                endpoints.push_back(qp);
            }

            if (NovaConfig::config->enable_rdma) {
                store = new NovaRDMARCStore(buf, worker_id, endpoints,
                                            NovaConfig::config->rdma_max_num_sends,
                                            NovaConfig::config->max_msg_size,
                                            NovaConfig::config->rdma_doorbell_batch_size,
                                            NovaConfig::config->my_server_id,
                                            NovaConfig::config->nova_buf,
                                            NovaConfig::config->nnovabuf,
                                            NovaConfig::config->rdma_port,
                                            cc);
            } else {
                store = new NovaRDMANoopStore();
            }

            nova::NovaCCServer *cc_server = new nova::NovaCCServer(rdma_ctrl,
                                                                   mem_manager,
                                                                   rtable_manager,
                                                                   log_manager,
                                                                   worker_id,
                                                                   true);
            leveldb::CCClient *dc_client = new leveldb::NovaCCClient(worker_id,
                                                                     store,
                                                                     mem_manager,
                                                                     rtable_manager,
                                                                     new leveldb::RDMALogWriter(
                                                                             store,
                                                                             mem_manager,
                                                                             log_manager),
                                                                     lower_client_req_id,
                                                                     upper_client_req_id,
                                                                     cc_server);
            cc_servers.push_back(cc_server);
            cc_server->rdma_store_ = store;
            cc->rdma_store_ = store;
            cc->thread_id_ = worker_id;
            cc->cc_client_ = dc_client;
            cc->cc_server_ = cc_server;
            worker_id++;
            buf += nrdma_buf_unit() *
                   NovaConfig::config->servers.size();
        }

        for (int i = 0;
             i <
             NovaConfig::config->num_conn_workers; i++) {
            conn_workers.push_back(new NovaCCConnWorker(i));
            conn_workers[i]->set_dbs(dbs_);
            conn_workers[i]->mem_manager_ = mem_manager;

            uint32_t scid = mem_manager->slabclassid(0, MAX_BLOCK_SIZE);
            conn_workers[i]->rdma_backing_mem = mem_manager->ItemAlloc(0, scid);

            conn_workers[i]->cc_client_ = new leveldb::NovaBlockCCClient(i);
            conn_workers[i]->cc_client_->ccs_ = async_workers;
            conn_workers[i]->rdma_threads = rdma_threads;
            conn_workers[i]->ctrl_ = rdma_ctrl;
            conn_workers[i]->rtable_manager_ = rtable_manager;
        }

        for (int i = 0;
             i <
             NovaConfig::config->num_compaction_workers; i++) {
            auto bg = static_cast<leveldb::NovaCCCompactionThread *>(bgs[i]);
            bg->cc_client_ = new leveldb::NovaBlockCCClient(i);
            bg->cc_client_->ccs_ = async_compaction_workers;
            bg->thread_id_ = i;
        }

        RDMA_ASSERT(buf == cache_buf);

        for (int i = 0;
             i < NovaConfig::config->num_cc_server_workers; i++) {
            NovaCCServerAsyncWorker *worker = new NovaCCServerAsyncWorker(
                    rtable_manager, cc_servers);
            cc_server_workers.push_back(worker);
        }

        // Assign workers to cc servers.
        for (int i = 0; i < cc_servers.size(); i++) {
            cc_servers[i]->async_workers_ = cc_server_workers;
        }

        for (int i = 0; i < reorg_bgs.size(); i++) {
            auto bg = reinterpret_cast<leveldb::NovaCCCompactionThread *>(reorg_bgs[i]);
            reorg_workers.emplace_back(&leveldb::NovaCCCompactionThread::Start,
                                       bg);
        }

        for (int db_index = 0; db_index < ndbs; db_index++) {
            dbs_[db_index]->rdma_threads_ = rdma_threads;
        }

        // Start the threads.
        for (int i = 0;
             i <
             NovaConfig::config->num_conn_async_workers; i++) {
            cc_workers.emplace_back(&NovaRDMAComputeComponent::Start,
                                    async_workers[i]);
        }
        for (int i = 0;
             i < NovaConfig::config->num_rdma_compaction_workers; i++) {
            cc_workers.emplace_back(&NovaRDMAComputeComponent::Start,
                                    async_compaction_workers[i]);
        }

        for (int i = 0;
             i < NovaConfig::config->num_compaction_workers; i++) {
            auto bg = reinterpret_cast<leveldb::NovaCCCompactionThread *>(bgs[i]);
            compaction_workers.emplace_back(
                    &leveldb::NovaCCCompactionThread::Start, bg);
        }
        for (int i = 0;
             i < NovaConfig::config->num_cc_server_workers; i++) {
            cc_server_async_workers.emplace_back(
                    &NovaCCServerAsyncWorker::Start, cc_server_workers[i]);
        }

        // Wait for all RDMA connections to setup.
        bool all_initialized = false;
        while (!all_initialized) {
            all_initialized = true;
            for (const auto &worker : async_workers) {
                if (!worker->IsInitialized()) {
                    all_initialized = false;
                    break;
                }
            }
            if (!all_initialized) {
                continue;
            }
            for (const auto &worker : async_compaction_workers) {
                if (!worker->IsInitialized()) {
                    all_initialized = false;
                    break;
                }
            }
            if (!all_initialized) {
                continue;
            }
            for (const auto &worker : bgs) {
                if (!worker->IsInitialized()) {
                    all_initialized = false;
                    break;
                }
            }
            usleep(10000);
        }

        for (int db_index = 0; db_index < ndbs; db_index++) {
            db_clients[db_index]->ccs_ = async_compaction_workers;
        }

        if (NovaConfig::config->recover_dbs) {
            for (int db_index = 0; db_index < ndbs; db_index++) {
                RDMA_ASSERT(dbs_[db_index]->Recover().ok());
            }
        }

        if (NovaConfig::config->enable_load_data) {
            LoadData();
        }

        for (auto db : dbs_) {
            db->StartTracing();

            db->processed_writes_ = 0;
            db->number_of_puts_no_wait_ = 0;
            db->number_of_puts_wait_ = 0;
            db->number_of_steals_ = 0;
            db->number_of_wait_due_to_contention_ = 0;
            db->number_of_gets_ = 0;
            db->number_of_memtable_hits_ = 0;
        }

        stat_thread_ = new NovaStatThread;
        stat_thread_->cc_server_workers_ = cc_server_workers;
        stat_thread_->bgs_ = bgs;
        stat_thread_->async_workers_ = async_workers;
        stat_thread_->async_compaction_workers_ = async_compaction_workers;
        stat_thread_->dbs_ = dbs_;
        stats_t_.emplace_back(
                std::thread(&NovaStatThread::Start, stat_thread_));

        // Start connection threads in the end after we have loaded all data.
        for (int i = 0;
             i <
             NovaConfig::config->num_conn_workers; i++) {
            conn_worker_threads.emplace_back(start, conn_workers[i]);
        }
        current_conn_worker_id_ = 0;

        usleep(1000000);
        nova::NovaConfig::config->print_mapping();
    }

    void make_socket_non_blocking(int sockfd) {
        int flags = fcntl(sockfd, F_GETFL, 0);
        if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) == -1) {
        }
    }

    void on_accept(int fd, short which, void *arg) {
        auto *server = (NovaCCNICServer *) arg;
        RDMA_ASSERT(fd == server->listen_fd_);
        RDMA_LOG(DEBUG) << "new connection " << fd;

        int client_fd;
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        client_fd = accept(fd, (struct sockaddr *) &client_addr, &client_len);
        RDMA_ASSERT(client_fd < NOVA_MAX_CONN) << client_fd
                                               << " not enough connections";
        RDMA_ASSERT(client_fd >= 0) << client_fd;
        make_socket_non_blocking(client_fd);
        RDMA_LOG(DEBUG) << "register " << client_fd;

        NovaCCConnWorker *store = server->conn_workers[server->current_conn_worker_id_];
        if (NovaConfig::config->num_conn_workers == 1) {
            server->current_conn_worker_id_ = 0;
        } else {
            server->current_conn_worker_id_ =
                    (server->current_conn_worker_id_ + 1) %
                    NovaConfig::config->num_conn_workers;
        }

        store->conn_mu.lock();
        store->conn_queue.push_back(client_fd);
        store->conn_mu.unlock();

//    char buf[1];
//    buf[0] = 'c';
//    write(store->on_new_conn_send_fd, buf, 1);
    }

    void NovaCCNICServer::Start() {
        SetupListener();
        struct event event{};
        struct event_config *ev_config;
        ev_config = event_config_new();
        RDMA_ASSERT(
                event_config_set_flag(ev_config, EVENT_BASE_FLAG_NOLOCK) == 0);
        RDMA_ASSERT(event_config_avoid_method(ev_config, "poll") == 0);
        RDMA_ASSERT(event_config_avoid_method(ev_config, "select") == 0);
        RDMA_ASSERT(event_config_set_flag(ev_config,
                                          EVENT_BASE_FLAG_EPOLL_USE_CHANGELIST) ==
                    0);
        base = event_base_new_with_config(ev_config);

        if (!base) {
            fprintf(stderr, "Can't allocate event base\n");
            exit(1);
        }

        RDMA_LOG(INFO) << "Using Libevent with backend method "
                       << event_base_get_method(base);
        const int f = event_base_get_features(base);
        if ((f & EV_FEATURE_ET)) {
            RDMA_LOG(INFO) << "Edge-triggered events are supported.";
        }
        if ((f & EV_FEATURE_O1)) {
            RDMA_LOG(INFO) <<
                           "O(1) event notification is supported.";
        }
        if ((f & EV_FEATURE_FDS)) {
            RDMA_LOG(INFO) << "All FD types are supported.";
        }

        /* Listen for notifications from other threads */
        memset(&event, 0, sizeof(struct event));
        RDMA_ASSERT(event_assign(&event, base, listen_fd_, EV_READ | EV_PERSIST,
                                 on_accept, (void *) this) == 0);
        RDMA_ASSERT(event_add(&event, 0) == 0) << listen_fd_;
        RDMA_ASSERT(event_base_loop(base, 0) == 0) << listen_fd_;
        RDMA_LOG(INFO) << "started";
    }

    void NovaCCNICServer::SetupListener() {
        int one = 1;
        struct linger ling = {0, 0};
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        RDMA_ASSERT(fd != -1) << "create socket failed";

        /**********************************************************
         * internet socket address structure: our address and port
         *********************************************************/
        struct sockaddr_in sin{};
        sin.sin_family = AF_INET;
        sin.sin_addr.s_addr = INADDR_ANY;
        sin.sin_port = htons(nport_);

        /**********************************************************
         * bind socket to address and port
         *********************************************************/
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (void *) &one, sizeof(one));
        setsockopt(fd, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling));
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (void *) &one, sizeof(one));

        int ret = bind(fd, (struct sockaddr *) &sin, sizeof(sin));
        RDMA_ASSERT(ret != -1) << "bind port failed";

        /**********************************************************
         * put socket into listening state
         *********************************************************/
        ret = listen(fd, 65536);
        RDMA_ASSERT(ret != -1) << "listen socket failed";
        listen_fd_ = fd;
        make_socket_non_blocking(listen_fd_);
    }
}