
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
#include <db/db_impl.h>
#include "leveldb/cache.h"

#include "leveldb/write_batch.h"
#include "db/filename.h"
#include "nic_server.h"
#include "ltc/stoc_file_client_impl.h"
#include "util/env_posix.h"
#include "ltc/db_helper.h"

namespace nova {
    void start(NICClientReqWorker *store) {
        store->Start();
    }

    LoadThread::LoadThread(std::vector<nova::RDMAMsgHandler *> &async_workers, nova::NovaMemManager *mem_manager,
                           std::set<uint32_t> &assigned_dbids, uint32_t tid) : async_workers_(async_workers),
                                                                               mem_manager_(mem_manager),
                                                                               assigned_frags_(assigned_dbids),
                                                                               tid_(tid) {
    }

    uint64_t LoadThread::LoadDataWithRangePartition() {
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
        unsigned int rand_seed = tid_;
        auto client = new leveldb::StoCBlockClient(tid_, nullptr);
        client->rdma_msg_handlers_ = async_workers_;

        int pivot = 0;
        int i = pivot;
        int loaded_frags = 0;
        std::vector<leveldb::DB *> dbs;
        while (loaded_frags < frags.size()) {
            if (frags[i]->ltc_server_id != NovaConfig::config->my_server_id) {
                loaded_frags++;
                i = (i + 1) % frags.size();
                continue;
            }

            uint32_t dbid = frags[i]->dbid;
            if (assigned_frags_.find(dbid) == assigned_frags_.end()) {
                loaded_frags++;
                i = (i + 1) % frags.size();
                continue;
            }

            // Insert cold keys first so that hot keys will be at the top level.
            leveldb::DB *db = reinterpret_cast<leveldb::DB *>(frags[i]->db);
            dbs.push_back(db);
            NOVA_LOG(INFO) << fmt::format("t[{}] Insert range {} to {}", tid_,
                                          frags[i]->range.key_start,
                                          frags[i]->range.key_end);
            for (uint64_t j = frags[i]->range.key_end - 1;
                 j >= frags[i]->range.key_start; j--) {
                auto v = static_cast<char>((j % 10) + 'a');

                std::string key(std::to_string(j));
                std::string val(
                        NovaConfig::config->load_default_value_size, v);

                for (int k = 0; k < NovaConfig::config->servers.size(); k++) {
                    state[k].rdma_wr_id = -1;
                    state[k].result = leveldb::StoCReplicateLogRecordResult::REPLICATE_LOG_RECORD_NONE;
                }
                leveldb::WriteOptions option;
                option.hash = j;
                option.rand_seed = &rand_seed;
                option.stoc_client = client;
                option.thread_id = tid_;
                option.local_write = true;
                option.replicate_log_record_states = state;
                // DO NOT update subranges since this is not the actual workload.
                option.is_loading_db = true;

                leveldb::Status s = db->Put(option, key, val);
                NOVA_ASSERT(s.ok());
                loaded_keys++;
                if (loaded_keys % 100000 == 0) {
                    timeval now{};
                    gettimeofday(&now, nullptr);
                    NOVA_LOG(INFO)
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

        for (auto db : dbs) {
            auto dbimpl = reinterpret_cast<leveldb::DBImpl *>(db);
            dbimpl->is_loading_db_ = true;
            db->StartCoordinatedCompaction();
        }
        // Wait until there are no SSTables at L0.
        while (NovaConfig::config->major_compaction_type != "no") {
            uint32_t l0tables = 0;
            uint32_t nmemtables = 0;
            bool needs_compaction = false;
            for (auto db : dbs) {
                leveldb::DBStats stats;
                stats.sstable_size_dist = new uint32_t[20];
                db->QueryDBStats(&stats);
                if (!needs_compaction) {
                    needs_compaction = stats.needs_compaction;
                }
                l0tables += stats.num_l0_sstables;
                nmemtables += db->FlushMemTables(true);
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
        for (auto db : dbs) {
            auto dbimpl = reinterpret_cast<leveldb::DBImpl *>(db);
            dbimpl->is_loading_db_ = false;
        }
        NOVA_LOG(INFO)
            << fmt::format("t[{}]: Completed loading data {}", tid_,
                           loaded_keys);
        return loaded_keys;
    }

    void LoadThread::VerifyLoad() {
        auto client = new leveldb::StoCBlockClient(tid_, nullptr);
        client->rdma_msg_handlers_ = async_workers_;
        leveldb::ReadOptions read_options = {};
        read_options.mem_manager = mem_manager_;
        read_options.stoc_client = client;

        read_options.thread_id = tid_;
        read_options.verify_checksums = false;
        std::vector<LTCFragment *> &frags = NovaConfig::config->cfgs[0]->fragments;
        for (int i = 0; i < frags.size(); i++) {
            if (frags[i]->ltc_server_id != NovaConfig::config->my_server_id) {
                continue;
            }
            leveldb::DB *db = reinterpret_cast<leveldb::DB *>(frags[i]->db);
            NOVA_LOG(INFO) << fmt::format("t[{}] Verify range {} to {}", tid_,
                                          frags[i]->range.key_start,
                                          frags[i]->range.key_end);

            for (uint64_t j = frags[i]->range.key_end - 1;
                 j >= frags[i]->range.key_start; j--) {
                auto v = static_cast<char>((j % 10) + 'a');
                std::string key = std::to_string(j);
                std::string expected_val(
                        NovaConfig::config->load_default_value_size, v
                );
                std::string value;
                leveldb::Status s = db->Get(read_options, key, &value);
                NOVA_ASSERT(s.ok()) << s.ToString();

                leveldb::Status status = db->Get(read_options, key, &value);
                NOVA_ASSERT(status.ok())
                    << fmt::format("key:{} status:{}", key, status.ToString());
                NOVA_ASSERT(expected_val.compare(value) == 0) << value;

                if (j == frags[i]->range.key_start) {
                    break;
                }
            }
            NOVA_LOG(INFO)
                << fmt::format("t[{}]: Success: Verified range {} to {}", tid_,
                               frags[i]->range.key_start,
                               frags[i]->range.key_end);
        }
    }

    void LoadThread::Start() {
        timeval start{};
        gettimeofday(&start, nullptr);

        uint64_t puts = 0;
        int iter = 1;
//        if (NovaConfig::config->num_mem_partitions == 1) {
//            iter = 1;
//        }
        for (int i = 0; i < iter; i++) {
            puts += LoadDataWithRangePartition();
        }
        timeval end{};
        gettimeofday(&end, nullptr);
        throughput = puts / std::max((int) (end.tv_sec - start.tv_sec), 1);
    }

    void NICServer::LoadData() {
        if (!NovaConfig::config->use_local_disk) {
            if (NovaConfig::config->cfgs[0]->IsStoC()) {
                return;
            }
        }

        uint32_t nloading_threads = 1;
        uint32_t ndb_per_thread = dbs_.size() / nloading_threads;
        uint32_t current_db_id = 0;
        NOVA_LOG(INFO) << fmt::format("{} dbs. {} dbs per load thread.", dbs_.size(), ndb_per_thread);
        std::vector<std::thread> load_threads;
        std::vector<LoadThread *> ts;
        for (int i = 0; i < nloading_threads; i++) {
            std::set<uint32_t> dbids;
            for (int j = 0; j < ndb_per_thread; j++) {
                dbids.insert(current_db_id);
                current_db_id += 1;
            }
            auto t = new LoadThread(fg_rdma_msg_handlers, mem_manager, dbids, i);
            ts.push_back(t);
            load_threads.emplace_back(std::thread(&LoadThread::Start, t));
        }

        timeval start{};
        gettimeofday(&start, nullptr);

        for (int i = 0; i < nloading_threads; i++) {
            load_threads[i].join();
        }

        timeval end{};
        gettimeofday(&end, nullptr);

        NOVA_LOG(INFO)
            << fmt::format("!!!!!!!!!!!!!!!!!!!!!!!Complete Load took {}",
                           (end.tv_sec - start.tv_sec));

        uint64_t thpt = 0;
        for (int i = 0; i < nloading_threads; i++) {
            NOVA_LOG(INFO)
                << fmt::format("t[{}],Throughput,{}", i, ts[i]->throughput);
            thpt += ts[i]->throughput;
        }
        NOVA_LOG(INFO) << fmt::format("Total throughput: {}", thpt);

        for (int i = 0; i < dbs_.size(); i++) {
            if (!dbs_[i]) {
                return;
            }
            NOVA_LOG(INFO) << "Database " << i;
            std::string value;
            dbs_[i]->GetProperty("leveldb.sstables", &value);
            NOVA_LOG(INFO) << "\n" << value;
            value.clear();
            dbs_[i]->GetProperty("leveldb.approximate-memory-usage", &value);
            NOVA_LOG(INFO) << "\n" << "leveldb memory usage " << value;
        }
    }

    NICServer::NICServer(RdmaCtrl *rdma_ctrl,
                         char *rdmabuf, int nport) : nport_(nport) {
        Configuration *cfg = NovaConfig::config->cfgs[0];
        for (int i = 0; i < cfg->fragments.size(); i++) {
            std::string db_path = DBName(NovaConfig::config->db_path, cfg->fragments[i]->dbid);
            mkdir(db_path.c_str(), 0777);
        }
        char *buf = rdmabuf;
        char *cache_buf = buf + nrdma_buf_server();

        uint32_t num_mem_partitions = 1;
        NovaConfig::config->num_mem_partitions = num_mem_partitions;
        uint64_t slab_size_mb = NovaConfig::config->manifest_file_size / 1024 / 1024;
        mem_manager = new NovaMemManager(cache_buf,
                                         num_mem_partitions,
                                         NovaConfig::config->mem_pool_size_gb,
                                         slab_size_mb);
        log_manager = new StoCInMemoryLogFileManager(mem_manager);
        NovaConfig::config->add_tid_mapping();
        int bg_thread_id = 0;
        for (int i = 0; i < NovaConfig::config->num_compaction_workers; i++) {
            {
                auto bg = new leveldb::LTCCompactionThread(mem_manager);
                bg_flush_memtable_threads.push_back(bg);
            }
            {
                auto bg = new leveldb::LTCCompactionThread(mem_manager);
                bg_compaction_threads.push_back(bg);
            }
        }

        leveldb::Cache *block_cache = nullptr;
        leveldb::Cache *row_cache = nullptr;
        if (NovaConfig::config->block_cache_mb > 0) {
            uint64_t cache_size =
                    (uint64_t) (NovaConfig::config->block_cache_mb) *
                    1024 * 1024;
            block_cache = leveldb::NewLRUCache(cache_size);

            NOVA_LOG(INFO)
                << fmt::format("Block cache size {}. Configured size {} MB",
                               block_cache->TotalCapacity(),
                               NovaConfig::config->block_cache_mb);
        }
        leveldb::MemTablePool *pool = new leveldb::MemTablePool;
        pool->num_available_memtables_ = NovaConfig::config->num_memtables;
        pool->range_cond_vars_ = new leveldb::port::CondVar *[cfg->fragments.size()];

        leveldb::EnvOptions env_option;
        env_option.sstable_mode = leveldb::NovaSSTableMode::SSTABLE_DISK;
        leveldb::PosixEnv *env = new leveldb::PosixEnv;
        env->set_env_option(env_option);

        leveldb::StocPersistentFileManager *stoc_file_manager = new leveldb::StocPersistentFileManager(env, mem_manager,
                                                                                                       NovaConfig::config->stoc_files_path,
                                                                                                       NovaConfig::config->max_stoc_file_size);
        std::vector<nova::RDMAMsgCallback *> rdma_threads;
        for (int db_index = 0; db_index < cfg->fragments.size(); db_index++) {
            if (NovaConfig::config->cfgs[0]->fragments[db_index]->ltc_server_id != NovaConfig::config->my_server_id) {
                dbs_.push_back(nullptr);
                continue;
            }
            auto reorg = new leveldb::LTCCompactionThread(mem_manager);
            auto coord = new leveldb::LTCCompactionThread(mem_manager);
            auto client = new leveldb::StoCBlockClient(db_index, stoc_file_manager);
            dbs_.push_back(CreateDatabase(0, db_index, block_cache, pool, mem_manager, client, bg_compaction_threads,
                                          bg_flush_memtable_threads, reorg, coord));
        }
        for (int db_index = 0; db_index < cfg->fragments.size(); db_index++) {
            NovaConfig::config->cfgs[0]->fragments[db_index]->db = dbs_[db_index];
        }

        // Assign request id space so that they won't conflict.
        int worker_id = 0;
        uint32_t max_req_id = UINT32_MAX - 1;
        uint32_t range_per_server =
                max_req_id / NovaConfig::config->servers.size();
        uint32_t lower_client_req_id =
                1 + (NovaConfig::config->my_server_id * range_per_server);
        uint32 upper_client_req_id = lower_client_req_id + range_per_server;

        NOVA_LOG(INFO)
            << fmt::format("Request Id range {}:{}", lower_client_req_id,
                           upper_client_req_id);
        std::vector<RDMAServerImpl *> rdma_servers;
        for (worker_id = 0; worker_id < NovaConfig::config->num_fg_rdma_workers; worker_id++) {
            RDMAAdmissionCtrl *admission_ctrl = new RDMAAdmissionCtrl;
            RDMAMsgHandler *rdma_msg_handler = new RDMAMsgHandler(rdma_ctrl, mem_manager, admission_ctrl);
            rdma_threads.push_back(rdma_msg_handler);
            fg_rdma_msg_handlers.push_back(rdma_msg_handler);
            NovaRDMABroker *broker = nullptr;
            std::vector<QPEndPoint> endpoints;
            for (int i = 0; i < NovaConfig::config->servers.size(); i++) {
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
                broker = new NovaRDMARCBroker(buf, worker_id, endpoints,
                                              NovaConfig::config->servers.size(),
                                              NovaConfig::config->rdma_max_num_sends,
                                              NovaConfig::config->max_msg_size,
                                              NovaConfig::config->rdma_doorbell_batch_size,
                                              NovaConfig::config->my_server_id,
                                              NovaConfig::config->nova_buf,
                                              NovaConfig::config->nnovabuf,
                                              NovaConfig::config->rdma_port,
                                              fg_rdma_msg_handlers[worker_id]);
            } else {
                broker = new NovaRDMANoopBroker();
            }

            // Log writers.
            nova::RDMAServerImpl *rdma_server = new nova::RDMAServerImpl(
                    rdma_ctrl,
                    mem_manager,
                    stoc_file_manager,
                    log_manager,
                    worker_id,
                    false,
                    admission_ctrl);
            auto log_writer = new leveldb::LogCLogWriter(broker, mem_manager,
                                                         log_manager);
            leveldb::StoCRDMAClient *stoc_client = new leveldb::StoCRDMAClient(
                    worker_id,
                    broker,
                    mem_manager,
                    log_writer,
                    lower_client_req_id,
                    upper_client_req_id,
                    rdma_server);

            rdma_servers.push_back(rdma_server);
            rdma_server->rdma_broker_ = broker;
            log_writer->admission_control_ = admission_ctrl;
            stoc_client->rdma_msg_handler_ = rdma_msg_handler;
            rdma_msg_handler->thread_id_ = worker_id;
            rdma_msg_handler->rdma_broker_ = broker;
            rdma_msg_handler->stoc_client_ = stoc_client;
            rdma_msg_handler->rdma_log_writer_ = log_writer;
            rdma_msg_handler->rdma_server_ = rdma_server;

            buf += nrdma_buf_unit() * NovaConfig::config->servers.size();
        }

        for (int i = 0; i < NovaConfig::config->num_bg_rdma_workers; i++) {
            RDMAAdmissionCtrl *admission_ctrl = new RDMAAdmissionCtrl;
            RDMAMsgHandler *cc = new RDMAMsgHandler(rdma_ctrl, mem_manager, admission_ctrl);
            rdma_threads.push_back(cc);
            bg_rdma_msg_handlers.push_back(cc);
            NovaRDMABroker *broker = nullptr;
            std::vector<QPEndPoint> endpoints;
            for (int j = 0; j < NovaConfig::config->servers.size(); j++) {
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
                broker = new NovaRDMARCBroker(buf, worker_id, endpoints,
                                              NovaConfig::config->servers.size(),
                                              NovaConfig::config->rdma_max_num_sends,
                                              NovaConfig::config->max_msg_size,
                                              NovaConfig::config->rdma_doorbell_batch_size,
                                              NovaConfig::config->my_server_id,
                                              NovaConfig::config->nova_buf,
                                              NovaConfig::config->nnovabuf,
                                              NovaConfig::config->rdma_port,
                                              cc);
            } else {
                broker = new NovaRDMANoopBroker();
            }
            nova::RDMAServerImpl *rdma_server = new nova::RDMAServerImpl(
                    rdma_ctrl,
                    mem_manager,
                    stoc_file_manager,
                    log_manager,
                    worker_id,
                    true,
                    admission_ctrl);
            auto log_writer = new leveldb::LogCLogWriter(broker, mem_manager,
                                                         log_manager);
            leveldb::StoCRDMAClient *stoc_client = new leveldb::StoCRDMAClient(
                    worker_id,
                    broker,
                    mem_manager,
                    log_writer,
                    lower_client_req_id,
                    upper_client_req_id,
                    rdma_server);
            stoc_client->rdma_msg_handler_ = cc;
            log_writer->admission_control_ = admission_ctrl;
            rdma_servers.push_back(rdma_server);
            rdma_server->rdma_broker_ = broker;
            cc->rdma_broker_ = broker;
            cc->thread_id_ = worker_id;
            cc->stoc_client_ = stoc_client;
            cc->rdma_log_writer_ = log_writer;
            cc->rdma_server_ = rdma_server;
            worker_id++;
            buf += nrdma_buf_unit() * NovaConfig::config->servers.size();
        }

        for (int i = 0; i < NovaConfig::config->num_migration_threads; i++) {
            auto client = new leveldb::StoCBlockClient(i, stoc_file_manager);
            client->rdma_msg_handlers_ = bg_rdma_msg_handlers;
            DBMigration *migrate = new DBMigration(mem_manager, client, log_manager, stoc_file_manager,
                                                   bg_rdma_msg_handlers, bg_compaction_threads,
                                                   bg_flush_memtable_threads);
            db_migration_threads.push_back(migrate);
            db_migrate_workers.emplace_back(&DBMigration::Start, migrate);
        }

        for (auto rdma_server : rdma_servers) {
            nova::RDMAWriteHandler *write_handler = new nova::RDMAWriteHandler(db_migration_threads);
            rdma_server->rdma_write_handler_ = write_handler;
        }

        for (int i = 0; i < NovaConfig::config->num_conn_workers; i++) {
            conn_workers.push_back(new NICClientReqWorker(i));
            conn_workers[i]->mem_manager_ = mem_manager;

            uint32_t scid = mem_manager->slabclassid(0, MAX_BLOCK_SIZE);
            conn_workers[i]->rdma_backing_mem = mem_manager->ItemAlloc(0, scid);
            conn_workers[i]->rdma_backing_mem_size = MAX_BLOCK_SIZE;
            memset(conn_workers[i]->rdma_backing_mem, 0, MAX_BLOCK_SIZE);

            conn_workers[i]->stoc_client_ = new leveldb::StoCBlockClient(i, stoc_file_manager);
            conn_workers[i]->stoc_client_->rdma_msg_handlers_ = fg_rdma_msg_handlers;
            conn_workers[i]->rdma_threads = rdma_threads;
            conn_workers[i]->ctrl_ = rdma_ctrl;
            conn_workers[i]->stoc_file_manager_ = stoc_file_manager;
            conn_workers[i]->db_migration_threads_ = db_migration_threads;
        }

        for (int i = 0; i < NovaConfig::config->num_compaction_workers; i++) {
            auto bg = static_cast<leveldb::LTCCompactionThread *>(bg_flush_memtable_threads[i]);
            bg->stoc_client_ = new leveldb::StoCBlockClient(i,
                                                            stoc_file_manager);
            bg->stoc_client_->rdma_msg_handlers_ = bg_rdma_msg_handlers;
            bg->thread_id_ = i;
        }
        for (int i = 0; i < NovaConfig::config->num_compaction_workers; i++) {
            auto bg = static_cast<leveldb::LTCCompactionThread *>(bg_compaction_threads[i]);
            bg->stoc_client_ = new leveldb::StoCBlockClient(i,
                                                            stoc_file_manager);
            bg->stoc_client_->rdma_msg_handlers_ = bg_rdma_msg_handlers;
            bg->thread_id_ = i;
        }
        NOVA_ASSERT(buf == cache_buf);

        leveldb::EnvOptions mem_env_option;
        mem_env_option.sstable_mode = leveldb::NovaSSTableMode::SSTABLE_MEM;
        leveldb::PosixEnv *mem_env = new leveldb::PosixEnv;
        mem_env->set_env_option(mem_env_option);
        auto user_comparator = new leveldb::YCSBKeyComparator();
        leveldb::Options storage_options = BuildStorageOptions(mem_manager,
                                                               mem_env);
        storage_options.comparator = new leveldb::InternalKeyComparator(
                user_comparator);
        for (int i = 0; i < NovaConfig::config->num_storage_workers; i++) {
            auto client = new leveldb::StoCBlockClient(i, stoc_file_manager);
            client->rdma_msg_handlers_ = bg_rdma_msg_handlers;
            StorageWorker *worker = new StorageWorker(
                    stoc_file_manager,
                    rdma_servers,
                    user_comparator,
                    storage_options,
                    client,
                    mem_manager,
                    i, mem_env);
            bg_storage_workers.push_back(worker);
        }
        for (int i = 0; i < NovaConfig::config->num_storage_workers; i++) {
            auto client = new leveldb::StoCBlockClient(i, stoc_file_manager);
            client->rdma_msg_handlers_ = fg_rdma_msg_handlers;
            StorageWorker *worker = new StorageWorker(
                    stoc_file_manager,
                    rdma_servers,
                    user_comparator,
                    storage_options,
                    client,
                    mem_manager,
                    i, mem_env);
            fg_storage_workers.push_back(worker);
        }
        for (int i = 0; i < NovaConfig::config->num_compaction_workers; i++) {
            auto client = new leveldb::StoCBlockClient(i, stoc_file_manager);
            client->rdma_msg_handlers_ = bg_rdma_msg_handlers;
            StorageWorker *worker = new StorageWorker(
                    stoc_file_manager,
                    rdma_servers,
                    user_comparator,
                    storage_options,
                    client,
                    mem_manager,
                    i, mem_env);
            compaction_storage_workers.push_back(worker);
        }

        // Assign workers to ltc servers.
        for (int i = 0; i < rdma_servers.size(); i++) {
            rdma_servers[i]->fg_storage_workers_ = fg_storage_workers;
            rdma_servers[i]->bg_storage_workers_ = bg_storage_workers;
            rdma_servers[i]->compaction_storage_workers_ = compaction_storage_workers;
        }

        for (int i = 0; i < dbs_.size(); i++) {
            if (!dbs_[i]) {
                continue;
            }
            auto db = reinterpret_cast<leveldb::DBImpl *>(dbs_[i]);
            auto reorg_thread = reinterpret_cast<leveldb::LTCCompactionThread *>(db->options_.reorg_thread);
            reorg_workers.emplace_back(&leveldb::LTCCompactionThread::Start, reorg_thread);
            auto coord_thread = reinterpret_cast<leveldb::LTCCompactionThread *>(db->options_.compaction_coordinator_thread);
            coord_thread->db_ = dbs_[i];
            coord_thread->stoc_client_ = new leveldb::StoCBlockClient(i, stoc_file_manager);
            coord_thread->stoc_client_->rdma_msg_handlers_ = bg_rdma_msg_handlers;
            coord_thread->thread_id_ = i;
            compaction_coord_workers.emplace_back(&leveldb::LTCCompactionThread::Start, coord_thread);
        }

        // Start the threads.
        if (NovaConfig::config->enable_rdma) {
            for (int i = 0; i < NovaConfig::config->num_fg_rdma_workers; i++) {
                fg_rdma_workers.emplace_back(&RDMAMsgHandler::Start, fg_rdma_msg_handlers[i]);
            }
            for (int i = 0; i < NovaConfig::config->num_bg_rdma_workers; i++) {
                fg_rdma_workers.emplace_back(&RDMAMsgHandler::Start, bg_rdma_msg_handlers[i]);
            }
        }
        for (int i = 0; i < NovaConfig::config->num_compaction_workers; i++) {
            auto bg = reinterpret_cast<leveldb::LTCCompactionThread *>(bg_flush_memtable_threads[i]);
            compaction_workers.emplace_back(&leveldb::LTCCompactionThread::Start, bg);
        }
        for (int i = 0; i < NovaConfig::config->num_compaction_workers; i++) {
            auto bg = reinterpret_cast<leveldb::LTCCompactionThread *>(bg_compaction_threads[i]);
            compaction_workers.emplace_back(&leveldb::LTCCompactionThread::Start, bg);
        }
        for (int i = 0; i < NovaConfig::config->num_storage_workers; i++) {
            storage_worker_threads.emplace_back(&StorageWorker::Start, fg_storage_workers[i]);
            storage_worker_threads.emplace_back(&StorageWorker::Start, bg_storage_workers[i]);
        }
        for (int i = 0; i < NovaConfig::config->num_compaction_workers; i++) {
            storage_worker_threads.emplace_back(&StorageWorker::Start, compaction_storage_workers[i]);
        }

        if (NovaConfig::config->enable_subrange_reorg && NovaConfig::config->use_ordered_flush) {
            auto client = new leveldb::StoCBlockClient(0, stoc_file_manager);
            client->rdma_msg_handlers_ = bg_rdma_msg_handlers;
            lsm_tree_cleaner_ = new leveldb::LSMTreeCleaner(log_manager, client);
            db_migrate_workers.emplace_back(&leveldb::LSMTreeCleaner::FlushingMemTables, lsm_tree_cleaner_);
        }

        if (NovaConfig::config->cfgs.size() > 1) {
            auto client = new leveldb::StoCBlockClient(0, stoc_file_manager);
            client->rdma_msg_handlers_ = bg_rdma_msg_handlers;
            lsm_tree_cleaner_ = new leveldb::LSMTreeCleaner(log_manager, client);
            db_migrate_workers.emplace_back(&leveldb::LSMTreeCleaner::CleanLSM, lsm_tree_cleaner_);
        }

        if (NovaConfig::config->cfgs.size() > 1) {
            auto client = new leveldb::StoCBlockClient(0, stoc_file_manager);
            client->rdma_msg_handlers_ = bg_rdma_msg_handlers;
            lsm_tree_cleaner_ = new leveldb::LSMTreeCleaner(log_manager, client);
            db_migrate_workers.emplace_back(&leveldb::LSMTreeCleaner::CleanLSMAfterCfgChange, lsm_tree_cleaner_);
        }

        // Wait for all RDMA connections to setup.
        bool all_initialized = false;
        while (!all_initialized) {
            all_initialized = true;
            if (NovaConfig::config->enable_rdma) {
                for (const auto &worker : fg_rdma_msg_handlers) {
                    if (!worker->IsInitialized()) {
                        all_initialized = false;
                        break;
                    }
                }
                if (!all_initialized) {
                    continue;
                }
                for (const auto &worker : bg_rdma_msg_handlers) {
                    if (!worker->IsInitialized()) {
                        all_initialized = false;
                        break;
                    }
                }
            }
            if (!all_initialized) {
                continue;
            }
            for (const auto &worker : bg_flush_memtable_threads) {
                if (!worker->IsInitialized()) {
                    all_initialized = false;
                    break;
                }
            }
            for (const auto &worker : bg_compaction_threads) {
                if (!worker->IsInitialized()) {
                    all_initialized = false;
                    break;
                }
            }
            usleep(10000);
        }

        for (int db_index = 0; db_index < cfg->fragments.size(); db_index++) {
            if (!dbs_[db_index]) {
                continue;
            }
            auto db = reinterpret_cast<leveldb::DBImpl *>(dbs_[db_index]);
            db->log_manager_ = log_manager;
            auto client = reinterpret_cast<leveldb::StoCBlockClient *>(db->options_.stoc_client);
            client->rdma_msg_handlers_ = bg_rdma_msg_handlers;
        }

        if (NovaConfig::config->recover_dbs) {
            for (int db_index = 0; db_index < cfg->fragments.size(); db_index++) {
                if (!dbs_[db_index]) {
                    continue;
                }
                NOVA_LOG(rdmaio::INFO) << fmt::format("!!!Recover database range {}", db_index);
                NOVA_ASSERT(dbs_[db_index]->Recover().ok());
            }
        }

        if (NovaConfig::config->enable_load_data) {
            LoadData();
        }

        for (auto db : dbs_) {
            if (!db) {
                continue;
            }
            db->StartTracing();
            db->processed_writes_ = 0;
            db->number_of_puts_no_wait_ = 0;
            db->number_of_puts_wait_ = 0;
            db->number_of_steals_ = 0;
            db->number_of_wait_due_to_contention_ = 0;
            db->number_of_gets_ = 0;
            db->number_of_memtable_hits_ = 0;
            db->StartCoordinatedCompaction();
        }

        stat_thread_ = new NovaStatThread;
        stat_thread_->bg_storage_workers_ = bg_storage_workers;
        stat_thread_->fg_storage_workers_ = fg_storage_workers;
        stat_thread_->compaction_storage_workers_ = compaction_storage_workers;
        stat_thread_->bgs_ = bg_flush_memtable_threads;

        stat_thread_->async_workers_ = fg_rdma_msg_handlers;
        stat_thread_->async_compaction_workers_ = bg_rdma_msg_handlers;
        stats_t_.emplace_back(std::thread(&NovaStatThread::Start, stat_thread_));

        NovaGlobalVariables::global.is_ready_to_process_requests = true;
        {
            // Wait for LTC to be ready for processing requests.
            leveldb::StoCBlockClient client(0, stoc_file_manager);
            client.rdma_msg_handlers_ = bg_rdma_msg_handlers;
            std::set<int> ready_ltcs;
            if (NovaConfig::config->use_local_disk || NovaConfig::config->cfgs[0]->IsLTC()) {
                ready_ltcs.insert(NovaConfig::config->my_server_id);
            }
            while (true) {
                for (auto &ltc : NovaConfig::config->cfgs[0]->ltc_servers) {
                    if (ready_ltcs.find(ltc) != ready_ltcs.end()) {
                        continue;
                    }
                    leveldb::StoCResponse response;
                    uint32_t req_id = client.InitiateIsReadyForProcessingRequests(
                            ltc);
                    client.Wait();
                    NOVA_ASSERT(client.IsDone(req_id, &response, nullptr));
                    NOVA_LOG(INFO)
                        << fmt::format("LTC-{} is ready? {}", ltc,
                                       response.is_ready_to_process_requests);
                    if (response.is_ready_to_process_requests) {
                        ready_ltcs.insert(ltc);
                    } else {
                        break;
                    }
                }
                if (ready_ltcs.size() == NovaConfig::config->cfgs[0]->ltc_servers.size()) {
                    break;
                }
                sleep(1);
            }
        }

        // Start connection threads in the end after we have loaded all data.
        for (int i = 0; i < NovaConfig::config->num_conn_workers; i++) {
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
        auto *server = (NICServer *) arg;
        NOVA_ASSERT(fd == server->listen_fd_);
        NOVA_LOG(DEBUG) << "new connection " << fd;

        int client_fd;
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        client_fd = accept(fd, (struct sockaddr *) &client_addr, &client_len);
        NOVA_ASSERT(client_fd < NOVA_MAX_CONN) << client_fd
                                               << " not enough connections";
        NOVA_ASSERT(client_fd >= 0) << client_fd;
        make_socket_non_blocking(client_fd);
        NOVA_LOG(DEBUG) << "register " << client_fd;

        NICClientReqWorker *store = server->conn_workers[server->current_conn_worker_id_];
        if (NovaConfig::config->num_conn_workers == 1) {
            server->current_conn_worker_id_ = 0;
        } else {
            server->current_conn_worker_id_ =
                    (server->current_conn_worker_id_ + 1) % NovaConfig::config->num_conn_workers;
        }

        store->conn_mu.lock();
        store->conn_queue.push_back(client_fd);
        store->conn_mu.unlock();
    }

    void NICServer::Start() {
        SetupListener();
        struct event event{};
        struct event_config *ev_config;
        ev_config = event_config_new();
        NOVA_ASSERT(event_config_set_flag(ev_config, EVENT_BASE_FLAG_NOLOCK) == 0);
        NOVA_ASSERT(event_config_avoid_method(ev_config, "poll") == 0);
        NOVA_ASSERT(event_config_avoid_method(ev_config, "select") == 0);
        NOVA_ASSERT(event_config_set_flag(ev_config, EVENT_BASE_FLAG_EPOLL_USE_CHANGELIST) == 0);
        base = event_base_new_with_config(ev_config);

        if (!base) {
            fprintf(stderr, "Can't allocate event base\n");
            exit(1);
        }

        NOVA_LOG(INFO) << "Using Libevent with backend method " << event_base_get_method(base);
        const int f = event_base_get_features(base);
        if ((f & EV_FEATURE_ET)) {
            NOVA_LOG(INFO) << "Edge-triggered events are supported.";
        }
        if ((f & EV_FEATURE_O1)) {
            NOVA_LOG(INFO) << "O(1) event notification is supported.";
        }
        if ((f & EV_FEATURE_FDS)) {
            NOVA_LOG(INFO) << "All FD types are supported.";
        }

        /* Listen for notifications from other threads */
        memset(&event, 0, sizeof(struct event));
        NOVA_ASSERT(event_assign(&event, base, listen_fd_, EV_READ | EV_PERSIST, on_accept, (void *) this) == 0);
        NOVA_ASSERT(event_add(&event, 0) == 0) << listen_fd_;
        NOVA_ASSERT(event_base_loop(base, 0) == 0) << listen_fd_;
        NOVA_LOG(INFO) << "started";
    }

    void NICServer::SetupListener() {
        int one = 1;
        struct linger ling = {0, 0};
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        NOVA_ASSERT(fd != -1) << "create socket failed";

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
        NOVA_ASSERT(ret != -1) << "bind port failed";

        /**********************************************************
         * put socket into listening state
         *********************************************************/
        ret = listen(fd, 65536);
        NOVA_ASSERT(ret != -1) << "listen socket failed";
        listen_fd_ = fd;
        make_socket_non_blocking(listen_fd_);
    }
}