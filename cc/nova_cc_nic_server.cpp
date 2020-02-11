
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <netinet/tcp.h>
#include <signal.h>
#include <fmt/core.h>

#include "leveldb/write_batch.h"
#include "db/filename.h"
#include "nova_cc_nic_server.h"
#include "nova_cc.h"
#include "util/env_posix.h"
#include "dc/nova_dc.h"
#include "mc/nova_mc_wb_worker.h"
#include "mc/nova_sstable.h"

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
                                    leveldb::EnvBGThread *bg_thread) {
            leveldb::EnvOptions env_option;
            env_option.sstable_mode = leveldb::NovaSSTableMode::SSTABLE_MEM;
            leveldb::PosixEnv *env = new leveldb::PosixEnv;
            env->set_env_option(env_option);
            leveldb::DB *db;
            leveldb::Options options;
            options.block_cache = cache;
            if (NovaCCConfig::cc_config->write_buffer_size_mb > 0) {
                options.write_buffer_size =
                        (uint64_t) (NovaCCConfig::cc_config->write_buffer_size_mb) *
                        1024 * 1024;
            }
            if (NovaConfig::config->sstable_size > 0) {
                options.max_file_size = NovaConfig::config->sstable_size;
            }

            options.max_dc_file_size =
                    std::max(options.write_buffer_size, options.max_file_size) +
                    1024 * 1024;
            options.env = env;
            options.create_if_missing = true;
            options.compression = leveldb::kSnappyCompression;
            options.filter_policy = leveldb::NewBloomFilterPolicy(10);
            options.bg_thread = bg_thread;
            options.enable_tracing = false;
            options.comparator = new YCSBKeyComparator();
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

    void NovaCCNICServer::LoadDataWithRangePartition() {
        // load data.
        timeval start{};
        gettimeofday(&start, nullptr);
        int loaded_keys = 0;
        std::vector<CCFragment *> &frags = NovaCCConfig::cc_config->fragments;
        leveldb::WriteOptions option;
        option.sync = true;

        for (int i = 0; i < frags.size(); i++) {
            if (frags[i]->cc_server_id !=
                NovaConfig::config->my_server_id) {
                continue;
            }
            leveldb::WriteBatch batch;
            int bs = 0;
            // Insert cold keys first so that hot keys will be at the top level.
            std::vector<std::string *> pointers;
            leveldb::DB *db = dbs_[frags[i]->dbid];

            RDMA_LOG(INFO) << "Insert " << frags[i]->range.key_start << " to "
                           << frags[i]->range.key_end;

            for (uint64_t j = frags[i]->range.key_end;
                 j >= frags[i]->range.key_start; j--) {
                auto v = static_cast<char>((j % 10) + 'a');

                std::string *key = new std::string(std::to_string(j));
                std::string *val = new std::string(
                        NovaConfig::config->load_default_value_size, v);
                pointers.push_back(key);
                pointers.push_back(val);
                batch.Put(*key, *val);
                bs += 1;

                if (bs == 1) {
                    leveldb::Status status = db->Write(option, &batch);
                    RDMA_ASSERT(status.ok()) << status.ToString();
                    batch.Clear();
                    bs = 0;
                    for (std::string *p : pointers) {
                        delete p;
                    }
                    pointers.clear();
                }
                loaded_keys++;
                if (loaded_keys % 100000 == 0) {
                    timeval now{};
                    gettimeofday(&now, nullptr);
                    RDMA_LOG(INFO) << "Load " << loaded_keys << " entries took "
                                   << now.tv_sec - start.tv_sec;
                }

                if (j == frags[i]->range.key_start) {
                    break;
                }
            }
            if (bs > 0) {
                leveldb::Status status = db->Write(option, &batch);
                RDMA_ASSERT(status.ok()) << status.ToString();
                for (std::string *p : pointers) {
                    delete p;
                }
            }
        }
        RDMA_LOG(INFO) << "Completed loading data " << loaded_keys;
    }

    void NovaCCNICServer::LoadData() {
        LoadDataWithRangePartition();
//        LoadDataWithRangePartition();
//        NovaAsyncTask task = {};
//        task.type = RequestType::VERIFY_LOAD;
//        async_workers[0]->AddTask(task);

        for (int i = 0; i < dbs_.size(); i++) {
            RDMA_LOG(INFO) << "Database " << i;
            std::string value;
            dbs_[i]->GetProperty("leveldb.sstables", &value);
            RDMA_LOG(INFO) << "\n" << value;
            value.clear();
            dbs_[i]->GetProperty("leveldb.approximate-memory-usage", &value);
            RDMA_LOG(INFO) << "\n" << "leveldb memory usage " << value;
        }
    }

    NovaCCNICServer::NovaCCNICServer(RdmaCtrl *rdma_ctrl,
                                     char *rdmabuf, int nport)
            : nport_(nport) {
        std::map<uint32_t, std::set<uint32_t >> dbids = NovaCCConfig::ReadDatabases(
                NovaCCConfig::cc_config->fragments);
        for (auto sid : dbids) {
            for (auto dbid : sid.second) {
                std::string db_path = DBName(NovaConfig::config->db_path,
                                             sid.first, dbid);
                mkdir(db_path.c_str(), 0777);
            }
        }

        char *buf = rdmabuf;
        char *cache_buf = buf + nrdma_buf_cc();
        manager = new NovaMemManager(cache_buf);
        log_manager = new LogFileManager(manager);

        int ndbs = NovaCCConfig::ParseNumberOfDatabases(
                NovaCCConfig::cc_config->fragments,
                &NovaCCConfig::cc_config->db_fragment,
                NovaConfig::config->my_server_id);
        int bg_thread_id = 0;
        for (int i = 0;
             i < NovaCCConfig::cc_config->num_compaction_workers; i++) {
            bgs.push_back(new leveldb::NovaCCCompactionThread(rdma_ctrl));
        }

        leveldb::Cache *block_cache = nullptr;
        if (NovaCCConfig::cc_config->block_cache_mb > 0) {
            uint64_t cache_size =
                    (uint64_t) (NovaCCConfig::cc_config->block_cache_mb) *
                    1024 * 1024;
            block_cache = leveldb::NewLRUCache(cache_size);
        }
        RDMA_LOG(INFO)
            << fmt::format("Block cache size {}. Configured size {} MB",
                           block_cache->TotalCapacity(),
                           NovaCCConfig::cc_config->block_cache_mb);

        for (int db_index = 0; db_index < ndbs; db_index++) {
            dbs_.push_back(
                    CreateDatabase(db_index, block_cache, bgs[bg_thread_id]));
            bg_thread_id += 1;
            bg_thread_id %= bgs.size();
        }
        NovaAsyncCompleteQueue **async_cq = new NovaAsyncCompleteQueue *[NovaCCConfig::cc_config->num_conn_workers];
        for (int worker_id = 0;
             worker_id <
             NovaCCConfig::cc_config->num_conn_workers; worker_id++) {
            async_cq[worker_id] = new NovaAsyncCompleteQueue;
            conn_workers.push_back(new NovaCCConnWorker(worker_id,
                                                        async_cq[worker_id]));
            conn_workers[worker_id]->set_dbs(dbs_);
            conn_workers[worker_id]->log_manager_ = log_manager;
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

        uint32_t nranges = NovaCCConfig::cc_config->fragments.size() /
                           NovaCCConfig::cc_config->cc_servers.size();
        leveldb::NovaRTableManager *rtable_manager = new leveldb::NovaRTableManager(
                env, manager, NovaConfig::config->rtable_path,
                NovaConfig::config->rtable_size,
                NovaConfig::config->servers.size(), nranges);
        int worker_id = 0;

        uint32_t max_req_id = UINT32_MAX - 1;
        uint32_t range_per_server =
                max_req_id / NovaCCConfig::cc_config->cc_servers.size();
        uint32_t lower_client_req_id =
                1 + (NovaConfig::config->my_server_id * range_per_server);
        uint32 upper_client_req_id = lower_client_req_id + range_per_server;

        RDMA_LOG(INFO)
            << fmt::format("Request Id range {}:{}", lower_client_req_id,
                           upper_client_req_id);
        std::vector<NovaCCServer *> cc_servers;
        for (worker_id = 0;
             worker_id <
             NovaCCConfig::cc_config->num_conn_async_workers; worker_id++) {
            NovaRDMAComputeComponent *cc = new NovaRDMAComputeComponent(
                    rdma_ctrl,
                    manager,
                    dbs_,
                    async_cq, true);
            async_workers.push_back(cc);
            cc->thread_id_ = worker_id;

            NovaRDMAStore *store = nullptr;
            std::vector<QPEndPoint> endpoints;
            for (int i = 0;
                 i < NovaCCConfig::cc_config->cc_servers.size(); i++) {
                if (i == NovaConfig::config->my_server_id) {
                    continue;
                }

                QPEndPoint qp;
                qp.host = NovaCCConfig::cc_config->cc_servers[i];
                qp.thread_id = worker_id;
                qp.server_id = i;
                endpoints.push_back(qp);
            }

            if (NovaConfig::config->enable_rdma) {
                store = new NovaRDMARCStore(buf, worker_id, endpoints,
                                            async_workers[worker_id]);
            } else {
                store = new NovaRDMANoopStore();
            }

            // Log writers.
            uint32_t scid = manager->slabclassid(worker_id,
                                                 NovaConfig::config->log_buf_size);
            char *rnic_buf = manager->ItemAlloc(worker_id, scid);
            RDMA_ASSERT(rnic_buf) << "Running out of memory";
            nova::NovaCCServer *cc_server = new nova::NovaCCServer(rdma_ctrl,
                                                                   manager,
                                                                   rtable_manager,
                                                                   log_manager,
                                                                   worker_id,
                                                                   false);
            leveldb::CCClient *dc_client = new leveldb::NovaCCClient(worker_id,
                                                                     store,
                                                                     manager,
                                                                     rtable_manager,
                                                                     new leveldb::log::RDMALogWriter(
                                                                             store,
                                                                             rnic_buf,
                                                                             manager,
                                                                             log_manager),
                                                                     lower_client_req_id,
                                                                     upper_client_req_id,
                                                                     cc_server);

            cc_servers.push_back(cc_server);
            cc_server->rdma_store_ = store;
            cc->rdma_store_ = store;
            cc->cc_client_ = dc_client;
            cc->cc_server_ = cc_server;

            buf += nrdma_buf_unit() *
                   NovaCCConfig::cc_config->cc_servers.size();
        }

        for (int i = 0;
             i < NovaCCConfig::cc_config->num_compaction_workers; i++) {
            NovaRDMAComputeComponent *cc = new NovaRDMAComputeComponent(
                    rdma_ctrl,
                    manager,
                    dbs_,
                    async_cq, false);
            cc->thread_id_ = worker_id;

            NovaRDMAStore *store = nullptr;
            std::vector<QPEndPoint> endpoints;
            for (int j = 0;
                 j < NovaCCConfig::cc_config->cc_servers.size(); j++) {
                if (j == NovaConfig::config->my_server_id) {
                    continue;
                }

                QPEndPoint qp;
                qp.host = NovaCCConfig::cc_config->cc_servers[j];
                qp.thread_id = worker_id;
                qp.server_id = j;
                endpoints.push_back(qp);
            }

            if (NovaConfig::config->enable_rdma) {
                store = new NovaRDMARCStore(buf, worker_id, endpoints, cc);
            } else {
                store = new NovaRDMANoopStore();
            }

            uint32_t scid = manager->slabclassid(worker_id,
                                                 NovaConfig::config->log_buf_size);
            char *rnic_buf = manager->ItemAlloc(worker_id, scid);
            RDMA_ASSERT(rnic_buf) << "Running out of memory";
            nova::NovaCCServer *cc_server = new nova::NovaCCServer(rdma_ctrl,
                                                                   manager,
                                                                   rtable_manager,
                                                                   log_manager,
                                                                   worker_id,
                                                                   true);
            leveldb::CCClient *dc_client = new leveldb::NovaCCClient(worker_id,
                                                                     store,
                                                                     manager,
                                                                     rtable_manager,
                                                                     new leveldb::log::RDMALogWriter(
                                                                             store,
                                                                             rnic_buf,
                                                                             manager,
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

            bgs[i]->mem_manager_ = manager;
            bgs[i]->dc_client_ = dc_client;
            bgs[i]->rdma_store_ = store;
            bgs[i]->thread_id_ = worker_id;
            bgs[i]->cc_server_ = cc_server;
            worker_id++;
            buf += nrdma_buf_unit() *
                   NovaCCConfig::cc_config->cc_servers.size();
        }

        RDMA_ASSERT(buf == cache_buf);

        for (int i = 0;
             i < NovaCCConfig::cc_config->num_cc_server_workers; i++) {
            NovaCCServerAsyncWorker *worker = new NovaCCServerAsyncWorker(
                    rtable_manager, cc_servers);
            cc_server_workers.push_back(worker);
        }

        // Assign workers to cc servers.
        for (int i = 0; i < cc_servers.size(); i++) {
            cc_servers[i]->async_workers_ = cc_server_workers;
        }

        // Assign async workers to dbs.
        DBAsyncWorkers *db_asyncs = new DBAsyncWorkers[NovaCCConfig::cc_config->db_fragment.size()];
        int async_worker_id = 0;
        bool assigned_all_workers = false;
        bool assigned_all_dbs = false;
        while (!assigned_all_dbs || !assigned_all_workers) {
            for (int i = 0;
                 i < NovaCCConfig::cc_config->db_fragment.size(); i++) {
                db_asyncs[i].workers.push_back(async_workers[async_worker_id]);
                async_worker_id++;
                if (async_worker_id == async_workers.size()) {
                    assigned_all_workers = true;
                }
                async_worker_id %= async_workers.size();
            }
            assigned_all_dbs = true;
        }

        for (int i = 0;
             i < NovaCCConfig::cc_config->db_fragment.size(); i++) {
            RDMA_LOG(INFO) << fmt::format("Assigned {} workers to db {}.",
                                          db_asyncs[i].workers.size(), i);
        }

        for (int worker_id = 0;
             worker_id <
             NovaCCConfig::cc_config->num_conn_workers; worker_id++) {
            conn_workers[worker_id]->db_async_workers_ = db_asyncs;
            conn_workers[worker_id]->db_current_async_worker_id_ = new int[NovaCCConfig::cc_config->db_fragment.size()];
        }

        // Start the threads.
        for (int worker_id = 0;
             worker_id <
             NovaCCConfig::cc_config->num_conn_async_workers; worker_id++) {
            cc_workers.emplace_back(&NovaRDMAComputeComponent::Start,
                                    async_workers[worker_id]);
        }
        for (int i = 0;
             i < NovaCCConfig::cc_config->num_compaction_workers; i++) {
            compaction_workers.emplace_back(
                    &leveldb::NovaCCCompactionThread::Start,
                    bgs[i]);
        }
        for (int i = 0;
             i < NovaCCConfig::cc_config->num_cc_server_workers; i++) {
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
            if (all_initialized) {
                for (const auto &worker : bgs) {
                    if (!worker->IsInitialized()) {
                        all_initialized = false;
                        break;
                    }
                }
            }
            usleep(10000);
        }


        if (NovaConfig::config->enable_load_data) {
            LoadData();
        }

        for (auto db : dbs_) {
            db->StartTracing();
        }

        // Start connection threads in the end after we have loaded all data.
        for (int worker_id = 0;
             worker_id <
             NovaCCConfig::cc_config->num_conn_workers; worker_id++) {
            conn_worker_threads.emplace_back(start, conn_workers[worker_id]);
        }

//    int cores[] = {8, 9, 10, 11, 12, 13, 14, 15, 24, 25, 26, 27, 28, 29, 30, 31};
//    for (int i = 0; i < worker_threads.size(); i++) {
//        // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
//        // only CPU i as set.
//        cpu_set_t cpuset;
//        CPU_ZERO(&cpuset);
//        CPU_SET(i, &cpuset);
//        int rc = pthread_setaffinity_np(worker_threads[i].native_handle(),
//                                        sizeof(cpu_set_t), &cpuset);
//    }
        current_conn_worker_id_ = 0;
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
        if (NovaCCConfig::cc_config->num_conn_workers == 1) {
            server->current_conn_worker_id_ = 0;
        } else {
            server->current_conn_worker_id_ =
                    (server->current_conn_worker_id_ + 1) %
                    NovaCCConfig::cc_config->num_conn_workers;
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