
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <netinet/tcp.h>
#include <signal.h>
#include "leveldb/write_batch.h"
#include "db/filename.h"
#include "nova_cc_server.h"
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
                                    leveldb::EnvBGThread *bg_thread) {
            leveldb::EnvOptions env_option;
            env_option.sstable_mode = leveldb::NovaSSTableMode::SSTABLE_DISK;
            leveldb::PosixEnv *env = new leveldb::PosixEnv;
            env->set_env_option(env_option);
            leveldb::DB *db;
            leveldb::Options options;
            options.block_cache = cache;
            if (NovaCCConfig::cc_config->write_buffer_size_mb > 0) {
                options.write_buffer_size =
                        NovaCCConfig::cc_config->write_buffer_size_mb * 1024 *
                        1024;
            }
            options.env = env;
            options.create_if_missing = true;
            options.compression = leveldb::kSnappyCompression;
            options.filter_policy = leveldb::NewBloomFilterPolicy(10);
            options.bg_thread = bg_thread;
            options.enable_tracing = false;
            options.comparator = new YCSBKeyComparator();
            leveldb::Logger *log = nullptr;
            std::string db_path = DBName(NovaConfig::config->db_path,
                                         NovaCCConfig::cc_config->my_cc_server_id,
                                         db_index);
            mkdir(db_path.c_str(), 0777);

            RDMA_ASSERT(env->NewLogger(
                    db_path + "/LOG-" + std::to_string(db_index), &log).ok());
            options.info_log = log;
            leveldb::Status status = leveldb::DB::Open(options, db_path, &db);
            RDMA_ASSERT(status.ok()) << "Open leveldb failed "
                                     << status.ToString();

            uint64_t index = 0;
            std::string logname = leveldb::LogFileName(db_path, 1111);
            ParseDBName(logname, &index);
            RDMA_ASSERT(index == db_index);
            return db;
        }
    }


    void start(NovaCCConnWorker *store) {
        store->Start();
    }

    void NovaCCServer::LoadDataWithRangePartition() {
        // load data.
        timeval start{};
        gettimeofday(&start, nullptr);
        int loaded_keys = 0;
        std::vector<Fragment *> &frags = NovaCCConfig::cc_config->fragments;
        leveldb::WriteOptions option;
        option.sync = true;

        for (int i = 0; i < frags.size(); i++) {
            if (frags[i]->server_ids[0] !=
                NovaCCConfig::cc_config->my_cc_server_id) {
                continue;
            }
            leveldb::WriteBatch batch;
            int bs = 0;
            // Insert cold keys first so that hot keys will be at the top level.
            std::vector<std::string *> pointers;
            leveldb::DB *db = dbs_[frags[i]->db_ids[0]];

            RDMA_LOG(INFO) << "Insert " << frags[i]->key_start << " to "
                           << frags[i]->key_end;

            for (uint64_t j = frags[i]->key_end;
                 j >= frags[i]->key_start; j--) {
                auto v = static_cast<char>((j % 10) + 'a');

                std::string *key = new std::string(std::to_string(j));
                std::string *val = new std::string(
                        NovaConfig::config->load_default_value_size, v);
                pointers.push_back(key);
                pointers.push_back(val);
                batch.Put(*key, *val);
                bs += 1;

                if (bs == 1000) {
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

                if (j == frags[i]->key_start) {
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
        // Compact the database.
        // db_->CompactRange(nullptr, nullptr);

        // Assert the loaded data is valid.
//        for (int i = 0; i < NovaConfig::config->nfragments; i++) {
//            if (frags[i]->server_ids[0] != NovaConfig::config->my_server_id) {
//                continue;
//            }
//            leveldb::DB *db = dbs_[frags[i]->db_ids[0]];
//            for (uint64_t j = frags[i]->key_start;
//                 j <= frags[i]->key_end; j++) {
//                auto v = static_cast<char>((j % 10) + 'a');
//                std::string key = std::to_string(j);
//                std::string expected_val(
//                        NovaConfig::config->load_default_value_size, v
//                );
//                std::string val;
//                leveldb::Status status = db->Get(leveldb::ReadOptions(), key,
//                                                 &val);
//                RDMA_ASSERT(status.ok()) << status.ToString();
//                RDMA_ASSERT(expected_val.compare(val) == 0) << val;
//            }
//        }
    }

    void NovaCCServer::LoadData() {
        LoadDataWithRangePartition();
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

    NovaCCServer::NovaCCServer(RdmaCtrl *rdma_ctrl,
                               char *rdmabuf, int nport)
            : nport_(nport) {
        char *buf = rdmabuf;
        char *cache_buf = buf + nrdma_buf_cc();
        manager = new NovaMemManager(cache_buf);
        log_manager = new LogFileManager(manager);

        int ndbs = NovaConfig::ParseNumberOfDatabases(
                NovaCCConfig::cc_config->fragments,
                &NovaCCConfig::cc_config->db_fragment,
                NovaCCConfig::cc_config->my_cc_server_id);

        int bg_thread_id = 0;
        for (int i = 0;
             i < NovaCCConfig::cc_config->num_compaction_workers; i++) {
            bgs.push_back(new leveldb::NovaCCCompactionThread(rdma_ctrl));
        }

        leveldb::Cache *cache = nullptr;
        if (NovaCCConfig::cc_config->block_cache_mb > 0) {
            cache = leveldb::NewLRUCache(
                    NovaCCConfig::cc_config->block_cache_mb * 1024 * 1024);
        }

        for (int db_index = 0; db_index < ndbs; db_index++) {
            dbs_.push_back(CreateDatabase(db_index, cache, bgs[bg_thread_id]));
            bg_thread_id += 1;
            bg_thread_id %= bgs.size();
        }
        NovaAsyncCompleteQueue **async_cq = new NovaAsyncCompleteQueue *[NovaCCConfig::cc_config->num_conn_workers];
        for (int worker_id = 0;
             worker_id <
             NovaCCConfig::cc_config->num_conn_workers; worker_id++) {
            async_cq[worker_id] = new NovaAsyncCompleteQueue;
            conn_workers[worker_id] = new NovaCCConnWorker(worker_id, this,
                                                           async_cq[worker_id]);
            conn_workers[worker_id]->set_dbs(dbs_);
            conn_workers[worker_id]->log_manager_ = log_manager;
        }
        int worker_id = 0;
        for (worker_id = 0;
             worker_id <
             NovaCCConfig::cc_config->num_async_workers; worker_id++) {
            async_workers[worker_id] = new NovaRDMAComputeComponent(rdma_ctrl,
                                                                    manager,
                                                                    dbs_,
                                                                    async_cq);
            NovaRDMAStore *store = nullptr;
            std::vector<QPEndPoint> endpoints;
            for (int i = 0;
                 i < NovaDCConfig::dc_config->dc_servers.size(); i++) {
                QPEndPoint qp;
                qp.host = NovaDCConfig::dc_config->dc_servers[i];
                qp.thread_id = worker_id;
                qp.server_id = NovaCCConfig::cc_config->cc_servers.size() + i;
                endpoints.push_back(qp);
            }

            if (NovaConfig::config->enable_rdma) {
                store = new NovaRDMARCStore(buf, worker_id, endpoints,
                                            async_workers[worker_id]);
            } else {
                store = new NovaRDMANoopStore();
            }

            // Log writers.
            async_workers[worker_id]->set_rdma_store(store);
            buf += nrdma_buf_unit();
        }

        for (int i = 0;
             i < NovaCCConfig::cc_config->num_compaction_workers; i++) {
            NovaRDMAComputeComponent *cc = new NovaRDMAComputeComponent(
                    rdma_ctrl,
                    manager,
                    dbs_,
                    async_cq);

            NovaRDMAStore *store = nullptr;
            std::vector<QPEndPoint> endpoints;
            for (int i = 0;
                 i < NovaDCConfig::dc_config->dc_servers.size(); i++) {
                QPEndPoint qp;
                qp.host = NovaDCConfig::dc_config->dc_servers[i];
                qp.thread_id = worker_id;
                qp.server_id = NovaCCConfig::cc_config->cc_servers.size() + i;
                endpoints.push_back(qp);
            }

            if (NovaConfig::config->enable_rdma) {
                store = new NovaRDMARCStore(buf, worker_id, endpoints, cc);
            } else {
                store = new NovaRDMANoopStore();
            }

            uint32_t scid = manager->slabclassid(
                    NovaConfig::config->log_buf_size);
            char *rnic_buf = manager->ItemAlloc(scid);

            leveldb::DCClient *dc_client = new leveldb::NovaDCClient(store,
                                                                     manager,
                                                                     new leveldb::log::RDMALogWriter(
                                                                             store,
                                                                             rnic_buf));
            bgs[i]->mem_manager_ = manager;
            bgs[i]->dc_client_ = dc_client;
            bgs[i]->rdma_store_ = store;
            worker_id++;
            buf += nrdma_buf_unit();
        }

        RDMA_ASSERT(buf == cache_buf);

        // Assign async workers to conn workers.
        if (NovaCCConfig::cc_config->num_conn_workers <
            NovaCCConfig::cc_config->num_async_workers) {
            int conn_worker_id = 0;
            for (int worker_id = 0;
                 worker_id <
                 NovaCCConfig::cc_config->num_async_workers; worker_id++) {
                conn_workers[conn_worker_id]->async_workers_.push_back(
                        async_workers[worker_id]);
                conn_worker_id += 1;
                conn_worker_id =
                        conn_worker_id %
                        NovaCCConfig::cc_config->num_conn_workers;
            }
        } else {
            int async_worker_id = 0;
            for (int worker_id = 0;
                 worker_id <
                 NovaCCConfig::cc_config->num_conn_workers; worker_id++) {
                conn_workers[worker_id]->async_workers_.push_back(
                        async_workers[async_worker_id]);
                async_worker_id += 1;
                async_worker_id =
                        async_worker_id %
                        NovaCCConfig::cc_config->num_async_workers;
            }
        }

        // Start the threads.
        for (int worker_id = 0;
             worker_id <
             NovaCCConfig::cc_config->num_async_workers; worker_id++) {
            cc_workers.emplace_back(&NovaRDMAComputeComponent::Start,
                                    async_workers[worker_id]);
        }
        for (int i = 0;
             i < NovaCCConfig::cc_config->num_compaction_workers; i++) {
            compaction_workers.emplace_back(
                    &leveldb::NovaCCCompactionThread::Start,
                    bgs[i]);
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
        auto *server = (NovaCCServer *) arg;
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

    void NovaCCServer::Start() {
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

    void NovaCCServer::SetupListener() {
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