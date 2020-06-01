
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <fmt/core.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <leveldb/write_batch.h>
#include "nova_mem_server.h"

namespace nova {

    void start(NovaConnWorker *store) {
        store->Start();
    }

    void NovaMemServer::LoadDataWithRangePartition() {
        // load data.
        timeval start{};
        gettimeofday(&start, nullptr);
        int loaded_keys = 0;
        Fragment **frags = NovaConfig::config->fragments;
        leveldb::WriteOptions option;
        option.log_record_mode = leveldb::LOG_NONE;
        for (int i = 0; i < NovaConfig::config->nfragments; i++) {
            if (frags[i]->server_ids[0] != NovaConfig::config->my_server_id) {
                continue;
            }
            leveldb::WriteBatch batch;
            int bs = 0;
            // Insert cold keys first so that hot keys will be at the top level.
            std::vector<std::string *> pointers;
            leveldb::DB *db = dbs_[frags[i]->dbid];

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


    void NovaMemServer::LoadData() {
        LoadDataWithRangePartition();

        for (auto &db : dbs_) {
            db->SetL0StartCompactionBytes(0);
            db->FlushMemTable(NovaConfig::config->log_record_mode);
        }
        while (true) {
            bool stop = true;
            for (auto &db : dbs_) {
                uint64_t bytes = db->L0CurrentBytes();
                if (bytes > 0) {
                    RDMA_LOG(INFO)
                        << fmt::format("Waiting for {} bytes at L0", bytes);
                    stop = false;
                    db->MaybeScheduleCompaction();
                }
            }
            if (stop) {
                break;
            }
            for (int i = 0; i < dbs_.size(); i++) {
                RDMA_LOG(INFO) << "Database " << i;
                std::string value;
                dbs_[i]->GetProperty("leveldb.sstables", &value);
                RDMA_LOG(INFO) << "\n" << value;
                value.clear();
                dbs_[i]->GetProperty("leveldb.approximate-memory-usage", &value);
                RDMA_LOG(INFO) << "\n" << "leveldb memory usage " << value;
            }
            sleep(1);
        }

        for (int i = 0; i < dbs_.size(); i++) {
            dbs_[i]->SetL0StartCompactionBytes(
                    NovaConfig::config->l0_start_compaction_bytes);
            RDMA_LOG(INFO) << "Database " << i;
            std::string value;
            dbs_[i]->GetProperty("leveldb.sstables", &value);
            RDMA_LOG(INFO) << "\n" << value;
            value.clear();
            dbs_[i]->GetProperty("leveldb.approximate-memory-usage", &value);
            RDMA_LOG(INFO) << "\n" << "leveldb memory usage " << value;
            leveldb::Log(dbs_[i]->infoLog(), "%s", "Load complete");
        }
    }

    NovaMemServer::NovaMemServer(const std::vector<leveldb::DB *> &dbs,
                                 char *rdmabuf, int nport)
            : nport_(nport) {
        dbs_ = dbs;
        conn_workers = new NovaConnWorker *[NovaConfig::config->num_conn_workers];
        async_workers = new NovaAsyncWorker *[NovaConfig::config->num_async_workers];
        char *buf = rdmabuf;
        uint64_t nrdmatotal_per_store =
                (NovaConfig::config->rdma_max_num_sends * 2) *
                NovaConfig::config->max_msg_size *
                NovaConfig::config->servers.size();
        char *cache_buf =
                buf +
                NovaConfig::config->num_conn_workers * nrdmatotal_per_store;
        manager = new NovaMemManager(cache_buf);
        log_manager = new LogFileManager(manager);
        if (NovaConfig::config->enable_load_data) {
            LoadData();
        }
        for (auto db : dbs) {
            db->StartTracing();
        }
        NovaAsyncCompleteQueue **async_cq = new NovaAsyncCompleteQueue *[NovaConfig::config->num_conn_workers];

        for (int worker_id = 0;
             worker_id < NovaConfig::config->num_conn_workers; worker_id++) {
            async_cq[worker_id] = new NovaAsyncCompleteQueue;
            conn_workers[worker_id] = new NovaConnWorker(worker_id, this,
                                                         async_cq[worker_id]);
            conn_workers[worker_id]->set_dbs(dbs);
            conn_workers[worker_id]->log_manager_ = log_manager;
        }

        for (int worker_id = 0;
             worker_id < NovaConfig::config->num_async_workers; worker_id++) {
            async_workers[worker_id] = new NovaAsyncWorker(dbs, async_cq);
            NovaRDMAStore *store = nullptr;

            std::vector<QPEndPoint> endpoints;
            for (int i = 0; i < NovaConfig::config->servers.size(); i++) {
                QPEndPoint qp;
                qp.host = NovaConfig::config->servers[i];
                qp.thread_id = worker_id;
                endpoints.push_back(qp);
            }

            if (NovaConfig::config->enable_rdma) {
                store = new NovaRDMARCStore(buf, worker_id, endpoints,
                                            async_workers[worker_id]);
            } else {
                store = new NovaRDMANoopStore();
            }

            // Log writers.
            async_workers[worker_id]->nic_log_writer_ = new leveldb::log::NICLogWriter(
                    &async_workers[worker_id]->socks_, log_manager);
            async_workers[worker_id]->rdma_log_writer_ = new leveldb::log::RDMALogWriter(
                    store, manager, log_manager);
            async_workers[worker_id]->log_manager_ = log_manager;
            async_workers[worker_id]->set_rdma_store(store);
            buf += nrdmatotal_per_store;
        }

        // Assign async workers to conn workers.
        if (NovaConfig::config->num_conn_workers <
            NovaConfig::config->num_async_workers) {
            int conn_worker_id = 0;
            for (int worker_id = 0;
                 worker_id <
                 NovaConfig::config->num_async_workers; worker_id++) {
                conn_workers[conn_worker_id]->async_workers_.push_back(
                        async_workers[worker_id]);
                conn_worker_id += 1;
                conn_worker_id =
                        conn_worker_id % NovaConfig::config->num_conn_workers;
            }
        } else {
            int async_worker_id = 0;
            for (int worker_id = 0;
                 worker_id <
                 NovaConfig::config->num_conn_workers; worker_id++) {
                conn_workers[worker_id]->async_workers_.push_back(
                        async_workers[async_worker_id]);
                async_worker_id += 1;
                async_worker_id =
                        async_worker_id % NovaConfig::config->num_async_workers;
            }
        }
        RDMA_LOG(INFO) << "Number of worker thread per conn thread "
                       << conn_workers[0]->async_workers_.size();

        // Start the threads.
        for (int worker_id = 0;
             worker_id < NovaConfig::config->num_async_workers; worker_id++) {
            async_worker_threads.emplace_back(&NovaAsyncWorker::Start,
                                              async_workers[worker_id]);
        }

        if (NovaConfig::config->log_record_mode ==
            leveldb::LOG_RDMA) {
            bool all_initialized = false;
            while (!all_initialized) {
                all_initialized = true;
                for (int worker_id = 0;
                     worker_id <
                     NovaConfig::config->num_async_workers; worker_id++) {
                    if (!async_workers[worker_id]->IsInitialized()) {
                        all_initialized = false;
                        break;
                    }
                }
                usleep(10000);
            }
        }
        for (int worker_id = 0;
             worker_id < NovaConfig::config->num_conn_workers; worker_id++) {
            worker_threads.emplace_back(start, conn_workers[worker_id]);
        }
        current_store_id_ = 0;
    }

    void make_socket_non_blocking(int sockfd) {
        int flags = fcntl(sockfd, F_GETFL, 0);
        if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) == -1) {
        }
    }

    void on_accept(int fd, short which, void *arg) {
        auto *server = (NovaMemServer *) arg;
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

        NovaConnWorker *store = server->conn_workers[server->current_store_id_];
        if (NovaConfig::config->num_conn_workers == 1) {
            server->current_store_id_ = 0;
        } else {
            server->current_store_id_ = (server->current_store_id_ + 1) %
                                        NovaConfig::config->num_conn_workers;
        }

        store->conn_mu.lock();
        store->conn_queue.push_back(client_fd);
        store->conn_mu.unlock();

//    char buf[1];
//    buf[0] = 'c';
//    write(store->on_new_conn_send_fd, buf, 1);
    }

    void NovaMemServer::Start() {
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

    void NovaMemServer::SetupListener() {
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