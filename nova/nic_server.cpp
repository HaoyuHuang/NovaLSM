
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nic_server.h"

#include <fcntl.h>
#include <fmt/core.h>
#include <netinet/tcp.h>
#include <signal.h>

#include "logging.hpp"

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
      rocksdb::WriteOptions option;
        for (uint32_t i = 0; i < NovaConfig::config->nfragments; i++) {
            if (frags[i]->server_ids[0] != NovaConfig::config->my_server_id) {
                continue;
            }
            // Insert cold keys first so that hot keys will be at the top level.
            std::vector<std::string *> pointers;
          rocksdb::DB *db = dbs_[frags[i]->dbid];
            RDMA_LOG(rdmaio::INFO) << fmt::format("DB-{} Insert {} {}", frags[i]->dbid,
                                          frags[i]->key_start,
                                          frags[i]->key_end);
            for (uint64_t j = frags[i]->key_start; j < frags[i]->key_end; j++) {
                auto v = static_cast<char>((j % 10) + 'a');
                std::string key(std::to_string(j));
                std::string val(
                        NovaConfig::config->load_default_value_size, v);
              rocksdb::Status status = db->Put(option, key, val);
//                RDMA_LOG(INFO) << fmt::format("Insert {}", key);
                RDMA_ASSERT(status.ok()) << status.ToString();
                loaded_keys++;
                if (loaded_keys % 100000 == 0) {
                    timeval now{};
                    gettimeofday(&now, nullptr);
                    RDMA_LOG(rdmaio::INFO) << "Load " << loaded_keys << " entries took "
                                   << now.tv_sec - start.tv_sec;
                }
            }
        }
        RDMA_LOG(rdmaio::INFO) << "Completed loading data " << loaded_keys;
        gettimeofday(&start, nullptr);
        loaded_keys = 0;
//        for (int i = 0; i < 10000000; i++) {
//            int index = rand();
//            int db_index = index % dbs_.size();
//            nova::Fragment *frag = nova::NovaConfig::config->db_fragment[db_index];
//            RDMA_ASSERT(frag);
//            auto db = dbs_[db_index];
//            RDMA_ASSERT(db);
//            uint64_t keys = frag->key_end - frag->key_start;
//            uint32_t key = frag->key_start + index % keys;
//            RDMA_ASSERT(key >= frag->key_start && key < frag->key_end);
//
//            leveldb::WriteBatch batch = {};
//            auto v = static_cast<char>((key % 10) + 'a');
//            std::string *strkey = new std::string(std::to_string(key));
//            std::string *strval = new std::string(
//                    NovaConfig::config->load_default_value_size, v);
//            batch.Put(*strkey, *strval);
//            leveldb::Status status = db->Write(option, &batch);
//            RDMA_ASSERT(status.ok()) << status.ToString();
//            delete strkey;
//            delete strval;
//
//            loaded_keys++;
//            if (loaded_keys % 100000 == 0) {
//                timeval now{};
//                gettimeofday(&now, nullptr);
//                RDMA_LOG(INFO) << "Load " << loaded_keys << " entries took "
//                               << now.tv_sec - start.tv_sec;
//            }
//        }

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
          rocksdb::CompactRangeOptions cro;
          db->CompactRange(cro, nullptr, nullptr);
        }

        for (uint32_t i = 0; i < dbs_.size(); i++) {
            RDMA_LOG(rdmaio::INFO) << "Database " << i;
            std::string value;
            dbs_[i]->GetProperty("rocksdb.levelstats", &value);
            RDMA_LOG(rdmaio::INFO) << "\n" << value;
            value.clear();
            dbs_[i]->GetProperty("leveldb.stats", &value);
            RDMA_LOG(rdmaio::INFO) << "\n" << "rocksdb stats " << value;
            rocksdb::Log(dbs_[i]->GetOptions().info_log, "%s", "Load complete");
        }
    }

    NovaMemServer::NovaMemServer(const std::vector<rocksdb::DB *> &dbs,
                                 char *rdmabuf, int nport)
            : nport_(nport) {
        dbs_ = dbs;
        conn_workers = new NovaConnWorker *[NovaConfig::config->num_conn_workers];
        async_workers = new NovaAsyncWorker *[NovaConfig::config->num_async_workers];
        manager = new NovaMemManager(rdmabuf);
        if (NovaConfig::config->enable_load_data) {
            LoadData();
        }
        NovaAsyncCompleteQueue **async_cq = new NovaAsyncCompleteQueue *[NovaConfig::config->num_conn_workers];
        for (int worker_id = 0;
             worker_id < NovaConfig::config->num_conn_workers; worker_id++) {
            async_cq[worker_id] = new NovaAsyncCompleteQueue;
            conn_workers[worker_id] = new NovaConnWorker(worker_id, this,
                                                         async_cq[worker_id]);
            conn_workers[worker_id]->set_dbs(dbs);
        }
        for (int worker_id = 0;
             worker_id < NovaConfig::config->num_async_workers; worker_id++) {
            async_workers[worker_id] = new NovaAsyncWorker(dbs, async_cq);
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
        RDMA_LOG(rdmaio::INFO) << "Number of worker thread per conn thread "
                       << conn_workers[0]->async_workers_.size();

        // Start the threads.
        for (int worker_id = 0;
             worker_id < NovaConfig::config->num_async_workers; worker_id++) {
            async_worker_threads.emplace_back(&NovaAsyncWorker::Start,
                                              async_workers[worker_id]);
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
        RDMA_LOG(rdmaio::DEBUG) << "new connection " << fd;

        int client_fd;
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        client_fd = accept(fd, (struct sockaddr *) &client_addr, &client_len);
        RDMA_ASSERT(client_fd < NOVA_MAX_CONN) << client_fd
                                               << " not enough connections";
        RDMA_ASSERT(client_fd >= 0) << client_fd;
        make_socket_non_blocking(client_fd);
        RDMA_LOG(rdmaio::DEBUG) << "register " << client_fd;

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

        RDMA_LOG(rdmaio::INFO) << "Using Libevent with backend method "
                       << event_base_get_method(base);
        const int f = event_base_get_features(base);
        if ((f & EV_FEATURE_ET)) {
            RDMA_LOG(rdmaio::INFO) << "Edge-triggered events are supported.";
        }
        if ((f & EV_FEATURE_O1)) {
            RDMA_LOG(rdmaio::INFO) <<
                           "O(1) event notification is supported.";
        }
        if ((f & EV_FEATURE_FDS)) {
            RDMA_LOG(rdmaio::INFO) << "All FD types are supported.";
        }

        /* Listen for notifications from other threads */
        memset(&event, 0, sizeof(struct event));
        RDMA_ASSERT(event_assign(&event, base, listen_fd_, EV_READ | EV_PERSIST,
                                 on_accept, (void *) this) == 0);
        RDMA_ASSERT(event_add(&event, 0) == 0) << listen_fd_;
        RDMA_ASSERT(event_base_loop(base, 0) == 0) << listen_fd_;
        RDMA_LOG(rdmaio::INFO) << "started";
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