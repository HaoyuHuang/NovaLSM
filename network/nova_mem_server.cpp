
//
// Created by Haoyu Huang on 4/4/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <netinet/tcp.h>
#include <signal.h>
#include "nova_mem_server.h"

void start(NovaMemWorker *store) {
    store->Start();
}

void NovaMemServer::LoadDataWithRangePartition() {
    char key[1024];
    char value[NovaConfig::config->load_default_value_size];
    // load data.
    timeval start{};
    gettimeofday(&start, nullptr);
    int loaded_keys = 0;
    Fragment **frags = NovaConfig::config->fragments;
    for (int i = 0; i < NovaConfig::config->nfragments; i++) {
        if (frags[i]->server_id != NovaConfig::config->my_server_id) {
            continue;
        }
        for (uint64_t j = frags[i]->key_start;
             j <= frags[i]->key_end; j++) {
            uint32_t nkey = int_to_str(key, j) - 1;
            auto v = static_cast<char>((j % 10) + '0');
            memset(value, v, NovaConfig::config->load_default_value_size);
            RDMA_LOG(DEBUG) << "Insert " << j;
            RDMA_ASSERT(manager->LocalPut(key, nkey, value,
                                          NovaConfig::config->load_default_value_size,
                                          true, false).success);
            loaded_keys++;
            if (loaded_keys % 100000 == 0) {
                timeval now{};
                gettimeofday(&now, nullptr);
                RDMA_LOG(INFO) << "Load " << loaded_keys << " entries took "
                               << now.tv_sec - start.tv_sec;
            }
        }
    }
    RDMA_LOG(INFO) << "Completed loading data " << loaded_keys;
    // Assert the loaded data is valid.
    for (int i = 0; i < NovaConfig::config->nfragments; i++) {
        if (frags[i]->server_id != NovaConfig::config->my_server_id) {
            continue;
        }
        for (uint64_t j = frags[i]->key_start;
             j <= frags[i]->key_end; j++) {
            uint32_t nkey = int_to_str(key, j) - 1;
            auto v = static_cast<char>((j % 10) + '0');
            memset(value, v, NovaConfig::config->load_default_value_size);
            GetResult result = manager->LocalGet(key, nkey);
            DataEntry it = result.data_entry;
            RDMA_ASSERT(it.stale == 0);
            RDMA_ASSERT(it.nkey == nkey) << key << " " << i << " "
                                         << it.nkey;
            RDMA_ASSERT(it.nval == NovaConfig::config->load_default_value_size)
                    << key;
            RDMA_ASSERT(memcmp(it.user_key(), key, nkey) == 0) << key;
            RDMA_ASSERT(
                    memcmp(it.user_value(), value, it.nval) ==
                    0) << key;
        }
    }
}

void NovaMemServer::LoadDataWithHashPartition() {
    char key[1024];
    char value[NovaConfig::config->load_default_value_size];
    // load data.
    timeval start{};
    gettimeofday(&start, nullptr);
    int loaded_keys = 0;
    for (uint64_t record_id = 0;
         record_id < NovaConfig::config->recordcount; record_id++) {
        Fragment *frag = NovaConfig::home_fragment(record_id);
        if (frag->server_id == NovaConfig::config->my_server_id) {
            uint32_t nkey = int_to_str(key, record_id) - 1;
            auto v = static_cast<char>((record_id % 26) + 'a');
            memset(value, v, NovaConfig::config->load_default_value_size);
            RDMA_LOG(DEBUG) << "Insert " << record_id;
            RDMA_ASSERT(manager->LocalPut(key, nkey, value,
                                          NovaConfig::config->load_default_value_size,
                                          true, false).success);
            loaded_keys++;
            if (loaded_keys % 100000 == 0) {
                timeval now{};
                gettimeofday(&now, nullptr);
                RDMA_LOG(INFO) << "Load " << loaded_keys << " entries took "
                               << now.tv_sec - start.tv_sec;
            }
        }
    }

    RDMA_LOG(INFO) << "Completed loading data " << loaded_keys;
    // Assert the loaded data is valid.
    for (uint64_t record_id = 0;
         record_id < NovaConfig::config->recordcount; record_id++) {
        Fragment *frag = NovaConfig::home_fragment(record_id);
        if (frag->server_id == NovaConfig::config->my_server_id) {
            uint32_t nkey = int_to_str(key, record_id) - 1;
            auto v = static_cast<char>((record_id % 26) + 'a');
            memset(value, v, NovaConfig::config->load_default_value_size);
            GetResult result = manager->LocalGet(key, nkey);
            DataEntry it = result.data_entry;
            RDMA_ASSERT(it.stale == 0);
            RDMA_ASSERT(it.nkey == nkey) << key << " " << it.nkey;
            RDMA_ASSERT(it.nval == NovaConfig::config->load_default_value_size)
                    << key;
            RDMA_ASSERT(memcmp(it.user_key(), key, nkey) == 0) << key;
            RDMA_ASSERT(
                    memcmp(it.user_value(), value, it.nval) ==
                    0) << key;
        }
    }
}

void NovaMemServer::LoadData() {
    if (NovaConfig::config->partition_mode == NovaRDMAPartitionMode::HASH) {
        LoadDataWithHashPartition();
    } else if (NovaConfig::config->partition_mode ==
               NovaRDMAPartitionMode::RANGE) {
        LoadDataWithRangePartition();
    }
    if (RDMA_LOG_LEVEL == DEBUG) {
        manager->PrintHashTable();
    }
}

NovaMemServer::NovaMemServer(char *rdmabuf, int nport) : nport_(nport) {
    workers = new NovaMemWorker *[NovaConfig::config->num_mem_workers];
    char *buf = rdmabuf;
    uint64_t nrdmatotal_per_store =
            (NovaConfig::config->rdma_max_num_sends * 2 +
             NovaConfig::config->rdma_max_num_reads) *
            NovaConfig::config->max_msg_size *
            NovaConfig::config->servers.size();
    char *cache_buf =
            buf + NovaConfig::config->num_mem_workers * nrdmatotal_per_store;
    manager = new NovaMemManager(cache_buf);
    if (NovaConfig::config->enable_load_data) {
        LoadData();
    }
    for (int worker_id = 0;
         worker_id < NovaConfig::config->num_mem_workers; worker_id++) {
        workers[worker_id] = new NovaMemWorker(worker_id, worker_id, this);
        NovaRDMAStore *store = nullptr;
        if (NovaConfig::config->enable_rdma) {
            store = new NovaRDMARCStore(buf, worker_id, workers[worker_id]);
        } else {
            store = new NovaRDMANoopStore();
        }
        workers[worker_id]->set_rdma_store(store);
        workers[worker_id]->set_mem_manager(manager);
        worker_threads.emplace_back(start, workers[worker_id]);
        buf += nrdmatotal_per_store;
    }

//    int cores[] = {8, 9, 10, 11, 12, 13, 14, 15, 24, 25, 26, 27, 28, 29, 30, 31};
    for (int i = 0; i < worker_threads.size(); i++) {
        // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
        // only CPU i as set.
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        int rc = pthread_setaffinity_np(worker_threads[i].native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
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

    NovaMemWorker *store = server->workers[server->current_store_id_];
    if (NovaConfig::config->num_mem_workers == 1) {
        server->current_store_id_ = 0;
    } else {
        server->current_store_id_ = (server->current_store_id_ + 1) %
                                    NovaConfig::config->num_mem_workers;
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