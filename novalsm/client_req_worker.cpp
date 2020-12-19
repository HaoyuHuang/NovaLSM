
//
// Created by Haoyu Huang on 3/28/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "client_req_worker.h"

#include "common/nova_console_logging.h"
#include "common/nova_common.h"
#include "common/nova_config.h"
#include "common/nova_client_sock.h"

#include <sys/types.h>
#include <sys/signalfd.h>
#include <sys/epoll.h>
#include <cerrno>
#include <poll.h>
#include <signal.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <assert.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include <event.h>
#include <leveldb/write_batch.h>
#include <ltc/storage_selector.h>

namespace nova {
    using namespace rdmaio;

    Connection *nova_conns[NOVA_MAX_CONN];
    mutex new_conn_mutex;

    SocketState socket_write_handler(int fd, Connection *conn) {
        NOVA_ASSERT(conn->response_size < NovaConfig::config->max_msg_size);
        NICClientReqWorker *store = (NICClientReqWorker *) conn->worker;
        struct iovec iovec_array[1];
        iovec_array[0].iov_base = conn->response_buf + conn->response_ind;
        iovec_array[0].iov_len = conn->response_size - conn->response_ind;
        struct msghdr msg;
        memset(&msg, 0, sizeof(msg));
        msg.msg_iov = &iovec_array[0];
        msg.msg_iovlen = 1;
        int n = 0;
        int total = 0;
        if (conn->response_ind == 0) {
            store->stats.nresponses++;
        }
        do {
            iovec_array[0].iov_base = (char *) (iovec_array[0].iov_base) + n;
            iovec_array[0].iov_len -= n;
            n = sendmsg(fd, &msg, MSG_NOSIGNAL);
            if (n <= 0) {
                if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    store->stats.nwritesagain++;
                    NOVA_LOG(WARNING) << "memstore[" << store->thread_id_
                                      << "]: "
                                      << "write socket would block fd: "
                                      << fd
                                      << " "
                                      << strerror(errno);
                    conn->state = ConnState::WRITE;
                    conn->UpdateEventFlags(EV_WRITE | EV_PERSIST);
                    return INCOMPLETE;
                }
                return CLOSED;
            }
            conn->response_ind += n;
            total = conn->response_ind;
            store->stats.nwrites++;
        } while (total < conn->response_size);
        return COMPLETE;
    }

    void event_handler(int fd, short which, void *arg) {
        auto *conn = (Connection *) arg;
        NOVA_ASSERT(fd == conn->fd) << fd << ":" << conn->fd;
        SocketState state;

        NICClientReqWorker *worker = (NICClientReqWorker *) conn->worker;

        if (conn->state == ConnState::READ) {
            if (worker->stats.nreqs % 100 == 0) {
                gettimeofday(&worker->start, nullptr);
                worker->read_start = worker->start;
            }
            state = socket_read_handler(fd, which, conn);
            if (state == COMPLETE) {
                if (worker->stats.nreqs % 99 == 0 &&
                    worker->stats.nreqs > 0) {
                    timeval now{};
                    gettimeofday(&now, nullptr);
                    auto elapsed =
                            (now.tv_sec - worker->read_start.tv_sec) *
                            1000 *
                            1000 +
                            (now.tv_usec - worker->read_start.tv_usec);
                    worker->stats.read_service_time +=
                            elapsed;
                }
                bool reply = process_socket_request_handler(fd, conn);
                if (reply) {
                    if (worker->stats.nreqs % 100 == 0) {
                        gettimeofday(&worker->write_start, nullptr);
                    }
                    state = socket_write_handler(fd, conn);
                    if (state == COMPLETE) {
                        write_socket_complete(fd, conn);
                    }
                }
                worker->stats.nreqs++;
            }
        } else {
            NOVA_ASSERT((which & EV_WRITE) > 0);
            state = socket_write_handler(fd, conn);
            if (state == COMPLETE) {
                write_socket_complete(fd, conn);
            }
        }

        if (state == CLOSED) {
            NOVA_ASSERT(event_del(&conn->event) == 0) << fd;
            close(fd);
        }
    }

    void write_socket_complete(int fd, Connection *conn) {
        NICClientReqWorker *worker = (NICClientReqWorker *) conn->worker;

        if (worker->stats.nreqs % 99 == 0 &&
            worker->stats.nreqs > 0) {
            timeval now{};
            gettimeofday(&now, nullptr);
            auto elapsed =
                    (now.tv_sec - worker->start.tv_sec) * 1000 *
                    1000 + (now.tv_usec - worker->start.tv_usec);
            auto write_elapsed =
                    (now.tv_sec - worker->write_start.tv_sec) * 1000 *
                    1000 + (now.tv_usec - worker->write_start.tv_usec);
            worker->stats.service_time +=
                    elapsed;
            worker->stats.write_service_time += write_elapsed;
        }
        conn->UpdateEventFlags(EV_READ | EV_PERSIST);
        // processing complete.
        worker->request_buf[0] = '~';
        conn->state = READ;
        worker->req_ind = 0;
        conn->response_ind = 0;
    }

    bool
    process_socket_get(int fd, Connection *conn, char *request_buf,
                       uint32_t server_cfg_id) {
        // Stats.
        NICClientReqWorker *worker = (NICClientReqWorker *) conn->worker;
        worker->stats.ngets++;
        uint64_t int_key = 0;
        uint32_t nkey = str_to_int(request_buf, &int_key) - 1;
        uint64_t hv = keyhash(request_buf, nkey);
        worker->stats.nget_hits++;

        leveldb::Slice key(request_buf, nkey);
        LTCFragment *frag = NovaConfig::home_fragment(hv, server_cfg_id);
        NOVA_ASSERT(frag) << fmt::format("cfg:{} key:{}", server_cfg_id, hv);

        if (!frag->is_ready_) {
            frag->is_ready_mutex_.Lock();
            while (!frag->is_ready_) {
                frag->is_ready_signal_.Wait();
            }
            frag->is_ready_mutex_.Unlock();
        }

        leveldb::DB *db = reinterpret_cast<leveldb::DB *>(frag->db);
        NOVA_ASSERT(db);
        std::string value;
        leveldb::ReadOptions read_options;
        read_options.hash = int_key;
        read_options.stoc_client = worker->stoc_client_;
        read_options.mem_manager = worker->mem_manager_;
        read_options.thread_id = worker->thread_id_;
        read_options.rdma_backing_mem = worker->rdma_backing_mem;
        read_options.rdma_backing_mem_size = worker->rdma_backing_mem_size;
        read_options.cfg_id = server_cfg_id;

        leveldb::Status s = db->Get(read_options, key, &value);
        NOVA_ASSERT(s.ok())
            << fmt::format("k:{} status:{}", key.ToString(), s.ToString());

        conn->response_buf = worker->buf;
        uint32_t response_size = 0;
        char *response_buf = conn->response_buf;
        uint32_t cfg_size = int_to_str(response_buf, server_cfg_id);
        response_size += cfg_size;
        response_buf += cfg_size;
        uint32_t value_size = int_to_str(response_buf, value.size());
        response_size += value_size;
        response_buf += value_size;
        memcpy(response_buf, value.data(), value.size());
        response_buf[0] = MSG_TERMINATER_CHAR;
        response_size += 1;
        conn->response_size = response_size;

        NOVA_ASSERT(conn->response_size <
                    NovaConfig::config->max_msg_size);
        return true;
    }

    bool
    process_reintialize_qps(int fd, Connection *conn) {
        NOVA_LOG(rdmaio::INFO) << "Reinitialize QPs";
        NICClientReqWorker *worker = (NICClientReqWorker *) conn->worker;
        for (int i = 0; i < worker->rdma_threads.size(); i++) {
            auto *thread = reinterpret_cast<RDMAMsgHandler *>(worker->rdma_threads[i]);
            thread->should_pause = true;
        }
        // Wait until all rdma threads are paused.
        bool wait = true;
        while (wait) {
            wait = false;
            for (int i = 0; i < worker->rdma_threads.size(); i++) {
                auto *thread = reinterpret_cast<RDMAMsgHandler *>(worker->rdma_threads[i]);
                if (!thread->paused) {
                    wait = true;
                    break;
                }
            }
        }
        for (int i = 0; i < worker->rdma_threads.size(); i++) {
            auto *thread = reinterpret_cast<RDMAMsgHandler *>(worker->rdma_threads[i]);
            auto *broker = reinterpret_cast<NovaRDMARCBroker *> (thread->rdma_broker_);
            broker->ReinitializeQPs(worker->ctrl_);
        }
        for (int i = 0; i < worker->rdma_threads.size(); i++) {
            auto *thread = reinterpret_cast<RDMAMsgHandler *>(worker->rdma_threads[i]);
            thread->should_pause = false;
            sem_post(&thread->sem_);
        }
        char *response_buf = worker->buf;
        int nlen = 1;
        int len = int_to_str(response_buf, nlen);
        conn->response_buf = worker->buf;
        conn->response_size = len + nlen;
        return true;
    }

    bool
    process_socket_query_ready_request(int fd, Connection *conn) {
        int current_cfg_id = NovaConfig::config->current_cfg_id;
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Query configuration change. Current cfg id: {}", current_cfg_id);
        NICClientReqWorker *worker = (NICClientReqWorker *) conn->worker;
        int ret_val = 0;
        for (int fragid = 0;
             fragid < NovaConfig::config->cfgs[current_cfg_id]->fragments.size(); fragid++) {
            auto current_frag = NovaConfig::config->cfgs[current_cfg_id]->fragments[fragid];
            if (current_frag->ltc_server_id == NovaConfig::config->my_server_id) {
                if (!current_frag->is_complete_) {
                    NOVA_LOG(rdmaio::INFO)
                        << fmt::format("Frag-{} is not ready {}", fragid, current_frag->DebugString());
                    ret_val = 1;
                }
            }
        }
        char *response_buf = worker->buf;
        int len = int_to_str(response_buf, ret_val);
        response_buf[len] = MSG_TERMINATER_CHAR;
        conn->response_buf = worker->buf;
        conn->response_size = len + 1;
        return true;
    }

    bool
    process_socket_change_config_request(int fd, Connection *conn) {
        int current_cfg_id = NovaConfig::config->current_cfg_id;
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Change configuration. Current cfg id: {}", current_cfg_id);
        NICClientReqWorker *worker = (NICClientReqWorker *) conn->worker;
        // Figure out the configuration change.
        std::vector<LTCFragment *> migrate_frags;

        timeval change_start{};
        gettimeofday(&change_start, nullptr);

        int new_cfg_id = current_cfg_id + 1;
        nova::Servers *new_stocs = new nova::Servers;
        std::vector<uint32_t> removed_stocs;
        for (int fragid = 0; fragid < NovaConfig::config->cfgs[current_cfg_id]->fragments.size(); fragid++) {
            auto old_frag = NovaConfig::config->cfgs[current_cfg_id]->fragments[fragid];
            auto current_frag = NovaConfig::config->cfgs[new_cfg_id]->fragments[fragid];
            if (old_frag->ltc_server_id != current_frag->ltc_server_id) {
                if (old_frag->ltc_server_id == NovaConfig::config->my_server_id) {
                    NOVA_LOG(rdmaio::INFO) << fmt::format("Migrate {}", current_frag->DebugString());
                    migrate_frags.push_back(old_frag);
                }
            } else {
                current_frag->db = old_frag->db;
                current_frag->is_ready_ = true;
                current_frag->is_complete_ = true;
            }
        }

        new_stocs->servers = NovaConfig::config->cfgs[new_cfg_id]->stoc_servers;
        new_stocs->server_ids = NovaConfig::config->cfgs[new_cfg_id]->stoc_server_ids;
        leveldb::StorageSelector::available_stoc_servers.store(new_stocs);
        for (int i = 0; i < NovaConfig::config->cfgs[current_cfg_id]->stoc_servers.size(); i++) {
            auto stoc_id = NovaConfig::config->cfgs[current_cfg_id]->stoc_servers[i];
            auto new_cfg = NovaConfig::config->cfgs[new_cfg_id];
            if (new_cfg->stoc_server_ids.find(stoc_id) == new_cfg->stoc_server_ids.end()) {
                removed_stocs.push_back(stoc_id);
            }
        }
        // Bump up cfg id.
        NovaConfig::config->current_cfg_id.fetch_add(1);
        NovaConfig::config->cfgs[new_cfg_id]->start_time_us_ = change_start.tv_sec * 1000000 + change_start.tv_usec;
        if (nova::NovaConfig::config->cfgs[current_cfg_id]->IsLTC()) {
            int thread_id = 0;
            std::vector<LTCFragment *> batch;
            int frags_per_thread = migrate_frags.size() / worker->db_migration_threads_.size();
            if (frags_per_thread == 0) {
                frags_per_thread = 1;
            }
            NOVA_LOG(rdmaio::INFO) << fmt::format("Migrate {} ranges per migration thread.", frags_per_thread);
            for (int i = 0; i < migrate_frags.size(); i++) {
                batch.push_back(migrate_frags[i]);
                if (batch.size() == frags_per_thread) {
                    thread_id = (thread_id + 1) % worker->db_migration_threads_.size();
                    worker->db_migration_threads_[thread_id]->AddSourceMigrateDB(batch);
                    batch.clear();
                }
            }
            if (!batch.empty()) {
                thread_id = (thread_id + 1) % worker->db_migration_threads_.size();
                worker->db_migration_threads_[thread_id]->AddSourceMigrateDB(batch);
            }
            if (!removed_stocs.empty()) {
                NOVA_ASSERT(removed_stocs.size() == 1);
                for (int fragid = 0; fragid < NovaConfig::config->cfgs[new_cfg_id]->fragments.size(); fragid++) {
                    auto current_frag = NovaConfig::config->cfgs[new_cfg_id]->fragments[fragid];
                    if (current_frag->ltc_server_id == nova::NovaConfig::config->my_server_id) {
                        thread_id = (thread_id + 1) % worker->db_migration_threads_.size();
                        worker->db_migration_threads_[thread_id]->AddStoCMigration(current_frag, removed_stocs);
                    }
                }
            }
        }
        char *response_buf = worker->buf;
        int len = int_to_str(response_buf, 1);
        response_buf[len] = MSG_TERMINATER_CHAR;
        conn->response_buf = worker->buf;
        conn->response_size = len + 1;
        return true;
    }

    bool
    process_socket_stats_request(int fd, Connection *conn) {
        NOVA_LOG(rdmaio::INFO) << "Obtain stats";
        NICClientReqWorker *worker = (NICClientReqWorker *) conn->worker;
        int num_l0_sstables = 0;
        bool needs_compaction = false;
        Configuration *cfg = NovaConfig::config->cfgs[NovaConfig::config->current_cfg_id];

        for (auto frag : cfg->fragments) {
            auto db = reinterpret_cast<leveldb::DB *>(frag->db);
            if (!db) {
                continue;
            }
            leveldb::DBStats stats;
            stats.sstable_size_dist = new uint32_t[20];
            db->QueryDBStats(&stats);
            if (!needs_compaction) {
                needs_compaction = stats.needs_compaction;
            }
//            num_l0_sstables += stats.num_l0_sstables;
            delete stats.sstable_size_dist;
        }

        if (needs_compaction) {
            num_l0_sstables = 10000;
        }

        char *response_buf = worker->buf;
        int nlen = 0;
        int len = int_to_str(response_buf, num_l0_sstables);
        conn->response_buf = worker->buf;
        conn->response_size = len;
        return true;
    }

    bool
    process_close_stoc_files(int fd, Connection *conn) {
        NOVA_LOG(rdmaio::INFO) << "Close StoC files";
        NICClientReqWorker *worker = (NICClientReqWorker *) conn->worker;

        for (int i = 0; i < worker->rdma_threads.size(); i++) {
            auto *thread = reinterpret_cast<RDMAMsgHandler *>(worker->rdma_threads[i]);
            thread->should_pause = true;
        }

        // Wait until all rdma threads are paused.
        bool wait = true;
        while (wait) {
            wait = false;
            for (int i = 0; i < worker->rdma_threads.size(); i++) {
                auto *thread = reinterpret_cast<RDMAMsgHandler *>(worker->rdma_threads[i]);
                if (!thread->paused) {
                    wait = true;
                    break;
                }
            }
        }

        for (auto &fn : worker->stoc_file_manager_->fn_stoc_file_map_) {
            NOVA_LOG(rdmaio::INFO)
                << fmt::format("Close stoc file {} name:{} id:{}", fn.first,
                               fn.second->stoc_file_name_,
                               fn.second->file_id());
            fn.second->Close();
        }
        char *response_buf = worker->buf;
        int nlen = 1;
        int len = int_to_str(response_buf, nlen);
        conn->response_buf = worker->buf;
        conn->response_size = len + nlen;
        return true;
    }

    bool
    process_socket_scan(int fd, Connection *conn, char *request_buf,
                        uint32_t server_cfg_id) {
        NICClientReqWorker *worker = (NICClientReqWorker *) conn->worker;
        worker->stats.nscans++;
        char *startkey;
        uint64_t key = 0;
        char *buf = request_buf;
        startkey = buf;
        int nkey = str_to_int(buf, &key) - 1;
        buf += nkey + 1;
        uint64_t nrecords;
        buf += str_to_int(buf, &nrecords);
        std::string skey(startkey, nkey);
        NOVA_LOG(DEBUG)
            << fmt::format("memstore[{}]: scan fd:{} key:{} nkey:{} nrecords:{}", worker->thread_id_, fd, skey,
                           nkey, nrecords);
        uint64_t hv = keyhash(startkey, nkey);
        auto cfg = NovaConfig::config->cfgs[server_cfg_id];
        LTCFragment *frag = NovaConfig::home_fragment(hv, server_cfg_id);
        NOVA_ASSERT(frag) << fmt::format("cfg:{} key:{}", server_cfg_id, hv);

        leveldb::ReadOptions read_options;
        read_options.stoc_client = worker->stoc_client_;
        read_options.mem_manager = worker->mem_manager_;
        read_options.thread_id = worker->thread_id_;
        read_options.rdma_backing_mem = worker->rdma_backing_mem;
        read_options.rdma_backing_mem_size = worker->rdma_backing_mem_size;
        read_options.cfg_id = server_cfg_id;
        int pivot_db_id = frag->dbid;
        int read_records = 0;
        uint64_t prior_last_key = -1;
        uint64_t scan_size = 0;

        conn->response_buf = worker->buf;
        char *response_buf = conn->response_buf;
        uint32_t cfg_size = int_to_str(response_buf, server_cfg_id);
        response_buf += cfg_size;
        scan_size += cfg_size;

        while (read_records < nrecords && pivot_db_id < cfg->fragments.size()) {
            frag = cfg->fragments[pivot_db_id];
            if (prior_last_key != -1 && prior_last_key != frag->range.key_start) {
                break;
            }
            if (frag->ltc_server_id != NovaConfig::config->my_server_id) {
                break;
            }

            if (!frag->is_ready_) {
                frag->is_ready_mutex_.Lock();
                while (!frag->is_ready_) {
                    frag->is_ready_signal_.Wait();
                }
                frag->is_ready_mutex_.Unlock();
            }

            leveldb::DB *db = reinterpret_cast<leveldb::DB *>(frag->db);
//            if (frag->range.key_end - frag->range.key_start == 1) {
//                // A range contains a single key. Perform get instead of scan.
//                std::string value;
//                leveldb::ReadOptions read_options;
//                read_options.hash = frag->range.key_start;
//                read_options.stoc_client = worker->stoc_client_;
//                read_options.mem_manager = worker->mem_manager_;
//                read_options.thread_id = worker->thread_id_;
//                read_options.rdma_backing_mem = worker->rdma_backing_mem;
//                read_options.rdma_backing_mem_size = worker->rdma_backing_mem_size;
//                read_options.cfg_id = server_cfg_id;
//                uint32_t len = nova::nint_to_str(frag->range.key_start) + 1;
//                char keybuf[len];
//                nova::int_to_str(keybuf, frag->range.key_start);
//                leveldb::Slice key(keybuf, len - 1);
//                leveldb::Status s = db->Get(read_options, key, &value);
//                NOVA_ASSERT(s.ok())
//                    << fmt::format("k:{} status:{}", key.ToString(), s.ToString());
//
//                scan_size += nint_to_str(key.size()) + 1;
//                scan_size += key.size();
//                scan_size += nint_to_str(value.size()) + 1;
//                scan_size += value.size();
//
//                response_buf += int_to_str(response_buf, key.size());
//                memcpy(response_buf, key.data(), key.size());
//                response_buf += key.size();
//                response_buf += int_to_str(response_buf, value.size());
//                memcpy(response_buf, value.data(), value.size());
//                response_buf += value.size();
//                read_records++;
//
//                prior_last_key = frag->range.key_end;
//                pivot_db_id += 1;
//                continue;
//            }
            leveldb::Iterator *iterator = db->NewIterator(read_options);
            iterator->Seek(startkey);
            while (iterator->Valid() && read_records < nrecords) {
                leveldb::Slice key = iterator->key();
                leveldb::Slice value = iterator->value();
                scan_size += nint_to_str(key.size()) + 1;
                scan_size += key.size();
                scan_size += nint_to_str(value.size()) + 1;
                scan_size += value.size();

                response_buf += int_to_str(response_buf, key.size());
                memcpy(response_buf, key.data(), key.size());
                response_buf += key.size();
                response_buf += int_to_str(response_buf, value.size());
                memcpy(response_buf, value.data(), value.size());
                response_buf += value.size();
                read_records++;
//                NOVA_LOG(rdmaio::INFO) << fmt::format("Getting key {}", key.ToString());
                iterator->Next();
            }
//            NOVA_LOG(rdmaio::INFO) << fmt::format("Go to next range partition {}", pivot_db_id + 1);
            delete iterator;
            prior_last_key = frag->range.key_end;
            pivot_db_id += 1;
        }

        NOVA_LOG(rdmaio::DEBUG) << fmt::format("Scan size:{}", scan_size);

        conn->response_buf[scan_size] = MSG_TERMINATER_CHAR;
        scan_size += 1;
        conn->response_size = scan_size;
        NOVA_ASSERT(conn->response_size < NovaConfig::config->max_msg_size);
        return true;
    }

    std::atomic_int_fast32_t total_writes;

    bool process_socket_put(int fd, Connection *conn, char *request_buf, uint32_t server_cfg_id) {
        // Stats.
        NICClientReqWorker *worker = (NICClientReqWorker *) conn->worker;
        worker->stats.nputs++;
        char *buf = request_buf;
        char *ckey;
        uint64_t key = 0;
        ckey = buf;
        int nkey = str_to_int(buf, &key) - 1;
        buf += nkey + 1;
        uint64_t nval;
        buf += str_to_int(buf, &nval);
        char *val = buf;
        uint64_t hv = keyhash(ckey, nkey);
        // I'm the home.
        leveldb::Slice dbkey(ckey, nkey);
        leveldb::Slice dbval(val, nval);

        worker->ResetReplicateState();
        worker->replicate_log_record_states[0].cfgid = server_cfg_id;
        leveldb::WriteOptions option;
        option.stoc_client = worker->stoc_client_;
        option.local_write = false;
        option.thread_id = worker->thread_id_;
        option.rand_seed = &worker->rand_seed;
        option.hash = key;
        option.total_writes = total_writes.fetch_add(1, std::memory_order_relaxed) + 1;
        option.replicate_log_record_states = worker->replicate_log_record_states;
        option.rdma_backing_mem = worker->rdma_backing_mem;
        option.rdma_backing_mem_size = worker->rdma_backing_mem_size;
        option.is_loading_db = false;
        LTCFragment *frag = NovaConfig::home_fragment(hv, server_cfg_id);
        NOVA_ASSERT(frag) << fmt::format("cfg:{} key:{}", server_cfg_id, hv);

        if (!frag->is_ready_) {
            frag->is_ready_mutex_.Lock();
            while (!frag->is_ready_) {
                frag->is_ready_signal_.Wait();
            }
            frag->is_ready_mutex_.Unlock();
        }

        leveldb::DB *db = reinterpret_cast<leveldb::DB *>(frag->db);
        NOVA_ASSERT(db) << fmt::format("cfg:{} key:{}", server_cfg_id, hv);

        leveldb::Status status = db->Put(option, dbkey, dbval);
        NOVA_ASSERT(status.ok()) << status.ToString();

        char *response_buf = worker->buf;
        uint32_t response_size = 0;
        uint32_t cfg_size = int_to_str(response_buf, server_cfg_id);
        response_buf += cfg_size;
        response_size += cfg_size;
        response_buf[0] = MSG_TERMINATER_CHAR;
        response_size += 1;
        conn->response_buf = worker->buf;
        conn->response_size = response_size;
        return true;
    }

    bool process_socket_request_handler(int fd, Connection *conn) {
        auto worker = (NICClientReqWorker *) conn->worker;
        char *request_buf = worker->request_buf;
        char msg_type = request_buf[0];
        request_buf++;
        uint32_t server_cfg_id = NovaConfig::config->current_cfg_id;
        if (msg_type == RequestType::GET || msg_type == RequestType::REQ_SCAN ||
            msg_type == RequestType::PUT) {
            uint64_t client_cfg_id = 0;
            request_buf += str_to_int(request_buf, &client_cfg_id);
            if (client_cfg_id != server_cfg_id) {
                char *response_buf = worker->buf;
                int len = int_to_str(response_buf, server_cfg_id);
                response_buf += len;
                response_buf[0] = MSG_TERMINATER_CHAR;
                conn->response_buf = worker->buf;
                conn->response_size = len + 1;
                return true;
            }
        }
        if (msg_type == RequestType::GET) {
            return process_socket_get(fd, conn, request_buf, server_cfg_id);
        } else if (msg_type == RequestType::REQ_SCAN) {
            return process_socket_scan(fd, conn, request_buf, server_cfg_id);
        } else if (msg_type == RequestType::PUT) {
            return process_socket_put(fd, conn, request_buf, server_cfg_id);
        } else if (msg_type == RequestType::REINITIALIZE_QP) {
            return process_reintialize_qps(fd, conn);
        } else if (msg_type == RequestType::CLOSE_STOC_FILES) {
            return process_close_stoc_files(fd, conn);
        } else if (msg_type == RequestType::STATS) {
            return process_socket_stats_request(fd, conn);
        } else if (msg_type == RequestType::CHANGE_CONFIG) {
            return process_socket_change_config_request(fd, conn);
        } else if (msg_type == RequestType::QUERY_CONFIG_CHANGE) {
            return process_socket_query_ready_request(fd, conn);
        }
        NOVA_ASSERT(false) << msg_type;
        return false;
    }

    SocketState socket_read_handler(int fd, short which, Connection *conn) {
        NOVA_ASSERT((which & EV_READ) > 0) << which;
        NICClientReqWorker *worker = (NICClientReqWorker *) conn->worker;
        char *buf = worker->request_buf + worker->req_ind;
        bool complete = false;

        if (worker->req_ind == 0) {
            int count = read(fd, buf, NovaConfig::config->max_msg_size);
            worker->stats.nreads++;
            if (count <= 0) {
                if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    worker->stats.nreadsagain++;
                    return INCOMPLETE;
                }
                return CLOSED;
            }
            if (count > 0) {
                if (buf[count - 1] == MSG_TERMINATER_CHAR) {
                    complete = true;
                }
                worker->req_ind += count;
                buf += count;
            }
        }
        while (!complete) {
            int count = read(fd, buf, 1);
            worker->stats.nreads++;
            if (count <= 0) {
                if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    worker->stats.nreadsagain++;
                    return INCOMPLETE;
                }
                return CLOSED;
            }
            if (count > 0) {
                if (buf[0] == MSG_TERMINATER_CHAR) {
                    break;
                }
                worker->req_ind += 1;
                buf += 1;
                NOVA_ASSERT(worker->req_ind < NovaConfig::config->max_msg_size);
            }
        }
        return COMPLETE;
    }

    void stats_handler(int fd, short which, void *arg) {
        NICClientReqWorker *store = (NICClientReqWorker *) arg;
        Stats diff = store->stats.diff(store->prev_stats);
        uint64_t service_time = 0;
        uint64_t read_service_time = 0;
        uint64_t write_service_time = 0;
        if (store->stats.nreqs > 0) {
            service_time = store->stats.service_time / store->stats.nreqs;
            read_service_time =
                    store->stats.read_service_time / store->stats.nreqs;
            write_service_time =
                    store->stats.write_service_time / store->stats.nreqs;
        }

        int asize = 0;
        int csize = 0;
        NOVA_LOG(INFO) << "memstore[" << store->thread_id_ << "]: req="
                       << diff.nreqs
                       << " res=" << diff.nresponses
                       << " r=" << diff.nreads
                       << " asize=" << asize
                       << " csize=" << csize
                       << " ra=" << diff.nreadsagain
                       << " w=" << diff.nwrites
                       << " wa=" << diff.nwritesagain
                       << " g=" << diff.ngets
                       << " p=" << diff.nputs
                       << " range=" << diff.nscans
                       << " gh=" << diff.nget_hits
                       << " pl=" << diff.nput_lc
                       << " gl=" << diff.nget_lc
                       << " glh=" << diff.nget_lc_hits
                       << " rg=" << diff.nget_rdma
                       << " rgs=" << diff.nget_rdma_stale
                       << " rgi=" << diff.nget_rdma_invalid
                       << " rig=" << diff.ngetindex_rdma
                       << " rigd=" << diff.ngetindex_rdma_indirect
                       << " rigi=" << diff.ngetindex_rdma_invalid
                       << " st=" << service_time
                       << " rst=" << read_service_time
                       << " wst=" << write_service_time
                       << " treq=" << store->stats.nreqs
                       << " tres=" << store->stats.nresponses
                       << " tput=" << store->stats.nputs
                       << " treplicate=" << store->stats.nreplicate_log_records
                       << " tclose=" << store->stats.nremove_log_records;
        store->prev_stats = store->stats;

//        if (store->store_id_ == 0) {
//            for (int i = 0; i < store->dbs_.size(); i++) {
//                RDMA_LOG(INFO) << "Database index " + i;
//                std::string value;
//                store->dbs_[i]->GetProperty("leveldb.sstables", &value);
//                RDMA_LOG(INFO) << "\n" << value;
//                value.clear();
//                store->dbs_[i]->GetProperty("leveldb.approximate-memory-usage",
//                                            &value);
//                RDMA_LOG(INFO) << "\n" << "leveldb memory usage " << value;
//            }
//        }
    }

    void new_conn_handler(int fd, short which, void *arg) {
        NICClientReqWorker *store = (NICClientReqWorker *) arg;
        new_conn_mutex.lock();
        store->conn_mu.lock();
        store->nconns += store->conn_queue.size();
        if (store->conn_queue.size() != 0) {
            NOVA_LOG(DEBUG) << "memstore[" << store->thread_id_ << "]: conns "
                            << store->nconns;
        }
        for (int i = 0; i < store->conn_queue.size(); i++) {
            int client_fd = store->conn_queue[i];
            Connection *conn = new Connection();
            conn->Init(client_fd, store);
            store->conns.push_back(conn);
            NOVA_ASSERT(client_fd < NOVA_MAX_CONN) << "memstore["
                                                   << store->thread_id_
                                                   << "]: too large "
                                                   << client_fd;
            nova_conns[client_fd] = conn;
            NOVA_LOG(DEBUG) << "memstore[" << store->thread_id_
                            << "]: connected "
                            << client_fd;
            NOVA_ASSERT(event_assign(&conn->event, store->base, client_fd,
                                     EV_READ | EV_PERSIST, event_handler,
                                     conn) ==
                        0)
                << fd;
            NOVA_ASSERT(event_add(&conn->event, 0) == 0) << client_fd;
        }
        store->conn_queue.clear();
        store->conn_mu.unlock();
        new_conn_mutex.unlock();
    }

    void NICClientReqWorker::Start() {
        NOVA_LOG(DEBUG) << "memstore[" << thread_id_ << "]: "
                        << "starting mem worker";

        nova::NovaConfig::config->add_tid_mapping();

        struct event new_conn_timer_event;
        struct event stats_event;
        struct event_config *ev_config;
        ev_config = event_config_new();
        NOVA_ASSERT(
                event_config_set_flag(ev_config, EVENT_BASE_FLAG_NOLOCK) == 0);
        NOVA_ASSERT(event_config_avoid_method(ev_config, "poll") == 0);
        NOVA_ASSERT(event_config_avoid_method(ev_config, "select") == 0);
        NOVA_ASSERT(event_config_set_flag(ev_config,
                                          EVENT_BASE_FLAG_EPOLL_USE_CHANGELIST) ==
                    0);
        base = event_base_new_with_config(ev_config);
        if (!base) {
            fprintf(stderr, "Can't allocate event base\n");
            exit(1);
        }
        NOVA_LOG(DEBUG) << "Using Libevent with backend method "
                        << event_base_get_method(base);
        const int f = event_base_get_features(base);
        if ((f & EV_FEATURE_ET)) {
            NOVA_LOG(DEBUG) << "Edge-triggered events are supported.";
        }

        if ((f & EV_FEATURE_O1)) {
            NOVA_LOG(DEBUG) <<
                            "O(1) event notification is supported.";
        }

        if ((f & EV_FEATURE_FDS)) {
            NOVA_LOG(DEBUG) << "All FD types are supported.";
        }

        /* Timer event for new connection */
        {
            struct timeval tv;
            tv.tv_sec = 2;
            tv.tv_usec = 0;
            memset(&new_conn_timer_event, 0, sizeof(struct event));
            NOVA_ASSERT(
                    event_assign(&new_conn_timer_event, base, -1, EV_PERSIST,
                                 new_conn_handler, (void *) this) == 0);
            NOVA_ASSERT(event_add(&new_conn_timer_event, &tv) == 0);
        }
        /* Timer event for stats */
//        {
//            struct timeval tv;
//            tv.tv_sec = 10;
//            tv.tv_usec = 0;
//            memset(&stats_event, 0, sizeof(struct event));
//            RDMA_ASSERT(
//                    event_assign(&stats_event, base, -1, EV_PERSIST,
//                                 stats_handler,
//                                 (void *) this) == 0);
//            RDMA_ASSERT(event_add(&stats_event, &tv) == 0);
//        }
        /* Timer event for RDMA */
//        if (NovaConfig::config->enable_rdma) {
//            struct timeval tv;
//            tv.tv_sec = 0;
//            tv.tv_usec = 10000;
//            memset(&rdma_timer_event, 0, sizeof(struct event));
//            RDMA_ASSERT(
//                    event_assign(&rdma_timer_event, base, -1, EV_PERSIST,
//                                 rdma_timer_event_handler, (void *) this) == 0);
//            RDMA_ASSERT(event_add(&rdma_timer_event, &tv) == 0);
//        }
        NOVA_ASSERT(event_base_loop(base, 0) == 0);
        NOVA_LOG(INFO) << "started";
    }

    void Connection::Init(int f, void *store) {
        fd = f;
        req_size = -1;
        response_ind = 0;
        response_size = 0;
        state = READ;
        this->worker = store;
        event_flags = EV_READ | EV_PERSIST;
    }

    void Connection::UpdateEventFlags(int new_flags) {
        if (event_flags == new_flags) {
            return;
        }
        event_flags = new_flags;
        NOVA_ASSERT(event_del(&event) == 0) << fd;
        NOVA_ASSERT(
                event_assign(&event, ((NICClientReqWorker *) worker)->base, fd,
                             new_flags,
                             event_handler,
                             this) ==
                0) << fd;
        NOVA_ASSERT(event_add(&event, 0) == 0) << fd;
    }
}