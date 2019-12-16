
//
// Created by Haoyu Huang on 3/28/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova_mem_worker.h"

#include "logging.hpp"
#include "nova_common.h"
#include "nova_mem_config.h"

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

namespace nova {
    using namespace rdmaio;

    Connection *nova_conns[NOVA_MAX_CONN];
    mutex new_conn_mutex;

    void timer_event_handler(int fd, short event, void *arg) {
        auto *worker = (NovaMemWorker *) arg;
        worker->rdma_store_->FlushPendingSends();
        worker->rdma_store_->PollSQ();
    }

    SocketState socket_write_handler(int fd, Connection *conn) {
        RDMA_LOG(DEBUG) << "WSOCK " << conn->response_buf;
        RDMA_ASSERT(conn->response_size < NovaConfig::config->max_msg_size);
        NovaMemWorker *store = conn->worker;
        struct iovec iovec_array[1];
        iovec_array[0].iov_base = conn->response_buf + conn->response_ind;
        iovec_array[0].iov_len = conn->response_size - conn->response_ind;
        struct msghdr msg;
        memset(&msg, 0, sizeof(msg));
        msg.msg_iov = &iovec_array[0];
        msg.msg_iovlen = 1;
        int n = 0;
        int total = 0;
        do {
            iovec_array[0].iov_base = (char *) (iovec_array[0].iov_base) + n;
            iovec_array[0].iov_len -= n;
            n = sendmsg(fd, &msg, MSG_NOSIGNAL);
            if (n <= 0) {
                if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    conn->worker->stats.nwritesagain++;
                    RDMA_LOG(WARNING) << "memstore[" << store->thread_id_
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
            conn->worker->stats.nwrites++;
        } while (total < conn->response_size);
        return COMPLETE;
    }

    void event_handler(int fd, short which, void *arg) {
        auto *conn = (Connection *) arg;
        RDMA_ASSERT(fd == conn->fd) << fd << ":" << conn->fd;
        SocketState state;

        if (conn->state == ConnState::READ) {
            if (conn->worker->stats.nreqs % 100 == 0) {
                gettimeofday(&conn->worker->start, nullptr);
                conn->worker->read_start = conn->worker->start;
            }
            state = socket_read_handler(fd, which, conn);
            if (state == COMPLETE) {
                if (conn->worker->stats.nreqs % 99 == 0 &&
                    conn->worker->stats.nreqs > 0) {
                    timeval now{};
                    gettimeofday(&now, nullptr);
                    auto elapsed =
                            (now.tv_sec - conn->worker->read_start.tv_sec) *
                            1000 *
                            1000 +
                            (now.tv_usec - conn->worker->read_start.tv_usec);
                    conn->worker->stats.read_service_time +=
                            elapsed;
                }
                bool reply = process_socket_request_handler(fd, conn);
                if (reply) {
                    if (conn->worker->stats.nreqs % 100 == 0) {
                        gettimeofday(&conn->worker->write_start, nullptr);
                    }
                    state = socket_write_handler(fd, conn);
                    if (state == COMPLETE) {
                        write_socket_complete(fd, conn);
                    }
                }
                conn->worker->stats.nreqs++;
                conn->worker->stats.nreqs_to_poll_rdma++;
            }
        } else {
            RDMA_ASSERT((which & EV_WRITE) > 0);
            state = socket_write_handler(fd, conn);
            if (state == COMPLETE) {
                write_socket_complete(fd, conn);
            }
        }

        if (state == CLOSED) {
            RDMA_LOG(INFO) << "closed " << fd;
            RDMA_ASSERT(event_del(&conn->event) == 0) << fd;
            close(fd);
        }

        if (NovaConfig::config->enable_rdma) {
            if (conn->worker->stats.nreqs_to_poll_rdma ==
                NovaConfig::config->rdma_pq_batch_size) {
                conn->worker->rdma_store_->FlushPendingSends();
                conn->worker->rdma_store_->PollSQ();
                conn->worker->stats.nreqs_to_poll_rdma = 0;
            }
        }
    }

    void write_socket_complete(int fd, Connection *conn) {
        if (conn->worker->stats.nreqs % 99 == 0 &&
            conn->worker->stats.nreqs > 0) {
            timeval now{};
            gettimeofday(&now, nullptr);
            auto elapsed =
                    (now.tv_sec - conn->worker->start.tv_sec) * 1000 *
                    1000 + (now.tv_usec - conn->worker->start.tv_usec);
            auto write_elapsed =
                    (now.tv_sec - conn->worker->write_start.tv_sec) * 1000 *
                    1000 + (now.tv_usec - conn->worker->write_start.tv_usec);
            conn->worker->stats.service_time +=
                    elapsed;
            conn->worker->stats.write_service_time += write_elapsed;
        }
        conn->UpdateEventFlags(EV_READ | EV_PERSIST);
        // processing complete.
        conn->state = READ;
        conn->req_ind = 0;
        conn->response_ind = 0;
    }

    bool
    process_socket_get(int fd, Connection *conn, bool no_redirect) {
        // Stats.
        conn->worker->stats.ngets++;
        char *buf = conn->request_buf;
        RDMA_ASSERT(
                buf[0] == RequestType::GET || buf[0] == RequestType::FORCE_GET)
            << buf;
        buf++;
        uint64_t int_key = 0;
        uint32_t nkey = str_to_int(buf, &int_key) - 1;
        uint64_t hv = NovaConfig::keyhash(buf, nkey);
        char *tmp = buf;
        tmp += nkey + 1;
        Fragment *frag = NovaConfig::home_fragment(hv);
        NovaMemWorker *store = conn->worker;
        RDMA_LOG(DEBUG) << "memstore[" << store->thread_id_ << "]: "
                        << " Get fd:"
                        << fd << " key:" << int_key << " nkey:" << nkey
                        << " hv:"
                        << hv << " home:" << frag->server_id
                        << " buf:" << conn->request_buf;
        int home_server = frag->server_id;
        conn->worker->stats.nget_hits++;

        std::string value;
        leveldb::Slice key(buf, nkey);
        leveldb::Status s = store->db_->Get(leveldb::ReadOptions(), key,
                                            &value);
        RDMA_ASSERT(s.ok());
//    RDMA_ASSERT(value.size() == NovaConfig::config->load_default_value_size);
        conn->response_buf = conn->buf;
        char *response_buf = conn->response_buf;
        conn->response_size = nint_to_str(value.size()) + 1 + 1 + value.size();

        response_buf += int_to_str(response_buf, value.size() + 1);
        response_buf[0] = 'h';
        response_buf += 1;
        memcpy(response_buf, value.data(), value.size());
        RDMA_ASSERT(
                conn->response_size < NovaConfig::config->max_msg_size);
        return true;
    }

    bool
    process_socket_range(int fd, Connection *conn) {
        // Stats.
        conn->worker->stats.nranges++;
        char *buf = conn->request_buf;
        RDMA_ASSERT(buf[0] == RequestType::REQ_RANGE) << buf;
        char *startkey;

        uint64_t key = 0;
        buf++;
        startkey = buf;
        int nkey = str_to_int(buf, &key) - 1;
        buf += nkey + 1;
        uint64_t nrecords;
        buf += str_to_int(buf, &nrecords);

        NovaMemWorker *store = conn->worker;
        std::string skey(startkey, nkey);
        RDMA_LOG(DEBUG) << "memstore[" << store->thread_id_ << "]: "
                        << " Range fd:"
                        << fd << " key:" << skey << " nkey:" << nkey
                        << " nrecords: " << nrecords;

        leveldb::Iterator *iterator = store->db_->NewIterator(
                leveldb::ReadOptions());
        iterator->Seek(startkey);
        int records = 0;
        leveldb::Slice keys[nrecords];
        leveldb::Slice values[nrecords];
        uint64_t rangesize = 0;
        while (iterator->Valid() && records < nrecords) {
            keys[records] = iterator->key();
            values[records] = iterator->value();
            rangesize += nint_to_str(keys[records].size()) + 1;
            rangesize += keys[records].size();
            rangesize += nint_to_str(values[records].size()) + 1;
            rangesize += values[records].size();
            RDMA_LOG(DEBUG) << "memstore[" << store->thread_id_ << "]: "
                            << " Range key " << keys[records].ToString()
                            << " value "
                            << values[records].ToString();
            records++;
            iterator->Next();
        }

        conn->response_buf = conn->buf;
        char *response_buf = conn->response_buf;
        conn->response_size = nint_to_str(rangesize) + 1 + rangesize;
        response_buf += int_to_str(response_buf, rangesize);

        for (int i = 0; i < records; i++) {
            response_buf += int_to_str(response_buf, keys[i].size());
            memcpy(response_buf, keys[i].data(), keys[i].size());
            response_buf += keys[i].size();
            response_buf += int_to_str(response_buf, values[i].size());
            memcpy(response_buf, values[i].data(), values[i].size());
            response_buf += values[i].size();
        }

        RDMA_ASSERT(
                conn->response_size < NovaConfig::config->max_msg_size);
        return true;
    }

    bool process_socket_put(int fd, Connection *conn) {
        // Stats.
        conn->worker->stats.nputs++;
        char *buf = conn->request_buf;
        RDMA_ASSERT(buf[0] == RequestType::PUT) << buf;
        char *ckey;
        uint64_t key = 0;
        buf++;
        ckey = buf;
        int nkey = str_to_int(buf, &key) - 1;
        buf += nkey + 1;
        uint64_t nval;
        buf += str_to_int(buf, &nval);
        char *val = buf;

        Fragment *frag = NovaConfig::home_fragment(key);
        NovaMemWorker *store = conn->worker;
        RDMA_LOG(DEBUG) << "memstore[" << store->thread_id_ << "]: "
                        << " put fd:"
                        << fd << ": key:" << key << " nkey:" << nkey << " nval:"
                        << nval << " buf:" << conn->request_buf;
//    RDMA_ASSERT(nval == NovaConfig::config->load_default_value_size);
        int home_server = frag->server_id;
        // I'm the home.
        leveldb::Slice dbkey(ckey, nkey);
        leveldb::Slice dbval(val, nval);
        leveldb::WriteOptions option;
        option.sync = NovaConfig::config->fsync;
        leveldb::Status status = store->db_->Put(option, dbkey, dbval);
        RDMA_ASSERT(status.ok()) << status.ToString();

//    RDMA_ASSERT(home_server == NovaConfig::config->my_server_id);
//    RDMA_ASSERT(
//            store->mem_manager_->LocalPut(ckey, nkey, val, nval, true,
//                                          false).success);
        char *response_buf = conn->buf;
        int nlen = 1;
        int len = int_to_str(response_buf, nlen);
        conn->response_buf = conn->buf;
        conn->response_size = len + nlen;
        RDMA_ASSERT(conn->response_size < NovaConfig::config->max_msg_size);
        return true;
    }

    bool process_socket_request_handler(int fd, Connection *conn) {
        char *buf = conn->request_buf;
        if (buf[0] == RequestType::GET) {
            return process_socket_get(fd, conn, /*no_redirect=*/false);
        }
        if (buf[0] == RequestType::FORCE_GET) {
            return process_socket_get(fd, conn, /*no_redirect=*/true);
        }
        if (buf[0] == RequestType::REQ_RANGE) {
            return process_socket_range(fd, conn);
        }
        if (buf[0] == RequestType::PUT) {
            return process_socket_put(fd, conn);
        }
        RDMA_ASSERT(false);
        return false;
    }

    SocketState socket_read_handler(int fd, short which, Connection *conn) {
        RDMA_ASSERT((which & EV_READ) > 0) << which;
        char *buf = conn->request_buf + conn->req_ind;
        bool complete = false;
        if (conn->req_ind == 0) {
            int count = read(fd, buf, NovaConfig::config->max_msg_size);
            conn->worker->stats.nreads++;
            if (count <= 0) {
                if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    conn->worker->stats.nreadsagain++;
                    return INCOMPLETE;
                }
                return CLOSED;
            }
            if (count > 0) {
                if (buf[count - 1] == MSG_TERMINATER_CHAR) {
                    complete = true;
                }
                conn->req_ind += count;
                buf += count;
            }
        }
        while (!complete) {
            int count = read(fd, buf, 1);
            conn->worker->stats.nreads++;
            if (count <= 0) {
                if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    conn->worker->stats.nreadsagain++;
                    return INCOMPLETE;
                }
                return CLOSED;
            }
            if (count > 0) {
                if (buf[0] == MSG_TERMINATER_CHAR) {
                    break;
                }
                conn->req_ind += 1;
                buf += 1;
                RDMA_ASSERT(conn->req_ind < NovaConfig::config->max_msg_size);
            }
        }
        return COMPLETE;
    }

    void stats_handler(int fd, short which, void *arg) {
        NovaMemWorker *store = (NovaMemWorker *) arg;
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
        RDMA_LOG(INFO) << "memstore[" << store->thread_id_ << "]: req="
                       << diff.nreqs
                       << " r=" << diff.nreads
                       << " ra=" << diff.nreadsagain
                       << " w=" << diff.nwrites
                       << " wa=" << diff.nwritesagain
                       << " g=" << diff.ngets
                       << " p=" << diff.nputs
                       << " range=" << diff.nranges
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
                       << " wst=" << write_service_time;
        store->prev_stats = store->stats;

        if (store->store_id_ == 0) {
            std::string value;
            store->db_->GetProperty("leveldb.sstables", &value);
            RDMA_LOG(INFO) << "\n" << value;
            value.clear();
            store->db_->GetProperty("leveldb.approximate-memory-usage", &value);
            RDMA_LOG(INFO) << "\n" << "leveldb memory usage " << value;
        }
    }

    void new_conn_handler(int fd, short which, void *arg) {
        NovaMemWorker *store = (NovaMemWorker *) arg;
        new_conn_mutex.lock();
        store->conn_mu.lock();
        store->nconns += store->conn_queue.size();
        if (store->conn_queue.size() != 0) {
            RDMA_LOG(INFO) << "memstore[" << store->thread_id_ << "]: conns "
                           << store->nconns;
        }
        for (int i = 0; i < store->conn_queue.size(); i++) {
            int client_fd = store->conn_queue[i];
            Connection *conn = new Connection();
            conn->Init(client_fd, store);
            store->conns.push_back(conn);
            RDMA_ASSERT(client_fd < NOVA_MAX_CONN) << "memstore["
                                                   << store->thread_id_
                                                   << "]: too large "
                                                   << client_fd;
            nova_conns[client_fd] = conn;
            RDMA_LOG(DEBUG) << "memstore[" << store->thread_id_
                            << "]: connected "
                            << client_fd;
            RDMA_ASSERT(event_assign(&conn->event, store->base, client_fd,
                                     EV_READ | EV_PERSIST, event_handler,
                                     conn) ==
                        0)
                << fd;
            RDMA_ASSERT(event_add(&conn->event, 0) == 0) << client_fd;
        }
        store->conn_queue.clear();
        store->conn_mu.unlock();
        new_conn_mutex.unlock();
    }

//void signal_new_conn_handler(int fd, short which, void *arg) {
//    NovaMemWorker *store = (NovaMemWorker *) arg;
//    RDMA_ASSERT(fd == store->on_new_conn_recv_fd);
//
//    char buf[1];
//    int n = read(fd, buf, 1);
//    RDMA_ASSERT(n > 0);
//    RDMA_ASSERT(buf[0] == 'c');
//    new_conn_handler(fd, which, store);
//}

    void NovaMemWorker::Start() {
        RDMA_LOG(INFO) << "memstore[" << thread_id_ << "]: "
                       << "starting mem worker";
        if (NovaConfig::config->enable_rdma) {
            rdma_store_->Init();
        }
        struct event event;
        struct event new_conn_timer_event;
        struct event rdma_timer_event;
        struct event stats_event;
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

        int fds[2];
        RDMA_ASSERT(pipe(fds) == 0);
        on_new_conn_recv_fd = fds[0];
        on_new_conn_send_fd = fds[1];

        /* Listen for notifications from other threads */
//    memset(&event, 0, sizeof(struct event));
//    RDMA_ASSERT(
//            event_assign(&event, base, on_new_conn_recv_fd, EV_READ | EV_PERSIST, signal_new_conn_handler,
//                         (void *) this) == 0)
//        << on_new_conn_recv_fd;
//    RDMA_ASSERT(event_add(&event, 0) == 0) << on_new_conn_recv_fd;

        /* Timer event for new connection */
        {
            struct timeval tv;
            tv.tv_sec = 2;
            tv.tv_usec = 0;
            memset(&new_conn_timer_event, 0, sizeof(struct event));
            RDMA_ASSERT(
                    event_assign(&new_conn_timer_event, base, -1, EV_PERSIST,
                                 new_conn_handler, (void *) this) == 0);
            RDMA_ASSERT(event_add(&new_conn_timer_event, &tv) == 0);
        }
        /* Timer event for stats */
        {
            struct timeval tv;
            tv.tv_sec = 10;
            tv.tv_usec = 0;
            memset(&stats_event, 0, sizeof(struct event));
            RDMA_ASSERT(
                    event_assign(&stats_event, base, -1, EV_PERSIST,
                                 stats_handler,
                                 (void *) this) == 0);
            RDMA_ASSERT(event_add(&stats_event, &tv) == 0);
        }
        /* Timer event for RDMA */
        if (NovaConfig::config->enable_rdma) {
            struct timeval tv;
            tv.tv_sec = 1;
            tv.tv_usec = 0;
            memset(&rdma_timer_event, 0, sizeof(struct event));
            RDMA_ASSERT(
                    event_assign(&rdma_timer_event, base, -1, EV_PERSIST,
                                 timer_event_handler, (void *) this) == 0);
            RDMA_ASSERT(event_add(&rdma_timer_event, &tv) == 0);
        }
        RDMA_ASSERT(event_base_loop(base, 0) == 0) << on_new_conn_recv_fd;
        RDMA_LOG(INFO) << "started";
    }

    void NovaMemWorker::ProcessRDMAREAD(char *buf) {
        uint64_t to_server_id = 0;
        uint64_t to_sock_fd = 0;
        char *key = nullptr;
        uint64_t nkey = 0;
        RequestType type;
        uint32_t req_len = ParseRDMARequest(buf, &type, &to_server_id,
                                            &to_sock_fd,
                                            &key, &nkey);
        RDMA_ASSERT(to_sock_fd < NOVA_MAX_CONN) << to_sock_fd;
        uint64_t hash = NovaConfig::keyhash(key, nkey);
        Connection *conn = nova_conns[to_sock_fd];
        char *databuf = buf + req_len;
        bool invalid = false;

        RDMA_LOG(DEBUG) << "rdma-rc[" << thread_id_ << "]: "
                        << "SQ: READ completes sid:" << to_server_id << " fd:"
                        << to_sock_fd << " key:" << key << " nkey:" << nkey
                        << " hash:" << hash
                        << " type:" << type << " buf:" << buf;

        if (type == RequestType::GET) {
            DataEntry entry = DataEntry::chars_to_dataitem(databuf);
            uint64_t fetched_key = 0;
            str_to_int(entry.user_key(), &fetched_key, entry.nkey);
            if (entry.stale) {
                invalid = true;
                stats.nget_rdma_stale++;
            } else if (entry.nkey + entry.nval >
                       NovaConfig::config->max_msg_size ||
                       entry.nkey != nkey ||
                       memcmp(entry.user_key(), key, nkey) != 0) {
                invalid = true;
            } else {
                // Verify checksum.
                uint64_t checksum = entry.compute_checksum(databuf);
                if (checksum != entry.checksum) {
                    invalid = true;
                }
            }
            if (invalid) {
                if (conn->number_get_retries >
                    NovaConfig::config->rdma_number_of_get_retries) {
                    // Exceeds max number of retries.
                    conn->number_get_retries = 0;
                    ProcessRDMAGETResponse(to_sock_fd,
                                           nullptr, /*fetch_from_origin=*/true);
                    return;
                }
                stats.nget_rdma_invalid++;
                conn->number_get_retries++;
            }
            if (invalid) {
                PostRDMAGETIndexRequest(to_sock_fd, key, nkey,
                                        to_server_id, /*remote_offset=*/0);
            } else {
                ProcessRDMAGETResponse(to_sock_fd,
                                       &entry, /*fetch_from_origin=*/
                                       false);
            }
            return;
        }

        if (type == RequestType::GET_INDEX) {
            // get index completes.
            char *bucket = databuf;
            IndexEntry index_entry{};
            for (uint32_t i = 0;
                 i < NovaConfig::config->nindex_entry_per_bucket; i++) {
                char *index_entry_buf = bucket + i * IndexEntry::size();
                index_entry = IndexEntry::chars_to_indexitem(index_entry_buf);
                if (index_entry.type == IndexEntryType::EMPTY) {
                    // empty.
                    continue;
                }
                if (index_entry.type == IndexEntryType::INDRECT_HEADER) {
                    break;
                }
                uint64_t computed_checksum = index_entry.compute_checksum(
                        index_entry_buf);
                RDMA_ASSERT(index_entry.type == IndexEntryType::DATA);
                if (index_entry.checksum != computed_checksum) {
                    stats.ngetindex_rdma_invalid++;
                    invalid = true;
                    break;
                }
                if (index_entry.hash == hash) {
                    // Store it in the location cache.
                    conn->worker->stats.nput_lc++;
                    mem_manager_->RemotePut(index_entry);
                    // hash matches, fetch the data.
                    PostRDMAGETRequest(to_sock_fd, key, nkey, to_server_id,
                                       index_entry.data_ptr,
                                       index_entry.data_size);
                    return;
                }
            }
            if (invalid) {
                if (conn->number_get_retries >
                    NovaConfig::config->rdma_number_of_get_retries) {
                    // Exceeds max number of retries.
                    conn->number_get_retries = 0;
                    ProcessRDMAGETResponse(to_sock_fd,
                                           nullptr, /*fetch_from_origin=*/true);
                    return;
                }
                stats.nget_rdma_invalid++;
                conn->number_get_retries++;
            }
            if (invalid) {
                // Fetch the index bucket again.
                PostRDMAGETIndexRequest(to_sock_fd, key, nkey,
                                        to_server_id, /*remote_offset=*/0);
            } else {
                // Valid index.
                if (index_entry.type == IndexEntryType::INDRECT_HEADER) {
                    stats.ngetindex_rdma_indirect++;
                    PostRDMAGETIndexRequest(to_sock_fd, key, nkey, to_server_id,
                                            index_entry.data_ptr);
                } else {
                    // the key does not exist.
                    ProcessRDMAGETResponse(to_sock_fd,
                                           nullptr, /*fetch_from_origin=*/
                                           false);
                }
            }
        }
    }

    void
    NovaMemWorker::PostRDMAGETRequest(int fd, char *key, uint64_t nkey,
                                      int home_server,
                                      uint64_t remote_offset,
                                      uint64_t remote_size) {
        RDMA_ASSERT(remote_size <= NovaConfig::config->max_msg_size);
        stats.nget_rdma++;
        char *rdma_send_buf = rdma_store_->GetSendBuf(home_server);
        int local_offset = GenerateRDMARequest(GET, rdma_send_buf, home_server,
                                               fd,
                                               key, nkey);
        RDMA_LOG(DEBUG) << "memstore[" << thread_id_ << "]: " << " RDMA-Get fd:"
                        << fd << " key:" << key << " home:" << home_server
                        << " off:" << remote_offset << " size:" << remote_size
                        << " loff:" << local_offset;
        rdma_store_->PostRead(remote_size,
                              home_server,
                              local_offset, remote_offset, /*is_offset=*/false);
    }

    void
    NovaMemWorker::PostRDMAGETIndexRequest(int fd, char *key, uint64_t nkey,
                                           int home_server,
                                           uint64_t remote_addr) {
        uint64_t raddr = remote_addr;
        bool is_offset = false;
        stats.ngetindex_rdma++;
        uint32_t bucket_size =
                IndexEntry::size() *
                NovaConfig::config->nindex_entry_per_bucket;
        if (remote_addr == 0) {
            is_offset = true;
            raddr = NovaConfig::config->index_buf_offset +
                    (NovaConfig::keyhash(key, nkey) %
                     NovaConfig::config->nbuckets) * bucket_size;
        }
        char *rdma_send_buf = rdma_store_->GetSendBuf(home_server);
        int local_offset = GenerateRDMARequest(GET_INDEX, rdma_send_buf,
                                               home_server, fd, key, nkey);
        rdma_store_->PostRead(bucket_size, home_server, local_offset,
                              raddr, is_offset);
    }

    void NovaMemWorker::ProcessRDMAGETResponse(uint64_t to_sock_fd,
                                               DataEntry *entry,
                                               bool fetch_from_origin) {
        // return the value.
        Connection *conn = nova_conns[to_sock_fd];
        if (fetch_from_origin) {
            char *response_buf = conn->buf;
            int nlen = 1;
            int len = int_to_str(response_buf, nlen);
            response_buf += len;
            response_buf[0] = RequestType::FORCE_GET;
            conn->response_buf = conn->buf;
            conn->response_size = len + nlen;
        } else {
            if (entry == nullptr) {
                // A miss.
                char *response_buf = conn->buf;
                int nlen = 1;
                int len = int_to_str(response_buf, nlen);
                response_buf += len;
                response_buf[0] = RequestType::MISS;
                conn->response_buf = conn->buf;
                conn->response_size = len + nlen;
            } else {
                conn->response_buf = entry->value();
                conn->response_size =
                        nint_to_str(entry->nval) + 1 + 1 + entry->nval;
            }
        }
        RDMA_ASSERT(socket_write_handler(to_sock_fd, conn) == COMPLETE);
        write_socket_complete(to_sock_fd, conn);
    }

    void Connection::Init(int f, NovaMemWorker *store) {
        request_buf = (char *) malloc(NovaConfig::config->max_msg_size);
        buf = (char *) malloc(NovaConfig::config->max_msg_size);
        RDMA_ASSERT(request_buf != NULL);
        RDMA_ASSERT(buf != NULL);

        memset(request_buf, 0, NovaConfig::config->max_msg_size);
        memset(buf, 0, NovaConfig::config->max_msg_size);
        fd = f;
        req_ind = 0;
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
        RDMA_ASSERT(event_del(&event) == 0) << fd;
        RDMA_ASSERT(
                event_assign(&event, worker->base, fd, new_flags,
                             event_handler,
                             this) ==
                0) << fd;
        RDMA_ASSERT(event_add(&event, 0) == 0) << fd;
    }
}