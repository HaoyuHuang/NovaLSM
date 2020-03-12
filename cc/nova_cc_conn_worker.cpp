
//
// Created by Haoyu Huang on 3/28/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova_cc_conn_worker.h"

#include "nova/logging.hpp"
#include "nova/nova_common.h"
#include "nova/nova_config.h"
#include "nova/nova_client_sock.h"

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

namespace nova {
    using namespace rdmaio;

    Connection *nova_conns[NOVA_MAX_CONN];
    mutex new_conn_mutex;

    SocketState socket_write_handler(int fd, Connection *conn) {
//        RDMA_LOG(DEBUG) << "WSOCK " << conn->response_size;
        RDMA_ASSERT(conn->response_size < NovaConfig::config->max_msg_size);
        NovaCCConnWorker *store = (NovaCCConnWorker *) conn->worker;
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
            store->stats.nwrites++;
        } while (total < conn->response_size);
        return COMPLETE;
    }

    void event_handler(int fd, short which, void *arg) {
        auto *conn = (Connection *) arg;
        RDMA_ASSERT(fd == conn->fd) << fd << ":" << conn->fd;
        SocketState state;

        NovaCCConnWorker *worker = (NovaCCConnWorker *) conn->worker;

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
            RDMA_ASSERT((which & EV_WRITE) > 0);
            state = socket_write_handler(fd, conn);
            if (state == COMPLETE) {
                write_socket_complete(fd, conn);
            }
        }

        if (state == CLOSED) {
            RDMA_ASSERT(event_del(&conn->event) == 0) << fd;
            close(fd);
        }
    }

    void write_socket_complete(int fd, Connection *conn) {
        NovaCCConnWorker *worker = (NovaCCConnWorker *) conn->worker;

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
        conn->request_buf[0] = '~';
        conn->state = READ;
        conn->req_ind = 0;
        conn->response_ind = 0;
    }

    bool
    process_socket_get(int fd, Connection *conn, bool no_redirect) {
        // Stats.
        NovaCCConnWorker *worker = (NovaCCConnWorker *) conn->worker;
        worker->stats.ngets++;
        char *buf = conn->request_buf;
        RDMA_ASSERT(
                buf[0] == RequestType::GET || buf[0] == RequestType::FORCE_GET)
            << buf;
        buf++;
        uint64_t int_key = 0;
        uint32_t nkey = str_to_int(buf, &int_key) - 1;
        uint64_t hv = keyhash(buf, nkey);
        char *tmp = buf;
        tmp += nkey + 1;
//        RDMA_LOG(DEBUG) << "memstore[" << worker->thread_id_ << "]: "
//                        << " Get fd:"
//                        << fd << " key:" << int_key << " nkey:" << nkey
//                        << " hv:"
//                        << hv;
        worker->stats.nget_hits++;

        leveldb::Slice key(buf, nkey);

        CCFragment *frag = NovaCCConfig::home_fragment(hv);
        leveldb::DB *db = worker->dbs_[frag->dbid];
        std::string value;
        leveldb::ReadOptions read_options;
        read_options.hash = int_key;
        read_options.dc_client = worker->cc_client_;
        read_options.mem_manager = worker->mem_manager_;
        read_options.thread_id = worker->thread_id_;

        leveldb::Status s = db->Get(read_options, key, &value);
        RDMA_ASSERT(s.ok())
            << fmt::format("k:{} status:{}", key.ToString(), s.ToString());

        conn->response_buf = conn->buf;
        char *response_buf = conn->response_buf;
        conn->response_size =
                nint_to_str(value.size()) + 1 + 1 + value.size();

        response_buf += int_to_str(response_buf, value.size() + 1);
        response_buf[0] = 'h';
        response_buf += 1;
        memcpy(response_buf, value.data(), value.size());
        RDMA_ASSERT(conn->response_size <
                    NovaConfig::config->max_msg_size);
        return true;
    }

    bool
    process_socket_range(int fd, Connection *conn) {
        // Stats.
        NovaCCConnWorker *worker = (NovaCCConnWorker *) conn->worker;
        worker->stats.nranges++;
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

        std::string skey(startkey, nkey);
//        RDMA_LOG(DEBUG) << "memstore[" << worker->thread_id_ << "]: "
//                        << " Range fd:"
//                        << fd << " key:" << skey << " nkey:" << nkey
//                        << " nrecords: " << nrecords;
        uint64_t hv = keyhash(startkey, nkey);
        CCFragment *frag = NovaCCConfig::home_fragment(hv);
        leveldb::Iterator *iterator = worker->dbs_[frag->dbid]->NewIterator(
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
//            RDMA_LOG(DEBUG) << "memstore[" << worker->thread_id_ << "]: "
//                            << " Range key " << keys[records].ToString()
//                            << " value "
//                            << values[records].ToString();
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
        NovaCCConnWorker *worker = (NovaCCConnWorker *) conn->worker;
        worker->stats.nputs++;
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
        uint64_t hv = keyhash(ckey, nkey);
//        RDMA_LOG(DEBUG) << "memstore[" << worker->thread_id_ << "]: "
//                        << " put fd:"
//                        << fd << ": key:" << key << " nkey:" << nkey << " nval:"
//                        << nval;
        // I'm the home.
        leveldb::Slice dbkey(ckey, nkey);
        leveldb::Slice dbval(val, nval);

        leveldb::WriteOptions option;
        option.dc_client = worker->cc_client_;
        option.sync = true;
        option.local_write = false;
        option.thread_id = worker->thread_id_;
        option.rand_seed = &worker->rand_seed;
        option.hash = key;
        CCFragment *frag = NovaCCConfig::home_fragment(hv);
        leveldb::DB *db = worker->dbs_[frag->dbid];

//        leveldb::WriteBatch batch;
//        batch.Put(dbkey, dbval);
//        db->GenerateLogRecords(option, &batch);

        leveldb::Status status = db->Put(option, dbkey, dbval);
//        RDMA_LOG(DEBUG) << "############### CC worker processed task "
//                        << fd << ":" << dbkey.ToString();
        RDMA_ASSERT(status.ok()) << status.ToString();

        char *response_buf = conn->buf;
        int nlen = 1;
        int len = int_to_str(response_buf, nlen);
        conn->response_buf = conn->buf;
        conn->response_size = len + nlen;
        return true;
    }

    bool process_socket_delete_log_file(int fd, Connection *conn) {
        // Stats.
//        NovaCCConnWorker *worker = (NovaCCConnWorker *) conn->worker;
//        worker->stats.nremove_log_records++;
//        char *buf = conn->request_buf;
//        RDMA_ASSERT(buf[0] == RequestType::DELETE_LOG_FILE) << buf;
//        buf++;
//        uint32_t logfilename_size = leveldb::DecodeFixed32(buf);
//        std::string logfile(buf + 4, logfilename_size);
//
//        RDMA_LOG(DEBUG) << "memstore[" << worker->thread_id_ << "]: "
//                        << " delete log file fd:"
//                        << fd << ": log:" << logfile << " nlog:"
//                        << logfilename_size << " buf:" << conn->request_buf;
//        worker->log_manager_->DeleteLogBuf(logfile);
//
//        char *response_buf = conn->buf;
//        leveldb::EncodeFixed32(response_buf, 1);
//        response_buf += 4;
//        response_buf[0] = RequestType::DELETE_LOG_FILE_SUCC;
//        conn->response_buf = conn->buf;
//        conn->response_size = 5;
//        RDMA_ASSERT(conn->response_size < NovaConfig::config->max_msg_size);
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
//        if (buf[0] == RequestType::DELETE_LOG_FILE) {
//            return process_socket_delete_log_file(fd, conn);
//        }
        RDMA_ASSERT(false) << buf[0];
        return false;
    }

    SocketState socket_read_handler(int fd, short which, Connection *conn) {
        RDMA_ASSERT((which & EV_READ) > 0) << which;
        char *buf = conn->request_buf + conn->req_ind;
        bool complete = false;
        NovaCCConnWorker *worker = (NovaCCConnWorker *) conn->worker;

        if (conn->req_ind == 0) {
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
                conn->req_ind += count;
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
                conn->req_ind += 1;
                buf += 1;
                RDMA_ASSERT(conn->req_ind < NovaConfig::config->max_msg_size);
            }
        }
        return COMPLETE;
    }

    void stats_handler(int fd, short which, void *arg) {
        NovaCCConnWorker *store = (NovaCCConnWorker *) arg;
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
        {
//            for (int i = 0;
//                 i < NovaCCConfig::cc_config->db_fragment.size(); i++) {
//                for (const auto &worker : store->db_async_workers_[i].workers) {
//                    asize += worker->size();
//                }
//            }
//            store->async_cq_->mutex.Lock();
//            csize = store->async_cq_->queue.size();
//            store->async_cq_->mutex.Unlock();
        }
        RDMA_LOG(INFO) << "memstore[" << store->thread_id_ << "]: req="
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
        NovaCCConnWorker *store = (NovaCCConnWorker *) arg;
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

    void NovaCCConnWorker::Start() {
        RDMA_LOG(INFO) << "memstore[" << thread_id_ << "]: "
                       << "starting mem worker";

        nova::NovaConfig::config->add_tid_mapping();

        struct event new_conn_timer_event;
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
        RDMA_ASSERT(event_base_loop(base, 0) == 0);
        RDMA_LOG(INFO) << "started";
    }

    void Connection::Init(int f, void *store) {
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
                event_assign(&event, ((NovaCCConnWorker *) worker)->base, fd,
                             new_flags,
                             event_handler,
                             this) ==
                0) << fd;
        RDMA_ASSERT(event_add(&event, 0) == 0) << fd;
    }
}