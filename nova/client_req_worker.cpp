
//
// Created by Haoyu Huang on 3/28/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "client_req_worker.h"

#include "logging.hpp"
#include "nova_common.h"
#include "nova_config.h"
#include "nova_client_sock.h"

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
#include <fmt/core.h>

#include <event.h>

namespace nova {
    using namespace rdmaio;

    Connection *nova_conns[NOVA_MAX_CONN];
    mutex new_conn_mutex;

    void async_complete_event_handler(int fd, short event, void *arg) {
        auto *worker = (NovaConnWorker *) arg;
        RDMA_LOG(DEBUG) << "memstore[" << worker->thread_id_
                        << "]: read async complete queue";
        char buffer[1] = {0};
        RDMA_ASSERT(read(fd, buffer, 1) == 1);
        RDMA_ASSERT(buffer[0] == 'a');

        worker->async_cq_->mutex.Lock();
        for (const NovaAsyncCompleteTask &task : worker->async_cq_->queue) {
            if (socket_write_handler(task.sock_fd, task.conn) ==
                SocketState::COMPLETE) {
                write_socket_complete(task.sock_fd, task.conn);
            }
        }
        worker->async_cq_->queue.clear();
        worker->async_cq_->mutex.Unlock();
    }


    SocketState socket_write_handler(int fd, Connection *conn) {
        RDMA_LOG(DEBUG) << "WSOCK " << conn->response_size;
        RDMA_ASSERT(conn->response_size < NovaConfig::config->max_msg_size);
        NovaConnWorker *store = (NovaConnWorker *) conn->worker;
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

        NovaConnWorker *worker = (NovaConnWorker *) conn->worker;

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
//                worker->stats.nreqs_to_poll_rdma++;
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
        NovaConnWorker *worker = (NovaConnWorker *) conn->worker;

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

    bool process_leveldb_get(NovaConnWorker *worker, Connection *conn,
                             leveldb::Slice &key) {
        uint64_t hv = NovaConfig::keyhash(key.data(),
                                          key.size());
        Fragment *frag = NovaConfig::home_fragment(hv);
        leveldb::DB *db = worker->dbs_[frag->dbid];
        std::string value;
        leveldb::Status s = db->Get(
                leveldb::ReadOptions(), key, &value);
        RDMA_ASSERT(s.ok());


        conn->response_buf = worker->buf;
        uint32_t response_size = 0;
        char *response_buf = conn->response_buf;
        uint32_t cfg_size = int_to_str(response_buf, 0);
        response_size += cfg_size;
        response_buf += cfg_size;
        uint32_t value_size = int_to_str(response_buf, value.size());
        response_size += value_size;
        response_buf += value_size;
        memcpy(response_buf, value.data(), value.size());
        response_buf[0] = MSG_TERMINATER_CHAR;
        response_size += 1;
        conn->response_size = response_size;

        RDMA_ASSERT(conn->response_size <
                    NovaConfig::config->max_msg_size);
        return true;
    }

    bool
    process_socket_get(int fd, Connection *conn, char *buf) {
        // Stats.
        NovaConnWorker *worker = (NovaConnWorker *) conn->worker;
        worker->stats.ngets++;
        uint64_t int_key = 0;
        uint32_t nkey = str_to_int(buf, &int_key) - 1;
        uint64_t hv = NovaConfig::keyhash(buf, nkey);
        char *tmp = buf;
        tmp += nkey + 1;
        Fragment *frag = NovaConfig::home_fragment(hv);
        RDMA_LOG(DEBUG) << "memstore[" << worker->thread_id_ << "]: "
                        << " Get fd:"
                        << fd << " key:" << int_key << " nkey:" << nkey
                        << " hv:"
                        << hv << " home:" << frag->server_ids[0] << " db:"
                        << frag->dbid;
        int home_server = frag->server_ids[0];
        worker->stats.nget_hits++;

        leveldb::Slice key(buf, nkey);
        return process_leveldb_get(worker, conn, key);
    }

    bool
    process_socket_scan(int fd, Connection *conn, char *buf) {
        // Stats.
        NovaConnWorker *worker = (NovaConnWorker *) conn->worker;
        worker->stats.nranges++;
        char *startkey;
        uint64_t key = 0;
        startkey = buf;
        int nkey = str_to_int(buf, &key) - 1;
        buf += nkey + 1;
        uint64_t nrecords;
        buf += str_to_int(buf, &nrecords);

        std::string skey(startkey, nkey);
        uint64_t hv = NovaConfig::keyhash(startkey, nkey);
        Fragment *frag = NovaConfig::home_fragment(hv);
        int pivot_db_id = frag->dbid;
        int read_records = 0;
        std::vector<std::string> keys;
        std::vector<std::string> values;

        conn->response_buf = worker->buf;
        char *response_buf = conn->response_buf;
        uint64_t msg_size = 0;

        uint32_t cfg_size = int_to_str(response_buf, 0);
        response_buf += cfg_size;
        msg_size += cfg_size;

        while (read_records < nrecords && pivot_db_id < worker->dbs_.size()) {
            leveldb::Iterator *iterator = worker->dbs_[pivot_db_id]->NewIterator(
                    leveldb::ReadOptions());
//            RDMA_LOG(INFO) << fmt::format("Open iterator on {}", pivot_db_id);
            iterator->Seek(startkey);
//            std::string val;
//            leveldb::Status s = worker->dbs_[pivot_db_id]->Get(
//                    leveldb::ReadOptions(), "562500", &val);
//            RDMA_LOG(INFO)
//                << fmt::format("s:{} db:{} val:{}", s.ToString(), pivot_db_id,
//                               val.size());
            while (iterator->Valid() && read_records < nrecords) {
                leveldb::Slice key = iterator->key();
                leveldb::Slice value = iterator->value();
                msg_size += nint_to_str(key.size()) + 1;
                msg_size += key.size();
                msg_size += nint_to_str(value.size()) + 1;
                msg_size += value.size();
//                RDMA_LOG(INFO) << fmt::format("Scan key {} value:{}:{}",
//                                              key.ToString(),
//                                              value.size(), value.data()[0]);
                response_buf += int_to_str(response_buf, key.size());
                memcpy(response_buf, key.data(), key.size());
                response_buf += key.size();
                response_buf += int_to_str(response_buf, value.size());
                memcpy(response_buf, value.data(), value.size());
                response_buf += value.size();
                read_records++;
                iterator->Next();
            }
            delete iterator;
            pivot_db_id += 1;
        }
        response_buf[0] = MSG_TERMINATER_CHAR;
        msg_size += 1;
        conn->response_size = msg_size;
//        RDMA_LOG(INFO)
//            << fmt::format("Scan fd: {} key:{} nkey:{} cardinality:{} size:{}",
//                           fd, skey, nkey, nrecords, msg_size);
        RDMA_ASSERT(
                conn->response_size < NovaConfig::config->max_msg_size)
            << conn->response_size;
        return true;
    }

    bool process_socket_put(int fd, Connection *conn, char *buf) {
        // Stats.
        NovaConnWorker *worker = (NovaConnWorker *) conn->worker;
        worker->stats.nputs++;
        char *ckey;
        uint64_t key = 0;
        ckey = buf;
        int nkey = str_to_int(buf, &key) - 1;
        buf += nkey + 1;
        uint64_t nval;
        buf += str_to_int(buf, &nval);
        char *val = buf;
        uint64_t hv = NovaConfig::keyhash(ckey, nkey);
        Fragment *frag = NovaConfig::home_fragment(hv);
        RDMA_LOG(DEBUG) << "memstore[" << worker->thread_id_ << "]: "
                       << " put fd:"
                       << fd << ": key:" << key << " nkey:" << nkey << " nval:"
                       << nval;

        leveldb::Slice dbkey(ckey, nkey);
        leveldb::Slice dbval(val, nval);

//        bool local_write =
//                NovaConfig::config->log_record_mode == leveldb::LOG_NONE ||
//                NovaConfig::config->log_record_mode == leveldb::LOG_DISK_SYNC ||
//                NovaConfig::config->log_record_mode == leveldb::LOG_DISK_ASYNC;

        leveldb::WriteOptions option;
        option.log_record_mode = NovaConfig::config->log_record_mode;
//        Fragment *frag = NovaConfig::home_fragment(hv);
        leveldb::DB *db = worker->dbs_[frag->dbid];
        leveldb::Status status = db->Put(option, dbkey, dbval);
        RDMA_LOG(DEBUG) << "############### Async worker processed task "
                        << fd
                        << ":" << dbkey.ToString();
        RDMA_ASSERT(status.ok()) << status.ToString();

        char *response_buf = worker->buf;
        uint32_t response_size = 0;
        uint32_t cfg_size = int_to_str(response_buf, 0);
        response_buf += cfg_size;
        response_size += cfg_size;
        response_buf[0] = MSG_TERMINATER_CHAR;
        response_size += 1;
        conn->response_buf = worker->buf;
        conn->response_size = response_size;
        return true;

        // I'm the home.
//        NovaAsyncTask task = {
//                .type = RequestType::PUT,
//                .conn_worker_id = worker->thread_id_,
//                .key = dbkey.ToString(),
//                .value = dbval.ToString(),
//                .sock_fd = fd,
//                .conn = conn
//        };
//        worker->AddTask(task);
    }

    bool process_socket_replicate_log_record(int fd, Connection *conn) {
        // Stats.
        NovaConnWorker *worker = (NovaConnWorker *) conn->worker;
        worker->stats.nreplicate_log_records++;
        char *buf = worker->request_buf;
        RDMA_ASSERT(buf[0] == RequestType::REPLICATE_LOG_RECORD) << buf;
        buf++;
        uint32_t logfilename_size = leveldb::DecodeFixed32(buf);
        std::string logfile(buf + 4, logfilename_size);
        buf += 4;
        buf += logfilename_size;
        uint32_t logrecord_size = leveldb::DecodeFixed32(buf);
        buf += 4;

        RDMA_LOG(DEBUG) << "memstore[" << worker->thread_id_ << "]: "
                        << " replicate log record fd:"
                        << fd << ": log:" << logfile << " nlog:"
                        << logfilename_size << " nlogrecord:"
                        << logrecord_size << " buf:" << worker->request_buf;

        worker->async_workers_[worker->current_async_worker_id_]->
                nic_log_writer_->AddLocalRecord(logfile,
                                                leveldb::Slice(buf,
                                                               logrecord_size));
        char *response_buf = worker->buf;
        leveldb::EncodeFixed32(response_buf, 1);
        response_buf += 4;
        response_buf[0] = RequestType::REPLICATE_LOG_RECORD_SUCC;
        conn->response_buf = worker->buf;
        conn->response_size = 5;
        RDMA_ASSERT(conn->response_size < NovaConfig::config->max_msg_size);
        return true;
    }

    bool process_socket_delete_log_file(int fd, Connection *conn) {
        // Stats.
        NovaConnWorker *worker = (NovaConnWorker *) conn->worker;
        worker->stats.nremove_log_records++;
        char *buf = worker->request_buf;
        RDMA_ASSERT(buf[0] == RequestType::DELETE_LOG_FILE) << buf;
        buf++;
        uint32_t logfilename_size = leveldb::DecodeFixed32(buf);
        std::string logfile(buf + 4, logfilename_size);

        RDMA_LOG(DEBUG) << "memstore[" << worker->thread_id_ << "]: "
                        << " delete log file fd:"
                        << fd << ": log:" << logfile << " nlog:"
                        << logfilename_size << " buf:" << worker->request_buf;
        worker->log_manager_->DeleteLogBuf(logfile);

        char *response_buf = worker->buf;
        leveldb::EncodeFixed32(response_buf, 1);
        response_buf += 4;
        response_buf[0] = RequestType::DELETE_LOG_FILE_SUCC;
        conn->response_buf = worker->buf;
        conn->response_size = 5;
        RDMA_ASSERT(conn->response_size < NovaConfig::config->max_msg_size);
        return true;
    }

    bool
    process_socket_drain_l0_sstable_request(int fd, Connection *conn) {
        RDMA_LOG(rdmaio::INFO) << "Drain SSTables";
        NovaConnWorker *worker = (NovaConnWorker *) conn->worker;
        NovaAsyncTask task;
        task.type = RequestType::DRAIN;
        task.dbs = worker->dbs_;
        worker->AddTask(task);

        char *response_buf = worker->buf;
        int nlen = 0;
        int len = int_to_str(response_buf, 0);
        conn->response_buf = worker->buf;
        conn->response_size = len;
        return true;
    }

    bool
    process_socket_stats_request(int fd, Connection *conn) {
        RDMA_LOG(rdmaio::INFO) << "Obtain stats";
        NovaConnWorker *worker = (NovaConnWorker *) conn->worker;
        int num_l0_sstables = 0;
        bool needs_compaction = false;
        for (auto db : worker->dbs_) {
            leveldb::DBStats stats;
            db->QueryDBStats(&stats);
            if (!needs_compaction) {
                needs_compaction = stats.needs_compaction;
            }
            num_l0_sstables += stats.num_l0_sstables;
        }

        if (num_l0_sstables == 0 && needs_compaction) {
            num_l0_sstables = 10000;
        }

        char *response_buf = worker->buf;
        int nlen = 0;
        int len = int_to_str(response_buf, num_l0_sstables);
        conn->response_buf = worker->buf;
        conn->response_size = len;
        return true;
    }

    bool process_socket_request_handler(int fd, Connection *conn) {
        auto worker = (NovaConnWorker *) conn->worker;
        char *buf = worker->request_buf;
        char *request_buf = worker->request_buf;
        char msg_type = request_buf[0];
        request_buf++;
        if (msg_type == RequestType::GET || msg_type == RequestType::REQ_SCAN ||
            msg_type == RequestType::PUT) {
            uint64_t client_cfg_id = 0;
            request_buf += str_to_int(request_buf, &client_cfg_id);
        }

        if (buf[0] == RequestType::GET) {
            return process_socket_get(fd, conn, request_buf);
        }
        if (buf[0] == RequestType::REQ_SCAN) {
            return process_socket_scan(fd, conn, request_buf);
        }
        if (buf[0] == RequestType::PUT) {
            return process_socket_put(fd, conn, request_buf);
        }
        if (buf[0] == RequestType::REPLICATE_LOG_RECORD) {
            return process_socket_replicate_log_record(fd, conn);
        }
        if (buf[0] == RequestType::DELETE_LOG_FILE) {
            return process_socket_delete_log_file(fd, conn);
        }
        if (buf[0] == RequestType::STATS) {
            return process_socket_stats_request(fd, conn);
        }
        if (buf[0] == RequestType::DRAIN) {
            buf[0] = '~';
            return process_socket_drain_l0_sstable_request(fd, conn);
        }
        RDMA_ASSERT(false) << buf[0];
        return false;
    }

    SocketState socket_read_handler(int fd, short which, Connection *conn) {
        RDMA_ASSERT((which & EV_READ) > 0) << which;
        auto worker = (NovaConnWorker *) conn->worker;
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
                RDMA_ASSERT(worker->req_ind < NovaConfig::config->max_msg_size);
            }
        }
        return COMPLETE;
    }

    void stats_handler(int fd, short which, void *arg) {
        NovaConnWorker *store = (NovaConnWorker *) arg;
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
            for (const auto &worker : store->async_workers_) {
                asize += worker->size();
            }
            store->async_cq_->mutex.Lock();
            csize = store->async_cq_->queue.size();
            store->async_cq_->mutex.Unlock();
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
    }

    void new_conn_handler(int fd, short which, void *arg) {
        NovaConnWorker *store = (NovaConnWorker *) arg;
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

    void NovaConnWorker::AddTask(const nova::NovaAsyncTask &task) {
//        uint64_t hv = NovaConfig::keyhash(task.key.data(),
//                                          task.key.size());
//        Fragment *frag = NovaConfig::home_fragment(hv);
//        uint32_t dbid = frag->dbid;
        async_workers_[0]->AddTask(task);
    }

    void NovaConnWorker::Start() {
        RDMA_LOG(DEBUG) << "memstore[" << thread_id_ << "]: "
                        << "starting mem worker";

        if (NovaConfig::config->log_record_mode == leveldb::LOG_NIC) {
            bool all_initialized = false;
            while (!all_initialized) {
                all_initialized = true;
                for (const auto &worker : async_workers_) {
                    if (!worker->IsInitialized()) {
                        all_initialized = false;
                        break;
                    }
                }
                usleep(10000);
            }
        }

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
        RDMA_LOG(DEBUG) << "Using Libevent with backend method "
                        << event_base_get_method(base);
        const int f = event_base_get_features(base);
        if ((f & EV_FEATURE_ET)) {
            RDMA_LOG(DEBUG) << "Edge-triggered events are supported.";
        }

        if ((f & EV_FEATURE_O1)) {
            RDMA_LOG(DEBUG) <<
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
        {
            memset(&async_cq_->readevent, 0, sizeof(struct event));
            RDMA_ASSERT(event_assign(&async_cq_->readevent, base,
                                     async_cq_->read_fd,
                                     EV_READ | EV_PERSIST,
                                     async_complete_event_handler,
                                     this) ==
                        0)
                << async_cq_->read_fd;
            RDMA_ASSERT(event_add(&async_cq_->readevent, 0) == 0)
                << async_cq_->read_fd;
        }
        /* Timer event for stats */
        {
//            struct timeval tv;
//            tv.tv_sec = 10;
//            tv.tv_usec = 0;
//            memset(&stats_event, 0, sizeof(struct event));
//            RDMA_ASSERT(
//                    event_assign(&stats_event, base, -1, EV_PERSIST,
//                                 stats_handler,
//                                 (void *) this) == 0);
//            RDMA_ASSERT(event_add(&stats_event, &tv) == 0);
        }
        RDMA_LOG(DEBUG) << "NIC worker started";
        RDMA_ASSERT(event_base_loop(base, 0) == 0);

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
        RDMA_ASSERT(event_del(&event) == 0) << fd;
        RDMA_ASSERT(
                event_assign(&event, ((NovaConnWorker *) worker)->base, fd,
                             new_flags,
                             event_handler,
                             this) ==
                0) << fd;
        RDMA_ASSERT(event_add(&event, 0) == 0) << fd;
    }
}