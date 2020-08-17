
//
// Created by Haoyu Huang on 12/25/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_ASYNC_WORKER_H
#define LEVELDB_NOVA_ASYNC_WORKER_H

#include <semaphore.h>

#include <list>
#include <string>

#include "nova_common.h"
#include "nova_config.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace nova {

#define RDMA_POLL_MIN_TIMEOUT_US 10
#define RDMA_POLL_MAX_TIMEOUT_US 100

struct NovaAsyncTask {
  RequestType type;
  int conn_worker_id;
  std::string key;
  std::string value;
  int sock_fd;
  Connection *conn;
  std::vector<rocksdb::DB *> dbs;
};

struct NovaAsyncCompleteTask {
  int sock_fd;
  Connection *conn;
};

struct NovaAsyncCompleteQueue {
  std::list<NovaAsyncCompleteTask> queue;
  rocksdb::port::Mutex mutex;
  int read_fd;
  int write_fd;
  struct event readevent;
};

class NovaAsyncWorker {
 public:
  NovaAsyncWorker(const std::vector<rocksdb::DB *> &dbs,
                  NovaAsyncCompleteQueue **cqs)
      : dbs_(dbs), cqs_(cqs) {
    sem_init(&sem_, 0, 0);
    conn_workers_ = new bool[NovaConfig::config->num_conn_workers];
  }

  bool IsInitialized();

  void Start();

  void AddTask(const NovaAsyncTask &task);

  int size();

 private:
  void Drain(const NovaAsyncTask &task);

  int ProcessQueue();

  bool is_running_ = false;

  sem_t sem_;
  std::vector<rocksdb::DB *> dbs_;
  rocksdb::port::Mutex mutex_;
  std::list<NovaAsyncTask> queue_;
  NovaAsyncCompleteQueue **cqs_;
  bool *conn_workers_;
};
}  // namespace nova

#endif  // LEVELDB_NOVA_ASYNC_WORKER_H
