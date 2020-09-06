
//
// Created by Haoyu Huang on 12/25/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova_async_worker.h"

#include <fmt/core.h>
#include <unistd.h>

#include "logging.hpp"
#include "logging/logging.h"
#include "nova_common.h"
#include "nova_config.h"

namespace nova {
void NovaAsyncWorker::AddTask(const NovaAsyncTask &task) {
  mutex_.Lock();
  queue_.push_back(task);
  mutex_.Unlock();
  sem_post(&sem_);
}

int NovaAsyncWorker::size() {
  mutex_.Lock();
  int size = queue_.size();
  mutex_.Unlock();
  return size;
}

void NovaAsyncWorker::Drain(const NovaAsyncTask &task) {
  RDMA_LOG(INFO) << "Drain memtables and SSTables at Level 0";
  for (uint32_t i = 0; i < dbs_.size(); i++) {
    if (!dbs_[i]) {
      continue;
    }
    RDMA_LOG(rdmaio::INFO) << "Database " << i;
    std::string value;
    dbs_[i]->GetProperty("rocksdb.levelstats", &value);
    RDMA_LOG(rdmaio::INFO) << "\n" << value;
    value.clear();
    dbs_[i]->GetProperty("leveldb.stats", &value);
    RDMA_LOG(rdmaio::INFO) << "\n"
                           << "rocksdb stats " << value;
  }

  std::vector<rocksdb::Options> options;
  for (const auto &db : task.dbs) {
    if (!db) {
      continue;
    }
    options.push_back(db->GetOptions());
    std::unordered_map<std::string, std::string> new_options;
    new_options["level0_file_num_compaction_trigger"] = "0";
    db->SetOptions(new_options);

    ROCKS_LOG_INFO(db->GetOptions().info_log, "%s", "Update options");
    db->GetOptions().Dump(db->GetOptions().info_log.get());

    rocksdb::FlushOptions fo;
    fo.wait = true;
    fo.allow_write_stall = false;
    rocksdb::Status s = db->Flush(fo);
    RDMA_ASSERT(s.ok()) << s.ToString();
  }

  while (true) {
    bool stop = true;
    for (auto &db : task.dbs) {
      if (!db) {
        continue;
      }
      rocksdb::ColumnFamilyMetaData metadata;
      db->GetColumnFamilyMetaData(&metadata);
      uint64_t bytes = metadata.levels[0].size;
      std::string value;
      bool need_compaction = false;
      RDMA_ASSERT(db->GetProperty("rocksdb.compaction-pending", &value));
      if (std::stoi(value) == 1) {
        need_compaction = true;
      }
      if (bytes > 0 || need_compaction) {
        RDMA_LOG(INFO) << fmt::format("Waiting for {} bytes at L0. {}", bytes,
                                      need_compaction);
        stop = false;
      }
    }
    if (stop) {
      break;
    }
    sleep(1);
  }
  for (uint32_t i = 0; i < task.dbs.size(); i++) {
    if (!task.dbs[i]) {
      continue;
    }
    std::unordered_map<std::string, std::string> new_options;
    new_options["level0_file_num_compaction_trigger"] =
        std::to_string(options[i].level0_file_num_compaction_trigger);
    task.dbs[i]->SetOptions(new_options);
    RDMA_LOG(rdmaio::INFO) << "Database " << i;
    ROCKS_LOG_INFO(dbs_[i]->GetOptions().info_log, "%s",
                   "Revert to original options");
    dbs_[i]->GetOptions().Dump(dbs_[i]->GetOptions().info_log.get());
    std::string value;
    dbs_[i]->GetProperty("rocksdb.levelstats", &value);
    RDMA_LOG(rdmaio::INFO) << "\n" << value;
    value.clear();
    dbs_[i]->GetProperty("rocksdb.stats", &value);
    RDMA_LOG(rdmaio::INFO) << "\n"
                           << "rocksdb stats " << value;
  }
}

int NovaAsyncWorker::ProcessQueue() {
  mutex_.Lock();
  if (queue_.empty()) {
    mutex_.Unlock();
    return 0;
  }
  std::list<NovaAsyncTask> queue(queue_.begin(), queue_.end());
  mutex_.Unlock();

  for (const NovaAsyncTask &task : queue) {
    if (task.type == RequestType::DRAIN) {
      Drain(task);
    }
  }

  mutex_.Lock();
  auto begin = queue_.begin();
  auto end = queue_.begin();
  std::advance(end, queue.size());
  queue_.erase(begin, end);
  mutex_.Unlock();
  return queue.size();
}

bool NovaAsyncWorker::IsInitialized() {
  mutex_.Lock();
  bool t = is_running_;
  mutex_.Unlock();
  return t;
}

void NovaAsyncWorker::Start() {
  RDMA_LOG(rdmaio::INFO) << "Async worker started";
  mutex_.Lock();
  is_running_ = true;
  mutex_.Unlock();

  while (is_running_) {
    sem_wait(&sem_);
    ProcessQueue();
  }
}
}  // namespace nova