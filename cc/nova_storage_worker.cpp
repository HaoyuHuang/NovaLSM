
//
// Created by Haoyu Huang on 5/9/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_storage_worker.h"

#include <fmt/core.h>
#include <semaphore.h>
#include "db/compaction.h"
#include "db/table_cache.h"

#include "db/filename.h"
#include "nova_cc_server.h"
#include "cc/nova_cc_client.h"
#include "nova/nova_config.h"
#include "db/version_set.h"

namespace nova {
    NovaStorageWorker::NovaStorageWorker(
            leveldb::NovaRTableManager *rtable_manager,
            std::vector<NovaCCServer *> &cc_servers,
            const leveldb::Comparator *user_comparator,
            const leveldb::Options &options,
            leveldb::CCClient *client,
            leveldb::MemManager *mem_manager,
            uint64_t thread_id, leveldb::Env *env)
            : rtable_manager_(
            rtable_manager), cc_servers_(cc_servers), user_comparator_(
            user_comparator), options_(options), icmp_(user_comparator),
              client_(client),
              mem_manager_(mem_manager), thread_id_(thread_id), env_(env) {
        stat_tasks_ = 0;
        stat_read_bytes_ = 0;
        stat_write_bytes_ = 0;
        sem_init(&sem_, 0, 0);
    }

    void NovaStorageWorker::AddTask(
            const nova::NovaStorageTask &task) {
        mutex_.lock();
        stat_tasks_ += 1;
        queue_.push_back(task);
        mutex_.unlock();

        sem_post(&sem_);
    }

    void NovaStorageWorker::Start() {
        RDMA_LOG(DEBUG) << "CC server worker started";

        nova::NovaConfig::config->add_tid_mapping();

        while (is_running_) {
            sem_wait(&sem_);

            std::vector<NovaStorageTask> tasks;
            mutex_.lock();

            while (!queue_.empty()) {
                auto task = queue_.front();
                tasks.push_back(task);
                queue_.pop_front();
            }
            mutex_.unlock();

            if (tasks.empty()) {
                continue;
            }

            std::map<uint32_t, std::vector<NovaServerCompleteTask>> t_tasks;
            for (auto &task : tasks) {
                stat_tasks_ += 1;
                NovaServerCompleteTask ct = {};
                ct.remote_server_id = task.remote_server_id;
                ct.dc_req_id = task.dc_req_id;
                ct.request_type = task.request_type;

                ct.rdma_buf = task.rdma_buf;
                ct.cc_mr_offset = task.cc_mr_offset;
                ct.rtable_handle = task.rtable_handle;

                if (task.request_type ==
                    leveldb::CCRequestType::CC_RTABLE_READ_BLOCKS) {
                    leveldb::Slice result;
                    rtable_manager_->ReadDataBlock(task.rtable_handle,
                                                   task.rtable_handle.offset,
                                                   task.rtable_handle.size,
                                                   task.rdma_buf, &result);
                    task.rtable_handle.size = result.size();
                    RDMA_ASSERT(result.size() <= task.rtable_handle.size);
                    stat_read_bytes_ += task.rtable_handle.size;
                } else if (task.request_type ==
                           leveldb::CCRequestType::CC_RTABLE_WRITE_SSTABLE) {
                    RDMA_ASSERT(task.persist_pairs.size() == 1);
                    leveldb::FileType type = leveldb::FileType::kCurrentFile;
                    for (auto &pair : task.persist_pairs) {
                        leveldb::NovaRTable *rtable = rtable_manager_->FindRTable(
                                pair.rtable_id);
                        uint64_t persisted_bytes = rtable->Persist(
                                pair.rtable_id);
                        stat_write_bytes_ += persisted_bytes;
                        RDMA_LOG(DEBUG) << fmt::format(
                                    "Persisting rtable {} for sstable {}",
                                    pair.rtable_id, pair.sstable_id);

                        leveldb::BlockHandle h = rtable->Handle(
                                pair.sstable_id, task.is_meta_blocks);
                        leveldb::RTableHandle rh = {};
                        rh.server_id = NovaConfig::config->my_server_id;
                        rh.rtable_id = pair.rtable_id;
                        rh.offset = h.offset();
                        rh.size = h.size();
                        ct.rtable_handles.push_back(rh);

                        RDMA_ASSERT(leveldb::ParseFileName(pair.sstable_id,
                                                           &type));
                        if (task.is_meta_blocks ||
                            type == leveldb::FileType::kTableFile) {
                            rtable->ForceSeal();
                        }
                    }
                } else if (task.request_type ==
                           leveldb::CCRequestType::CC_COMPACTION) {
                    leveldb::TableCache table_cache(
                            task.compaction_request->dbname, options_, 0,
                            nullptr);
                    leveldb::VersionFileMap version_files(&table_cache);
                    leveldb::Compaction *compaction = new leveldb::Compaction(
                            &version_files, &icmp_,
                            &options_,
                            task.compaction_request->source_level,
                            task.compaction_request->target_level);
                    compaction->grandparents_ = task.compaction_request->guides;
                    for (int which = 0; which < 2; which++) {
                        compaction->inputs_[which] = task.compaction_request->inputs[which];
                        for (auto meta : compaction->inputs_[which]) {
                            version_files.fn_files_[meta->number] = meta;
                        }
                    }
                    for (auto meta : compaction->grandparents_) {
                        version_files.fn_files_[meta->number] = meta;
                    }

                    // This will delete the subranges.
                    leveldb::SubRanges srs;
                    srs.subranges = task.compaction_request->subranges;
                    compaction->input_version_ = &version_files;

                    leveldb::CompactionState *state = new leveldb::CompactionState(
                            compaction, &srs);
                    state->smallest_snapshot = task.compaction_request->smallest_snapshot;
                    std::function<uint64_t(void)> fn_generator = []() {
                        uint32_t fn = storage_file_number_seq.fetch_add(1);
                        uint64_t stocid = nova::NovaConfig::config->my_server_id;
                        return (stocid << 32) | fn;
                    };
                    {
                        std::vector<leveldb::FileMetaData *> files;
                        for (int which = 0; which < 2; which++) {
                            for (int i = 0;
                                 i < compaction->num_input_files(which); i++) {
                                files.push_back(compaction->input(which, i));
                            }
                        }
                        FetchMetadataFilesInParallel(files,
                                                     task.compaction_request->dbname,
                                                     options_,
                                                     reinterpret_cast<leveldb::NovaBlockCCClient *>(client_),
                                                     env_);
                    }
                    leveldb::CompactionJob job(fn_generator, env_,
                                               task.compaction_request->dbname,
                                               user_comparator_,
                                               options_, this);
                    RDMA_LOG(rdmaio::DEBUG)
                        << fmt::format("storage[{}]: {}", thread_id_,
                                       compaction->DebugString(
                                               user_comparator_));
                    auto it = compaction->MakeInputIterator(&table_cache, this);
                    leveldb::CompactionStats stats = state->BuildStats();
                    job.CompactTables(state, it, &stats, true,
                                      leveldb::CompactType::kCompactSSTables);
                    ct.compaction_state = state;
                    ct.compaction_request = task.compaction_request;
//                    {
//                        leveldb::ReadOptions options;
//                        options.mem_manager = options_.mem_manager;
//                        options.dc_client = client_;
//                        options.thread_id = 0;
//                        uint32_t scid = options.mem_manager->slabclassid(0,
//                                                                         8192);
//                        options.rdma_backing_mem = options.mem_manager->ItemAlloc(
//                                0, scid);
//                        options.rdma_backing_mem_size = 8192;
//                        auto file = state->outputs[0];
//                        auto s = file.smallest.user_key();
//                        auto l = file.largest.user_key();
//                        uint64_t start = 0;
//                        uint64_t end = 0;
//                        nova::str_to_int(s.data(), &start, s.size());
//                        nova::str_to_int(l.data(), &end, l.size());
//
//                        RDMA_LOG(rdmaio::INFO)
//                            << fmt::format("Is the file valid? {} ",
//                                           file.DebugString());
//                        for (int i = start; i <= end; i++) {
//                            leveldb::LookupKey k(std::to_string(i),
//                                                 leveldb::kMaxSequenceNumber);
//                            table_cache.Get(options, &file, file.number,
//                                            file.converted_file_size, 0,
//                                            k.internal_key(),
//                                            nullptr, nullptr);
//                        }
//                    }
                } else {
                    RDMA_ASSERT(false);
                }
                RDMA_LOG(DEBUG)
                    << fmt::format(
                            "CCWorker: Working on t:{} ss:{} req:{} type:{}",
                            task.cc_server_thread_id, ct.remote_server_id,
                            ct.dc_req_id,
                            ct.request_type);
                t_tasks[task.cc_server_thread_id].push_back(ct);
            }

            for (auto &it : t_tasks) {
                cc_servers_[it.first]->AddCompleteTasks(it.second);
            }
        }
    }
}