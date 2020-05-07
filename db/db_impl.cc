// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <atomic>
#include <set>
#include <string>
#include <vector>
#include <list>
#include <cc/nova_cc.h>
#include <fmt/core.h>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "leveldb/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "subrange_manager.h"
#include "compaction.h"

namespace leveldb {
    namespace {
        uint64_t time_diff(timeval t1, timeval t2) {
            return (t2.tv_sec - t1.tv_sec) * 1000000 +
                   (t2.tv_usec - t1.tv_usec);
        }
    }

    uint64_t TableLocator::Lookup(const leveldb::Slice &key, uint64_t hash) {
        RDMA_ASSERT(hash >= 0 && hash <= MAX_BUCKETS);
        TableLocation &loc = table_locator_[hash % MAX_BUCKETS];
//        uint64_t memtable_id = 0;
//        loc.mutex.lock();
//        memtable_id = loc.memtable_id;
//        loc.mutex.unlock();
//        return memtable_id;
        return loc.memtable_id.load();
    }

    void TableLocator::Insert(const leveldb::Slice &key, uint64_t hash,
                              uint32_t memtableid) {
        RDMA_ASSERT(hash >= 0 && hash <= MAX_BUCKETS) << hash;
        TableLocation &loc = table_locator_[hash % MAX_BUCKETS];
//        loc.mutex.lock();
//        loc.memtable_id = memtableid;
//        loc.mutex.unlock();
        loc.memtable_id.store(memtableid);
    }


    const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
    struct DBImpl::Writer {
        explicit Writer(port::Mutex *mu)
                : batch(nullptr), sync(false), done(false), cv(mu) {}

        Status status;
        WriteBatch *batch;
        bool sync;
        bool done;
        port::CondVar cv;
    };

// Fix user-supplied options to be reasonable
    template<class T, class V>
    static void ClipToRange(T *ptr, V minvalue, V maxvalue) {
        if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
        if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
    }

    Options SanitizeOptions(const std::string &dbname,
                            const InternalKeyComparator *icmp,
                            const InternalFilterPolicy *ipolicy,
                            const Options &src) {
        Options result = src;
        result.comparator = icmp;
        result.filter_policy = (src.filter_policy != nullptr) ? ipolicy
                                                              : nullptr;
        ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
        ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
        ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
        ClipToRange(&result.block_size, 1 << 10, 4 << 20);
        if (result.info_log == nullptr) {
            // Open a log file in the same directory as the db
            src.env->CreateDir(dbname);  // In case it does not exist
            src.env->RenameFile(InfoLogFileName(dbname),
                                OldInfoLogFileName(dbname));
            Status s = src.env->NewLogger(InfoLogFileName(dbname),
                                          &result.info_log);
            if (!s.ok()) {
                // No place suitable for logging
                result.info_log = nullptr;
            }
        }
        return result;
    }

    static int TableCacheSize(const Options &sanitized_options) {
        // Reserve ten files or so for other uses and give the rest to TableCache.
        return sanitized_options.max_open_files - kNumNonTableCacheFiles;
    }

    DBImpl::DBImpl(const Options &raw_options, const std::string &dbname)
            : env_(raw_options.env),
              internal_comparator_(raw_options.comparator),
              internal_filter_policy_(raw_options.filter_policy),
              options_(SanitizeOptions(dbname, &internal_comparator_,
                                       &internal_filter_policy_, raw_options)),
              owns_info_log_(options_.info_log != raw_options.info_log),
              owns_cache_(options_.block_cache != raw_options.block_cache),
              dbname_(dbname),
              db_profiler_(new DBProfiler(raw_options.enable_tracing,
                                          raw_options.trace_file_path)),
              table_cache_(new TableCache(dbname_, options_,
                                          TableCacheSize(options_),
                                          db_profiler_)),
              db_lock_(nullptr),
              shutting_down_(false),
              seed_(0),
              manual_compaction_(nullptr),
              versions_(new VersionSet(dbname_, &options_, table_cache_,
                                       &internal_comparator_)),
              compaction_threads_(raw_options.bg_threads),
              reorg_thread_(raw_options.reorg_thread),
              compaction_coordinator_thread_(
                      raw_options.compaction_coordinator_thread),
              memtable_available_signal_(&range_lock_),
              user_comparator_(raw_options.comparator) {
        memtable_id_seq_.store(100);
        if (options_.enable_table_locator) {
            table_locator_ = new TableLocator;
            for (int i = 0; i < MAX_BUCKETS; i++) {
                table_locator_->Insert(Slice(), i, 0);
            }
        }
        uint32_t sid;
        nova::ParseDBIndexFromDBName(dbname_, &sid, &dbid_);
    }

    DBImpl::~DBImpl() {
        // Wait for background work to finish.
        if (db_lock_ != nullptr) {
            env_->UnlockFile(db_lock_);
        }

        delete versions_;
        delete table_cache_;

        if (owns_info_log_) {
            delete options_.info_log;
        }
        if (owns_cache_) {
            delete options_.block_cache;
        }
    }

    void DBImpl::EvictFileFromCache(uint64_t file_number) {
        table_cache_->Evict(file_number);
    }

    void DBImpl::DeleteObsoleteVersions(leveldb::EnvBGThread *bg_thread) {
        mutex_.AssertHeld();
        versions_->DeleteObsoleteVersions();
    }

    void DBImpl::CleanUpTableLocator(
            std::map<uint32_t, std::vector<uint64_t>> &memtableid_l0fns) {
        for (auto &it : memtableid_l0fns) {
            versions_->mid_table_mapping_[it.first].DeleteL0File(it.second);
        }
    }

    void DBImpl::DeleteObsoleteFiles(EnvBGThread *bg_thread,
                                     std::map<uint32_t, std::vector<uint64_t>> *memtableid_l0fns) {
        mutex_.AssertHeld();

        if (!bg_error_.ok()) {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return;
        }

        // Make a set of all of the live files
        std::set<uint64_t> live;
        versions_->AddLiveFiles(&live);
        std::vector<std::string> files_to_delete;
        std::map<uint32_t, std::vector<SSTableRTablePair>> server_pairs;
        auto it = compacted_tables_.begin();
        while (it != compacted_tables_.end()) {
            uint64_t fn = it->first;
            FileMetaData &meta = it->second;

            if (live.find(fn) != live.end()) {
                // Do not remove if it is still alive.
                it++;
                continue;
            }
            table_cache_->Evict(meta.number);

            if (meta.level == 0 && memtableid_l0fns) {
                auto l0m = l0fn_memtableids.find(meta.number);
                if (l0m != l0fn_memtableids.end()) {
                    for (int i = 0; i < l0m->second.size(); i++) {
                        (*memtableid_l0fns)[l0m->second[i]].push_back(
                                l0m->first);
                    }
                    l0fn_memtableids.erase(meta.number);
                }
            }

            // Delete data files.
            auto handles = meta.data_block_group_handles;
            for (int i = 0; i < handles.size(); i++) {
                SSTableRTablePair pair = {};
                pair.sstable_id = TableFileName(dbname_, meta.number, false);
                pair.rtable_id = handles[i].rtable_id;
                server_pairs[handles[i].server_id].push_back(pair);
            }
            files_to_delete.push_back(
                    TableFileName(dbname_, meta.number, false));
            // Delete metadata file.
            {
                auto &it = server_pairs[meta.meta_block_handle.server_id];
                bool found = false;
                for (auto &rtable : it) {
                    if (rtable.rtable_id == meta.meta_block_handle.rtable_id ||
                        meta.meta_block_handle.rtable_id == 0) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    SSTableRTablePair pair = {};
                    pair.sstable_id = TableFileName(dbname_, meta.number, true);
                    pair.rtable_id = meta.meta_block_handle.rtable_id;
                    it.push_back(pair);
                }
            }
            it = compacted_tables_.erase(it);
        }
        // While deleting all files unblock other threads. All files being deleted
        // have unique names which will not collide with newly created files and
        // are therefore safe to delete while allowing other threads to proceed.
        mutex_.Unlock();
        for (const std::string &filename : files_to_delete) {
            env_->DeleteFile(dbname_ + "/" + filename);
        }
        for (auto &it : server_pairs) {
            bg_thread->dc_client()->InitiateDeleteTables(it.first,
                                                         it.second);
            for (auto &tble : it.second) {
                RDMA_LOG(rdmaio::INFO)
                    << fmt::format("Delete {} {}", tble.rtable_id,
                                   tble.sstable_id);
            }
        }
        mutex_.Lock();
    }

    DBImpl::NovaCCRecoveryThread::NovaCCRecoveryThread(
            uint32_t client_id,
            std::vector<leveldb::MemTable *> memtables,
            MemManager *mem_manager)
            : client_id_(client_id), memtables_(memtables),
              mem_manager_(mem_manager) {
        sem_init(&sem_, 0, 0);
    }

    void DBImpl::NovaCCRecoveryThread::Recover() {
        sem_wait(&sem_);
        if (log_replicas_.empty()) {
            return;
        }
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("t{}: started memtables:{}", client_id_,
                           log_replicas_.size());
        timeval start{};
        gettimeofday(&start, nullptr);

        timeval new_memtable_end{};
        gettimeofday(&new_memtable_end, nullptr);
        new_memtable_time = time_diff(start, new_memtable_end);

        for (int i = 0; i < log_replicas_.size(); i++) {
            char *buf = log_replicas_[i];
            leveldb::MemTable *memtable = memtables_[i];

            leveldb::LevelDBLogRecord record;
            uint32_t record_size = nova::DecodeLogRecord(buf, &record);
            while (record_size != 0) {
                memtable->Add(record.sequence_number,
                              leveldb::ValueType::kTypeValue, record.key,
                              record.value);
                recovered_log_records += 1;
                buf += record_size;
                record_size = nova::DecodeLogRecord(buf, &record);
                max_sequence_number = std::max(max_sequence_number,
                                               record.sequence_number);
            }
        }

        RDMA_ASSERT(memtables_.size() == log_replicas_.size());

        timeval end{};
        gettimeofday(&end, nullptr);
        recovery_time = time_diff(start, end);
    }

    Status DBImpl::Recover() {
        timeval start = {};
        gettimeofday(&start, nullptr);

        uint32_t stoc_id = options_.manifest_stoc_id;
        std::string manifest = DescriptorFileName(dbname_, 0);
        auto client = reinterpret_cast<NovaBlockCCClient *> (options_.dc_client);
        uint32_t scid = options_.mem_manager->slabclassid(0,
                                                          options_.max_file_size);
        char *buf = options_.mem_manager->ItemAlloc(0, scid);
        RTableHandle handle = {};
        handle.server_id = stoc_id;
        handle.rtable_id = 0;
        handle.offset = 0;
        handle.size = options_.max_file_size;

        RDMA_LOG(rdmaio::INFO) << fmt::format(
                    "Recover the latest verion from manifest file {} at StoC-{}",
                    manifest, stoc_id);

        client->InitiateRTableReadDataBlock(handle, 0, options_.max_file_size,
                                            buf,
                                            manifest);
        client->Wait();

        std::map<std::string, uint64_t> logfile_buf;
        nova::CCFragment *frag = nova::NovaConfig::config->db_fragment[dbid_];
        if (!frag->log_replica_stoc_ids.empty()) {
            uint32_t stoc_server_id = nova::NovaConfig::config->dc_servers[frag->log_replica_stoc_ids[0]].server_id;
            RDMA_LOG(rdmaio::INFO) << fmt::format(
                        "Recover the latest memtables from log files at StoC-{}",
                        stoc_server_id);
            client->InitiateQueryLogFile(stoc_server_id,
                                         nova::NovaConfig::config->my_server_id,
                                         dbid_, &logfile_buf);
            client->Wait();
        }

        for (auto &it : logfile_buf) {
            RDMA_LOG(rdmaio::INFO)
                << fmt::format("log file {}:{}", it.first, it.second);
        }

        // Recover log records.
        std::vector<VersionSubRange> subrange_edits;
        RDMA_ASSERT(versions_->Recover(Slice(buf, options_.max_file_size),
                                       &subrange_edits).ok());

        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Recovered Version: {}",
                           versions_->current()->DebugString());
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Recovered lsn:{} lfn:{}", versions_->last_sequence_,
                           versions_->NextFileNumber());
        versions_->last_sequence_.fetch_add(1);

        // Inform all StoCs of the mapping between a file and rtable id.
        std::vector<FileMetaData *> *files = versions_->current()->files_;
        std::map<uint32_t, std::map<std::string, uint32_t>> stoc_fn_rtableid;
        for (int level = 0; level < config::kNumLevels; level++) {
            for (int i = 0; i < files[level].size(); i++) {
                auto meta = files[level][i];
                std::string metafilename = TableFileName(dbname_, meta->number,
                                                         true);
                std::string filename = TableFileName(dbname_, meta->number,
                                                     false);
                auto meta_handle = meta->meta_block_handle;
                stoc_fn_rtableid[meta_handle.server_id][metafilename] = meta_handle.rtable_id;

                for (auto &data_block : meta->data_block_group_handles) {
                    stoc_fn_rtableid[data_block.server_id][filename] = data_block.rtable_id;
                }
            }
        }

        for (auto &mapping : stoc_fn_rtableid) {
            uint32_t stoc_id = mapping.first;
            std::map<std::string, uint32_t> &fnrtable = mapping.second;
            RDMA_LOG(rdmaio::INFO)
                << fmt::format("Recover Install FileRTable mapping {} size:{}",
                               stoc_id,
                               fnrtable.size());
            client->InitiateFileNameRTableMapping(stoc_id, fnrtable);
            client->Wait();
        }


        // Fetch metadata blocks.
        for (int level = 0; level < config::kNumLevels; level++) {
            for (int i = 0; i < files[level].size(); i++) {
                auto meta = files[level][i];
                std::string filename = TableFileName(dbname_, meta->number,
                                                     true);
                uint32_t backing_scid = options_.mem_manager->slabclassid(0,
                                                                          meta->meta_block_handle.size);
                char *backing_buf = options_.mem_manager->ItemAlloc(0,
                                                                    backing_scid);
                RDMA_LOG(rdmaio::INFO)
                    << fmt::format("Recover metadata blocks {} handle:{}",
                                   filename,
                                   meta->meta_block_handle.DebugString());
                client->InitiateRTableReadDataBlock(meta->meta_block_handle, 0,
                                                    meta->meta_block_handle.size,
                                                    backing_buf, filename);
                client->Wait();

                WritableFile *writable_file;
                EnvFileMetadata env_meta = {};
                filename = TableFileName(dbname_, meta->number);
                Status s = env_->NewWritableFile(filename, env_meta,
                                                 &writable_file);
                RDMA_ASSERT(s.ok());
                Slice sstable_rtable(backing_buf,
                                     meta->meta_block_handle.size);
                s = writable_file->Append(sstable_rtable);
                RDMA_ASSERT(s.ok());
                s = writable_file->Flush();
                RDMA_ASSERT(s.ok());
                s = writable_file->Sync();
                RDMA_ASSERT(s.ok());
                s = writable_file->Close();
                RDMA_ASSERT(s.ok());
                delete writable_file;
                writable_file = nullptr;

                options_.mem_manager->FreeItem(0, backing_buf, backing_scid);
            }
        }

        // Rebuild table locator.
        ReadOptions ro;
        ro.mem_manager = options_.mem_manager;
        ro.dc_client = options_.dc_client;
        ro.thread_id = 0;
        ro.hash = 0;
        uint32_t memtableid = 0;
        for (int i = 0; i < files[0].size(); i++) {
            auto meta = files[0][i];
            RDMA_LOG(rdmaio::INFO)
                << fmt::format("Recover L0 data file {} ", meta->DebugString());
            auto it = table_cache_->NewIterator(
                    AccessCaller::kCompaction, ro, *meta, meta->number, 0,
                    meta->converted_file_size);
            it->SeekToFirst();
            while (it->Valid()) {
                ParsedInternalKey ik;
                ParseInternalKey(it->key(), &ik);
                uint64_t hash;
                nova::str_to_int(ik.user_key.data(), &hash, ik.user_key.size());
                table_locator_->Insert(ik.user_key, hash, memtableid);

                AtomicMemTable &mem = versions_->mid_table_mapping_[memtableid];
                mem.l0_file_numbers_.push_back(meta->number);
                mem.is_immutable_ = true;
                mem.is_flushed_ = true;
                it->Next();
            }
            memtableid++;
            delete it;
        }

        if (options_.enable_subranges) {
            SubRanges *srs = subrange_manager_->latest_subranges_;
            srs->subranges.resize(subrange_edits.size());
            for (const auto &edit : subrange_edits) {
                srs->subranges[edit.subrange_id].lower = SubRange::Copy(
                        edit.lower);
                srs->subranges[edit.subrange_id].upper = SubRange::Copy(
                        edit.upper);
                srs->subranges[edit.subrange_id].lower_inclusive = edit.lower_inclusive;
                srs->subranges[edit.subrange_id].upper_inclusive = edit.upper_inclusive;
                srs->subranges[edit.subrange_id].num_duplicates = edit.num_duplicates;
            }
            srs->AssertSubrangeBoundary(user_comparator_);
            RDMA_LOG(rdmaio::INFO)
                << fmt::format("Recovered Subranges: {}", srs->DebugString());
        }

        for (const auto &logfile : logfile_buf) {
            uint32_t index = logfile.first.find_last_of('-');
            uint32_t log_file_number = std::stoi(
                    logfile.first.substr(index + 1));
            versions_->MarkFileNumberUsed(log_file_number);
        }
        options_.mem_manager->FreeItem(0, buf, scid);

        uint32_t recovered_log_records = 0;
        timeval rdma_read_complete;
        gettimeofday(&rdma_read_complete, nullptr);

        RDMA_ASSERT(RecoverLogFile(logfile_buf, &recovered_log_records,
                                   &rdma_read_complete).ok());
        timeval end = {};
        gettimeofday(&end, nullptr);

        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Total recovery duration: {},{},{},{},{}",
                           logfile_buf.size(),
                           recovered_log_records,
                           time_diff(start, rdma_read_complete),
                           time_diff(rdma_read_complete, end),
                           time_diff(start, end));
        return Status::OK();
    }

    Status
    DBImpl::RecoverLogFile(const std::map<std::string, uint64_t> &logfile_buf,
                           uint32_t *recovered_log_records,
                           timeval *rdma_read_complete) {
        if (logfile_buf.empty()) {
            return Status::OK();
        }

        auto client = reinterpret_cast<NovaBlockCCClient *> (options_.dc_client);
        std::vector<NovaCCRecoveryThread *> recovery_threads;
        uint32_t memtable_per_thread =
                logfile_buf.size() / options_.num_recovery_thread;
        uint32_t remainder = logfile_buf.size() % options_.num_recovery_thread;
        RDMA_ASSERT(logfile_buf.size() < partitioned_active_memtables_.size());
        uint32_t memtable_index = 0;
        uint32_t thread_id = 0;
        while (memtable_index < logfile_buf.size()) {
            std::vector<MemTable *> memtables;
            for (int j = 0; j < memtable_per_thread; j++) {
                memtables.push_back(
                        partitioned_active_memtables_[memtable_index]->memtable);
                memtable_index++;
            }
            if (remainder > 0) {
                memtable_index++;
                remainder--;
            }
            NovaCCRecoveryThread *thread = new NovaCCRecoveryThread(
                    thread_id, memtables, options_.mem_manager);
            recovery_threads.push_back(thread);
            thread_id++;
        }

        // Pin each recovery thread to a core.
        std::vector<std::thread> threads;
        for (int i = 0; i < recovery_threads.size(); i++) {
            threads.emplace_back(&NovaCCRecoveryThread::Recover,
                                 recovery_threads[i]);
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i, &cpuset);
            int rc = pthread_setaffinity_np(threads[i].native_handle(),
                                            sizeof(cpu_set_t), &cpuset);
            RDMA_ASSERT(rc == 0) << rc;
        }

        std::vector<char *> rdma_bufs;
        std::vector<uint32_t> reqs;
        nova::CCFragment *frag = nova::NovaConfig::config->db_fragment[dbid_];
        uint32_t stoc_server_id = nova::NovaConfig::config->dc_servers[frag->log_replica_stoc_ids[0]].server_id;
        for (auto &replica : logfile_buf) {
            uint32_t scid = options_.mem_manager->slabclassid(0,
                                                              options_.max_log_file_size);
            char *rdma_buf = options_.mem_manager->ItemAlloc(0, scid);
            RDMA_ASSERT(rdma_buf);
            rdma_bufs.push_back(rdma_buf);
            uint32_t reqid = client->InitiateReadInMemoryLogFile(rdma_buf,
                                                                 stoc_server_id,
                                                                 replica.second,
                                                                 options_.max_log_file_size);
            reqs.push_back(reqid);
        }

        // Wait for all RDMA READ to complete.
        for (auto &replica : logfile_buf) {
            client->Wait();
        }

        for (int i = 0; i < reqs.size(); i++) {
            leveldb::CCResponse response;
            RDMA_ASSERT(client->IsDone(reqs[i], &response, nullptr));
        }

        gettimeofday(rdma_read_complete, nullptr);

        // put all rdma foreground to sleep.
        for (int i = 0; i < rdma_threads_.size(); i++) {
            auto *thread = reinterpret_cast<nova::NovaRDMAComputeComponent *>(rdma_threads_[i]);
            thread->should_pause = true;
        }

        // Divide.
        RDMA_LOG(rdmaio::INFO)
            << fmt::format(
                    "Start recovery: memtables:{} memtable_per_thread:{}",
                    logfile_buf.size(),
                    memtable_per_thread);

        std::vector<char *> replicas;
        thread_id = 0;
        for (int i = 0; i < logfile_buf.size(); i++) {
            if (replicas.size() == memtable_per_thread) {
                recovery_threads[thread_id]->log_replicas_ = replicas;
                replicas.clear();
                thread_id += 1;
            }
            replicas.push_back(rdma_bufs[i]);
        }

        if (!replicas.empty()) {
            recovery_threads[thread_id]->log_replicas_ = replicas;
        }

        for (int i = 0;
             i < options_.num_recovery_thread; i++) {
            sem_post(&recovery_threads[i]->sem_);
        }
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Start recovery: recovery threads:{}",
                           recovery_threads.size());
        for (int i = 0; i < recovery_threads.size(); i++) {
            threads[i].join();
        }

        uint32_t log_records = 0;
        for (int i = 0; i < recovery_threads.size(); i++) {
            auto recovery = recovery_threads[i];
            log_records += recovery->recovered_log_records;

            RDMA_LOG(rdmaio::INFO)
                << fmt::format("recovery duration of {}: {},{},{},{}",
                               i,
                               recovery->log_replicas_.size(),
                               recovery->recovered_log_records,
                               recovery->new_memtable_time,
                               recovery->recovery_time);
        }
        *recovered_log_records = log_records;

        uint64_t max_sequence = 0;
        for (auto &recovery : recovery_threads) {
            max_sequence = std::max(max_sequence,
                                    recovery->max_sequence_number);
        }

        if (versions_->LastSequence() < max_sequence) {
            versions_->SetLastSequence(max_sequence);
        }

        for (int i = 0; i < rdma_threads_.size(); i++) {
            auto *thread = reinterpret_cast<nova::NovaRDMAComputeComponent *>(rdma_threads_[i]);
            thread->should_pause = false;
            sem_post(&thread->sem_);
        }
        return Status::OK();
    }

    void DBImpl::TestCompact(leveldb::EnvBGThread *bg_thread,
                             const std::vector<leveldb::EnvBGTask> &tasks) {
        VersionEdit edit;
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            FileMetaData &meta = imm->meta();
            meta.number = versions_->NewFileNumber();
            meta.flush_timestamp = versions_->last_sequence_;
            meta.level = 0;
            Status s;
            Iterator *iter = imm->NewIterator(TraceType::IMMUTABLE_MEMTABLE,
                                              AccessCaller::kCompaction);
            s = TestBuildTable(dbname_, env_, options_, table_cache_, iter,
                               &meta, bg_thread);
            RDMA_ASSERT(s.ok()) << s.ToString();
            delete iter;
            // Note that if file_size is zero, the file has been deleted and
            // should not be added to the manifest.
            int level = 0;
            if (meta.file_size > 0) {
                RDMA_ASSERT(imm->memtableid() != 0);
                edit.AddFile(level, imm->memtableid(),
                             meta.number,
                             meta.file_size,
                             meta.converted_file_size,
                             meta.flush_timestamp,
                             meta.smallest,
                             meta.largest,
                             meta.meta_block_handle,
                             meta.data_block_group_handles);
            }
        }
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
        Version *v = new Version(&internal_comparator_, table_cache_, &options_,
                                 versions_->version_id_seq_.fetch_add(1));
        mutex_.Lock();
        Status s = versions_->LogAndApply(&edit, v);
        RDMA_ASSERT(s.ok());
        mutex_.Unlock();

        std::map<uint32_t, std::vector<EnvBGTask>> pid_tasks;
        for (auto &task : tasks) {
            pid_tasks[task.memtable_partition_id].push_back(task);
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            auto atomic_imm = &versions_->mid_table_mapping_[imm->memtableid()];
            atomic_imm->is_immutable_ = true;
            atomic_imm->SetFlushed(dbname_, {imm->meta().number});
        }

        for (auto &it : pid_tasks) {
            // New verion is installed. Then remove it from the immutable memtables.
            MemTablePartition *p = partitioned_active_memtables_[it.first];
            p->mutex.Lock();
            bool no_slots = p->available_slots.empty();
            for (auto &task : it.second) {
                RDMA_ASSERT(task.imm_slot < partitioned_imms_.size());
                RDMA_ASSERT(partitioned_imms_[task.imm_slot] != 0);
                partitioned_imms_[task.imm_slot] = 0;
                p->available_slots.push(task.imm_slot);
                RDMA_ASSERT(p->available_slots.size() <= p->imm_slots.size());
            }
            if (no_slots) {
                p->background_work_finished_signal_.SignalAll();
            }
            number_of_immutable_memtables_.fetch_add(-it.second.size());
            p->mutex.Unlock();
        }
    }

    bool DBImpl::CompactMemTableStaticPartition(leveldb::EnvBGThread *bg_thread,
                                                const std::vector<leveldb::EnvBGTask> &tasks) {
        VersionEdit edit;
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            RDMA_ASSERT(imm);
            FileMetaData &meta = imm->meta();
            meta.number = versions_->NewFileNumber();
            meta.flush_timestamp = versions_->last_sequence_;
            meta.level = 0;
            Status s;
            Iterator *iter = imm->NewIterator(TraceType::IMMUTABLE_MEMTABLE,
                                              AccessCaller::kCompaction);
            s = BuildTable(dbname_, env_, options_, table_cache_, iter,
                           &meta, bg_thread);
            RDMA_ASSERT(s.ok()) << s.ToString();
            delete iter;
            // Note that if file_size is zero, the file has been deleted and
            // should not be added to the manifest.
            int level = 0;
            if (meta.file_size > 0) {
                RDMA_ASSERT(imm->memtableid() != 0);
                edit.AddFile(level, imm->memtableid(),
                             meta.number,
                             meta.file_size,
                             meta.converted_file_size,
                             meta.flush_timestamp,
                             meta.smallest,
                             meta.largest,
                             meta.meta_block_handle,
                             meta.data_block_group_handles);
            }
        }

        // Include the latest version.
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
        Version *v = new Version(&internal_comparator_, table_cache_, &options_,
                                 versions_->version_id_seq_.fetch_add(1));
        mutex_.Lock();
        Status s = versions_->LogAndApply(&edit, v);
        RDMA_ASSERT(s.ok());
        mutex_.Unlock();

        std::map<uint32_t, std::vector<EnvBGTask>> pid_tasks;
        for (auto &task : tasks) {
            pid_tasks[task.memtable_partition_id].push_back(task);
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            auto atomic_imm = &versions_->mid_table_mapping_[imm->memtableid()];
            atomic_imm->is_immutable_ = true;
            atomic_imm->SetFlushed(dbname_, {imm->meta().number});
        }

        std::vector<uint32_t> closed_memtable_log_files;
        for (auto &it : pid_tasks) {
            // New verion is installed. Then remove it from the immutable memtables.
            MemTablePartition *p = partitioned_active_memtables_[it.first];
            p->mutex.Lock();
            bool no_slots = p->available_slots.empty();
            for (auto &task : it.second) {
                RDMA_ASSERT(task.imm_slot < partitioned_imms_.size());
                RDMA_ASSERT(partitioned_imms_[task.imm_slot] != 0);
                partitioned_imms_[task.imm_slot] = 0;
                p->available_slots.push(task.imm_slot);
                RDMA_ASSERT(p->available_slots.size() <= p->imm_slots.size());
            }
            if (no_slots) {
                p->background_work_finished_signal_.SignalAll();
            }
            number_of_immutable_memtables_.fetch_add(-it.second.size());
            for (auto &log_file : p->closed_log_files) {
                closed_memtable_log_files.push_back(log_file);
            }
            p->closed_log_files.clear();
            p->mutex.Unlock();
        }

        // Delete log files.
        if (nova::NovaConfig::config->log_record_mode ==
            nova::NovaLogRecordMode::LOG_RDMA) {
            for (const auto &file : closed_memtable_log_files) {
                bg_thread->dc_client()->InitiateCloseLogFile(
                        fmt::format("{}-{}-{}", server_id_, dbid_, file),
                        dbid_);
            }
        }
    }

    bool DBImpl::CompactMultipleMemTablesStaticPartition(
            leveldb::EnvBGThread *bg_thread,
            const std::vector<leveldb::EnvBGTask> &tasks) {
        VersionEdit edit;
        std::vector<Iterator *> iterators;
        CompactionStats stats;
        std::vector<uint32_t> memtableids;
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            RDMA_ASSERT(imm);
            Iterator *iter = imm->NewIterator(TraceType::IMMUTABLE_MEMTABLE,
                                              AccessCaller::kCompaction);
            iterators.push_back(iter);
            stats.input_source.num_files += 1;
            stats.input_source.file_size += options_.max_file_size;
            memtableids.push_back(imm->memtableid());
        }

        Iterator *it = NewMergingIterator(&internal_comparator_, &iterators[0],
                                          iterators.size());
        SubRanges *subranges = nullptr;
        if (subrange_manager_) {
            subranges = subrange_manager_->latest_subranges_;
        }
        CompactionState *state = new CompactionState(nullptr, subranges);
        std::function<uint64_t(void)> fn_generator = std::bind(
                &VersionSet::NewFileNumber, versions_);
        CompactionJob job(fn_generator, env_, dbname_, user_comparator_,
                          options_);
        job.CompactTables(state, bg_thread, it, &stats,
                          options_.prune_memtable_before_flushing,
                          kCompactMemTables);
        for (auto memit : iterators) {
            delete memit;
        }
        iterators.clear();

        InstallCompactionResults(state, &edit, 0);
        // Include the latest version.
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
        Version *v = new Version(&internal_comparator_, table_cache_, &options_,
                                 versions_->version_id_seq_.fetch_add(1));
        mutex_.Lock();
        Status s = versions_->LogAndApply(&edit, v);
        RDMA_ASSERT(s.ok());
        for (auto &out : state->outputs) {
            l0fn_memtableids[out.number] = memtableids;
        }
        mutex_.Unlock();

        std::vector<uint64_t> l0fns;
        l0fns.resize(state->outputs.size());
        for (int i = 0; i < state->outputs.size(); i++) {
            l0fns[i] = state->outputs[i].number;
        }

        std::map<uint32_t, std::vector<EnvBGTask>> pid_tasks;
        for (auto &task : tasks) {
            pid_tasks[task.memtable_partition_id].push_back(task);
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            auto atomic_imm = &versions_->mid_table_mapping_[imm->memtableid()];
            atomic_imm->is_immutable_ = true;
            atomic_imm->SetFlushed(dbname_, l0fns);
        }

        std::vector<uint32_t> closed_memtable_log_files;
        for (auto &it : pid_tasks) {
            // New verion is installed. Then remove it from the immutable memtables.
            MemTablePartition *p = partitioned_active_memtables_[it.first];
            p->mutex.Lock();
            bool no_slots = p->available_slots.empty();
            for (auto &task : it.second) {
                RDMA_ASSERT(task.imm_slot < partitioned_imms_.size());
                RDMA_ASSERT(partitioned_imms_[task.imm_slot] != 0);
                partitioned_imms_[task.imm_slot] = 0;
                p->available_slots.push(task.imm_slot);
                RDMA_ASSERT(p->available_slots.size() <= p->imm_slots.size());
            }
            if (no_slots) {
                p->background_work_finished_signal_.SignalAll();
            }
            number_of_immutable_memtables_.fetch_add(-it.second.size());
            for (auto &log_file : p->closed_log_files) {
                closed_memtable_log_files.push_back(log_file);
            }
            p->closed_log_files.clear();
            p->mutex.Unlock();
        }

        // Delete log files.
        if (nova::NovaConfig::config->log_record_mode ==
            nova::NovaLogRecordMode::LOG_RDMA) {
            for (const auto &file : closed_memtable_log_files) {
                bg_thread->dc_client()->InitiateCloseLogFile(
                        fmt::format("{}-{}-{}", server_id_, dbid_, file),
                        dbid_);
            }
        }

        delete state;
    }

    bool DBImpl::CompactMemTable(EnvBGThread *bg_thread,
                                 const std::vector<EnvBGTask> &tasks) {
        VersionEdit edit;
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            versions_->mid_table_mapping_[imm->memtableid()].mutex_.lock();
            RDMA_ASSERT(
                    versions_->mid_table_mapping_[imm->memtableid()].is_immutable_);
            RDMA_ASSERT(
                    !versions_->mid_table_mapping_[imm->memtableid()].is_flushed_);
            versions_->mid_table_mapping_[imm->memtableid()].mutex_.unlock();


            FileMetaData &meta = imm->meta();
            meta.number = versions_->NewFileNumber();
            meta.flush_timestamp = versions_->last_sequence_;
            meta.level = 0;

            Status s;
            Iterator *iter = imm->NewIterator(TraceType::IMMUTABLE_MEMTABLE,
                                              AccessCaller::kCompaction);
            s = BuildTable(dbname_, env_, options_, table_cache_, iter,
                           &meta,
                           bg_thread);
            RDMA_ASSERT(s.ok()) << s.ToString();
            delete iter;
            // Note that if file_size is zero, the file has been deleted and
            // should not be added to the manifest.
            int level = 0;
            if (meta.file_size > 0) {
                RDMA_ASSERT(imm->memtableid() != 0);
                edit.AddFile(level, imm->memtableid(),
                             meta.number,
                             meta.file_size,
                             meta.converted_file_size,
                             meta.flush_timestamp,
                             meta.smallest,
                             meta.largest,
                             meta.meta_block_handle,
                             meta.data_block_group_handles);
            }
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format(
                        "db[{}]: !!!!!!!!!!!!!!!!!!!!!!!!!!!! Flush memtable-{}",
                        dbid_, imm->memtableid());
        }

        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
        Version *v = new Version(&internal_comparator_, table_cache_, &options_,
                                 versions_->version_id_seq_.fetch_add(1));
        RDMA_ASSERT(v->version_id() < MAX_LIVE_MEMTABLES);
        mutex_.Lock();
        Status s = versions_->LogAndApply(&edit, v);
        RDMA_ASSERT(s.ok());
        mutex_.Unlock();

        uint32_t num_available = 0;
        bool wakeup_all = false;
        range_lock_.Lock();
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            if (imm->is_pinned_) {
                number_of_available_pinned_memtables_++;
                RDMA_ASSERT(number_of_available_pinned_memtables_ <=
                            min_memtables_);
            } else {
                num_available += 1;
                wakeup_all = true;
            }
        }
        number_of_immutable_memtables_ -= tasks.size();
        std::vector<uint32_t> closed_memtable_log_files(
                closed_memtable_log_files_.begin(),
                closed_memtable_log_files_.end());
        closed_memtable_log_files_.clear();
        range_lock_.Unlock();

        if (nova::NovaConfig::config->log_record_mode ==
            nova::NovaLogRecordMode::LOG_RDMA) {
            for (const auto &file : closed_memtable_log_files) {
                bg_thread->dc_client()->InitiateCloseLogFile(
                        fmt::format("{}-{}-{}", server_id_, dbid_, file),
                        dbid_);
            }
        }

        options_.memtable_pool->mutex_.lock();
        options_.memtable_pool->num_available_memtables_ += num_available;
        RDMA_ASSERT(options_.memtable_pool->num_available_memtables_ <
                    nova::NovaConfig::config->num_memtables - dbs_.size());
        options_.memtable_pool->mutex_.unlock();

        if (wakeup_all) {
            for (int i = 0; i < dbs_.size(); i++) {
                options_.memtable_pool->range_cond_vars_[i]->SignalAll();
            }
        } else {
            options_.memtable_pool->range_cond_vars_[dbid_]->SignalAll();
        }

        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            versions_->mid_table_mapping_[imm->memtableid()].SetFlushed(dbname_,
                                                                        {imm->meta().number});
        }
        return true;
    }

    void DBImpl::RecordBackgroundError(const Status &s) {
        mutex_.AssertHeld();
        if (bg_error_.ok()) {
            bg_error_ = s;
        }
    }

    void DBImpl::ScheduleBGTask(int thread_id,
                                MemTable *imm,
                                void *compaction,
                                uint32_t partition_id,
                                uint32_t imm_slot,
                                unsigned int *rand_seed) {
        if (thread_id == -1) {
            thread_id = EnvBGThread::bg_thread_id_seq.fetch_add(1,
                                                                std::memory_order_relaxed) %
                        compaction_threads_.size();
        }
        EnvBGTask task = {};
        task.db = this;
        task.compaction_task = compaction;
        task.memtable = imm;
        if (imm) {
            task.memtable_size_mb = imm->ApproximateMemoryUsage() / 1024 / 1024;
        }
        task.memtable_partition_id = partition_id;
        task.imm_slot = imm_slot;
        if (compaction_threads_[thread_id]->Schedule(task)) {
        }
    }

    void DBImpl::PerformCompaction(leveldb::EnvBGThread *bg_thread,
                                   const std::vector<EnvBGTask> &tasks) {
        std::vector<EnvBGTask> memtable_tasks;
        std::vector<EnvBGTask> sstable_tasks;
        for (auto &task : tasks) {
            if (task.memtable) {
                memtable_tasks.push_back(task);
            }
            if (task.compaction_task) {
                sstable_tasks.push_back(task);
            }
        }

        if (!memtable_tasks.empty()) {
            if (options_.memtable_type == MemTableType::kStaticPartition) {
                if (options_.enable_flush_multiple_memtables) {
                    CompactMultipleMemTablesStaticPartition(bg_thread,
                                                            memtable_tasks);
                } else {
                    CompactMemTableStaticPartition(bg_thread, memtable_tasks);

                }
            } else {
                CompactMemTable(bg_thread, memtable_tasks);
            }
        }

        for (const auto &task : sstable_tasks) {
            CompactionState *state = reinterpret_cast<CompactionState *> (task.compaction_task);
            auto c = state->compaction;
            Iterator *input = versions_->MakeInputIterator(c, bg_thread);
            CompactionStats stats;
            stats.input_source.num_files = c->num_input_files(0);
            stats.input_source.level = c->level();
            stats.input_source.file_size = c->num_input_file_sizes(0);

            stats.input_target.num_files = c->num_input_files(1);
            stats.input_target.level = c->target_level();
            stats.input_target.file_size = c->num_input_file_sizes(1);

            std::function<uint64_t(void)> fn_generator = std::bind(
                    &VersionSet::NewFileNumber, versions_);
            CompactionJob job(fn_generator, env_, dbname_, user_comparator_,
                              options_);
            Status status = job.CompactTables(state, bg_thread, input,
                                              &stats, true, kCompactSSTables);
        }

        while (options_.major_compaction_type ==
               MajorCompactionType::kMajorSingleThreaded) {
            MutexLock l(&mutex_);
            if (is_major_compaciton_running_) {
                return;
            }

            if (versions_->NeedsCompaction() ||
                versions_->current()->files_[0].size() >=
                config::kL0_StopWritesTrigger) {

                Compaction *c = versions_->PickCompaction(
                        bg_thread->thread_id());
                if (c == nullptr) {
                    continue;
                }

                EnvBGTask task = {};
                task.compaction_task = c;
                PerformMajorCompaction(bg_thread, task);
                delete c;
            } else {
                return;
            }
        }
    }

    void DBImpl::CoordinateLocalMajorCompaction(leveldb::Version *current) {
        std::map<uint32_t, std::vector<uint64_t>> mid_l0fns;
        std::vector<Compaction *> compactions;
        current->ComputeNonOverlappingSet(&compactions);
        std::string debug = "Coordinated compaction picks compaction sets: ";
        for (auto c : compactions) {
            debug += c->DebugString(user_comparator_);
            debug += "\n";
        }
        RDMA_LOG(rdmaio::INFO) << debug;

        {
            std::string reason;
            bool valid = current->AssertNonOverlappingSet(compactions, &reason);
            RDMA_ASSERT(valid) << fmt::format("assertion failed {}", reason);
        }

        if (compactions.empty()) {
            return;
        }

        VersionEdit edit;
        for (auto it = compactions.begin(); it != compactions.end(); it++) {
            auto c = *it;
            if (c->IsTrivialMove()) {
                // Move file to next level
                assert(c->level() >= 0);
                assert(c->num_input_files(0) == 1);
                assert(c->num_input_files(1) == 0);
                FileMetaData *f = c->input(0, 0);
                edit.DeleteFile(c->level(), f->memtable_id, f->number);
                edit.AddFile(c->target_level(),
                             0,
                             f->number, f->file_size,
                             f->converted_file_size,
                             f->flush_timestamp,
                             f->smallest,
                             f->largest,
                             f->meta_block_handle,
                             f->data_block_group_handles);
                std::string output = fmt::format(
                        "Moved #{}@{} to level-{} {} bytes\n",
                        f->number, c->level(), c->target_level(), f->file_size);
                Log(options_.info_log, "%s", output.c_str());
                RDMA_LOG(rdmaio::INFO) << output;
                delete c;
                it = compactions.erase(it);
            }
        }

        SubRanges *subs = nullptr;
        if (subrange_manager_) {
            subs = subrange_manager_->latest_subranges_;
        }
        uint64_t smallest_snapshot = versions_->LastSequence();

        std::vector<CompactionState *> states;
        for (int i = 0; i < compactions.size(); i++) {
            auto state = new CompactionState(compactions[i], subs);
            state->smallest_snapshot = smallest_snapshot;
            states.push_back(state);
        }

        if (!compactions.empty()) {
            int tid = 0;
            for (int i = 1; i < states.size(); i++) {
                int thread_id = (tid + i) % compaction_threads_.size();
                ScheduleBGTask(thread_id, nullptr, states[i], 0, 0, nullptr);
            }
            auto c = compactions[0];
            Iterator *input = versions_->MakeInputIterator(c,
                                                           compaction_coordinator_thread_);
            CompactionStats stats;
            stats.input_source.num_files = c->num_input_files(0);
            stats.input_source.level = c->level();
            stats.input_source.file_size = c->num_input_file_sizes(0);
            stats.input_target.num_files = c->num_input_files(1);
            stats.input_target.level = c->target_level();
            stats.input_target.file_size = c->num_input_file_sizes(1);

            std::function<uint64_t(void)> fn_generator = std::bind(
                    &VersionSet::NewFileNumber, versions_);
            CompactionJob job(fn_generator, env_, dbname_, user_comparator_,
                              options_);
            Status status = job.CompactTables(states[0],
                                              compaction_coordinator_thread_,
                                              input, &stats, true,
                                              kCompactSSTables);
            // Wait for majors to complete.
            for (int i = 1; i < compactions.size(); i++) {
                sem_wait(&compactions[i]->complete_signal_);
            }
        }

        for (auto state : states) {
            InstallCompactionResults(state, &edit,
                                     state->compaction->target_level());
        }
        versions_->AppendChangesToManifest(&edit,
                                           manifest_file_,
                                           options_.manifest_stoc_id);
        Version *v = new Version(&internal_comparator_, table_cache_, &options_,
                                 versions_->version_id_seq_.fetch_add(1));
        mutex_.Lock();
        for (auto state : states) {
            versions_->AddCompactedInputs(state->compaction,
                                          &compacted_tables_);
        }
        versions_->LogAndApply(&edit, v);
        DeleteObsoleteVersions(compaction_coordinator_thread_);
        DeleteObsoleteFiles(compaction_coordinator_thread_, &mid_l0fns);
        mutex_.Unlock();
        CleanUpTableLocator(mid_l0fns);

        RDMA_LOG(rdmaio::INFO) << v->DebugString();

        for (auto c : compactions) {
            delete c;
        }
        for (auto state : states) {
            delete state;
        }
    }

    void DBImpl::CoordinateStoCMajorCompaction(leveldb::Version *current) {
        // TODO:
    }

    void DBImpl::CoordinateMajorCompaction() {
        while (options_.major_compaction_type == kMajorCoordinated ||
               options_.major_compaction_type == kMajorCoordinatedStoC) {
            mutex_.Lock();
            if (!versions_->NeedsCompaction() &&
                versions_->current()->files_[0].size() <=
                config::kL0_StopWritesTrigger) {
                mutex_.Unlock();
                sleep(1);
                continue;
            }
            Version *current = versions_->current();
            RDMA_ASSERT(versions_->versions_[current->version_id()].Ref() ==
                        current);
            mutex_.Unlock();

            if (options_.major_compaction_type == kMajorCoordinated) {
                CoordinateLocalMajorCompaction(current);
            } else {
                CoordinateStoCMajorCompaction(current);
            }
            versions_->versions_[current->version_id()].Unref(
                    dbname_);
        }
    }

    bool DBImpl::PerformMajorCompaction(EnvBGThread *bg_thread,
                                        const EnvBGTask &task) {
        Compaction *c = reinterpret_cast<Compaction *>(task.compaction_task);
        Status status;
        if (c == nullptr) {
            // Nothing to do
            return false;
        }
        is_major_compaciton_running_ = true;
        if (c->IsTrivialMove()) {
            // Move file to next level
            assert(c->level() >= 0);
            assert(c->num_input_files(0) == 1);
            assert(c->num_input_files(1) == 0);
            FileMetaData *f = c->input(0, 0);
            c->edit()->DeleteFile(c->level(), f->memtable_id, f->number);
            c->edit()->AddFile(c->target_level(),
                               0,
                               f->number, f->file_size,
                               f->converted_file_size,
                               f->flush_timestamp,
                               f->smallest,
                               f->largest,
                               f->meta_block_handle,
                               f->data_block_group_handles);
            Version *v = new Version(&internal_comparator_, table_cache_,
                                     &options_,
                                     versions_->version_id_seq_.fetch_add(1));
            status = versions_->LogAndApply(c->edit(), v);
            if (!status.ok()) {
                RecordBackgroundError(status);
            }
            std::string output = fmt::format(
                    "Moved #{}@{} to level-{} {} bytes {}\n",
                    f->number, c->level(), c->target_level(), f->file_size,
                    status.ToString().c_str());
            Log(options_.info_log, "%s", output.c_str());
            RDMA_LOG(rdmaio::INFO) << output;
        } else {
            // Release mutex while we're actually doing the compaction work
            mutex_.Unlock();
            std::map<uint32_t, std::vector<uint64_t>> mid_l0fns;
            CompactionState *compact = new CompactionState(c, nullptr);
            compact->smallest_snapshot = versions_->LastSequence();
            Iterator *input = versions_->MakeInputIterator(compact->compaction,
                                                           bg_thread);
            CompactionStats stats;
            stats.input_source.num_files = c->num_input_files(0);
            stats.input_source.level = c->level();
            stats.input_source.file_size = c->num_input_file_sizes(0);

            stats.input_target.num_files = c->num_input_files(1);
            stats.input_target.level = c->target_level();
            stats.input_target.file_size = c->num_input_file_sizes(1);

            std::function<uint64_t(void)> fn_generator = std::bind(
                    &VersionSet::NewFileNumber, versions_);
            CompactionJob job(fn_generator, env_, dbname_, user_comparator_,
                              options_);
            status = job.CompactTables(compact, bg_thread, input, &stats, true,
                                       kCompactSSTables);
            InstallCompactionResults(compact, compact->compaction->edit(),
                                     c->target_level());
            versions_->AppendChangesToManifest(compact->compaction->edit(),
                                               manifest_file_,
                                               options_.manifest_stoc_id);
            Version *v = new Version(&internal_comparator_, table_cache_,
                                     &options_,
                                     versions_->version_id_seq_.fetch_add(1));
            mutex_.Lock();
            versions_->AddCompactedInputs(compact->compaction,
                                          &compacted_tables_);
            versions_->LogAndApply(compact->compaction->edit(), v);
            if (!status.ok()) {
                RecordBackgroundError(status);
            }
            versions_->versions_[c->input_version_->version_id()].Unref(
                    dbname_);
            DeleteObsoleteVersions(bg_thread);
            DeleteObsoleteFiles(bg_thread, &mid_l0fns);
            CleanUpTableLocator(mid_l0fns);
            delete compact;
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("!!!!!!!!!!!!!Compaction complete");
        }
        is_major_compaciton_running_ = false;
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("New version: {}",
                           versions_->current()->DebugString());
        if (status.ok()) {
        } else {
            Log(options_.info_log, "Compaction error: %s",
                status.ToString().c_str());
        }
        return true;
    }

    Status DBImpl::InstallCompactionResults(CompactionState *compact,
                                            VersionEdit *edit,
                                            int target_level) {
        // Add compaction outputs
        if (compact->compaction) {
            compact->compaction->AddInputDeletions(edit);
        }
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionState::Output &out = compact->outputs[i];
            edit->AddFile(target_level,
                          0,
                          out.number,
                          out.file_size,
                          out.converted_file_size,
                          versions_->last_sequence_,
                          out.smallest, out.largest,
                          out.meta_block_handle,
                          out.data_block_group_handles);
        }
        return Status::OK();
    }

    namespace {
        struct IterState {
            port::Mutex *const mu;
            Version *const version
            GUARDED_BY(mu);
            MemTable *const mem
            GUARDED_BY(mu);
            MemTable *const imm
            GUARDED_BY(mu);

            IterState(port::Mutex *mutex, MemTable *mem, MemTable *imm,
                      Version *version)
                    : mu(mutex), version(version), mem(mem), imm(imm) {}
        };

        static void CleanupIteratorState(void *arg1, void *arg2) {
            IterState *state = reinterpret_cast<IterState *>(arg1);
            state->mu->Lock();
            state->mem->Unref();
            if (state->imm != nullptr) state->imm->Unref();
            state->version->Unref();
            state->mu->Unlock();
            delete state;
        }
    }  // anonymous namespace

    Iterator *DBImpl::NewInternalIterator(const ReadOptions &options,
                                          SequenceNumber *latest_snapshot,
                                          uint32_t *seed) {
        return nullptr;
    }

    Status DBImpl::Get(const ReadOptions &options, const Slice &key,
                       std::string *value) {
        number_of_gets_ += 1;
        Status s = Status::OK();
        std::string tmp;
        SequenceNumber snapshot = kMaxSequenceNumber;
        AtomicMemTable *memtable = nullptr;
        std::vector<uint64_t> l0fns;
        if (table_locator_ != nullptr) {
            uint32_t memtableid = table_locator_->Lookup(key, options.hash);
            if (memtableid != 0) {
                RDMA_ASSERT(memtableid < MAX_LIVE_MEMTABLES) << memtableid;
                memtable = versions_->mid_table_mapping_[memtableid].Ref(
                        &l0fns);
            }
            LookupKey lkey(key, snapshot);
            if (memtable != nullptr) {
                number_of_memtable_hits_ += 1;
                RDMA_ASSERT(memtable->memtable_->memtableid() == memtableid);
                RDMA_ASSERT(memtable->memtable_->Get(lkey, value, &s))
                    << fmt::format("key:{} memtable:{} s:{}",
                                   key.ToString(),
                                   memtable->memtable_->memtableid(),
                                   s.ToString());
                versions_->mid_table_mapping_[memtableid].Unref(dbname_);
            } else {
                Version *current = nullptr;
                uint32_t vid = 0;
                while (current == nullptr) {
                    vid = versions_->current_version_id();
                    RDMA_ASSERT(vid < MAX_LIVE_MEMTABLES) << vid;
                    current = versions_->versions_[vid].Ref();
                }
                RDMA_ASSERT(current->version_id() == vid);
                if (!l0fns.empty()) {
                    s = current->Get(options, l0fns, lkey, value);
                } else {
                    // search L1 files.
                    LookupKey lkey(key, snapshot);
                    Version::GetStats stats;
                    s = current->Get(options, lkey, value, &stats,
                                     GetSearchScope::kL1AndAbove);

                }
                versions_->versions_[vid].Unref(dbname_);
            }
            return s;
        }
        return s;
    }

    void DBImpl::StartTracing() {
        if (db_profiler_) {
            db_profiler_->StartTracing();
        }
    }

    Iterator *DBImpl::NewIterator(const ReadOptions &options) {
        SequenceNumber latest_snapshot;
        uint32_t seed;
        Iterator *iter = NewInternalIterator(options, &latest_snapshot, &seed);
        return NewDBIterator(this, user_comparator(), iter,
                             (options.snapshot != nullptr
                              ? static_cast<const SnapshotImpl *>(options.snapshot)
                                      ->sequence_number()
                              : latest_snapshot),
                             seed);
    }

    const Snapshot *DBImpl::GetSnapshot() {
        MutexLock l(&mutex_);
        return snapshots_.New(versions_->LastSequence());
    }

    void DBImpl::ReleaseSnapshot(const Snapshot *snapshot) {
        MutexLock l(&mutex_);
        snapshots_.Delete(static_cast<const SnapshotImpl *>(snapshot));
    }

// Convenience methods
    Status
    DBImpl::Put(const WriteOptions &o, const Slice &key, const Slice &val) {
        processed_writes_ += 1;
        if (options_.memtable_type == MemTableType::kStaticPartition) {
            if (options_.enable_subranges && o.update_subranges) {
                // Update subranges is false during loading the database.
                return WriteSubrange(o, key, val);
            } else {
                return WriteStaticPartition(o, key, val);
            }
        }
        return Write(o, key, val);
    }

    Status DBImpl::Delete(const WriteOptions &options, const Slice &key) {
        return DB::Delete(options, key);
    }

    Status DBImpl::GenerateLogRecords(const leveldb::WriteOptions &options,
                                      leveldb::WriteBatch *updates) {
//        mutex_.Lock();
//        std::string logfile = current_log_file_name_;
//        std::list<std::string> closed_files(closed_log_files_.begin(),
//                                            closed_log_files_.end());
//        closed_log_files_.clear();
//        mutex_.Unlock();
//        // Synchronous replication.
//        uint32_t server_id = 0;
//        uint32_t dbid = 0;
//        nova::ParseDBIndexFromFile(current_log_file_name_, &server_id, &dbid);
//
//        auto dc = reinterpret_cast<leveldb::NovaBlockCCClient *>(options.dc_client);
//        RDMA_ASSERT(dc);
//        dc->set_dbid(dbid);
//        options.dc_client->InitiateReplicateLogRecords(
//                logfile, options.thread_id,
//                WriteBatchInternal::Contents(updates));
//
//        for (const auto &file : closed_files) {
//            options.dc_client->InitiateCloseLogFile(file);
//        }
        return Status::OK();
    }

    void DBImpl::StealMemTable(const leveldb::WriteOptions &options) {
        uint32_t number_of_tries = std::min((size_t) 3, dbs_.size() - 1);
        uint32_t rand_range_index = rand_r(options.rand_seed) % dbs_.size();

        for (int i = 0; i < number_of_tries; i++) {
            // steal a memtable from another range.
            rand_range_index = (rand_range_index + 1) % dbs_.size();
            if (rand_range_index == dbid_) {
                rand_range_index = (rand_range_index + 1) % dbs_.size();
            }

            auto steal_from_range = reinterpret_cast<DBImpl *>(dbs_[rand_range_index]);
            if (!steal_from_range->range_lock_.TryLock()) {
                continue;
            }
            bool steal_success = false;
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format(
                        "db[{}]: Try Steal from range {} {}",
                        dbid_,
                        steal_from_range->dbid_,
                        steal_from_range->active_memtables_.size());

            double expected_share =
                    (double) steal_from_range->processed_writes_ /
                    (double) options.total_writes;
            double actual_share =
                    (double) (steal_from_range->number_of_active_memtables_ +
                              steal_from_range->number_of_immutable_memtables_)
                    /
                    (double) nova::NovaConfig::config->num_memtables;

            if (steal_from_range->active_memtables_.size() > 1 &&
                processed_writes_ >=
                (double) (steal_from_range->processed_writes_) * 1.1 &&
                actual_share > expected_share) {
                uint32_t memtable_index = rand_r(options.rand_seed) %
                                          steal_from_range->active_memtables_.size();

                AtomicMemTable *steal_table = steal_from_range->active_memtables_[memtable_index];
                if (steal_table->mutex_.try_lock()) {
                    if (steal_table->number_of_pending_writes_ == 0 &&
                        !steal_table->is_immutable_ &&
                        steal_table->memtable_size_ >
                        0.5 * options_.write_buffer_size) {
                        number_of_steals_ += 1;
                        steal_table->is_immutable_ = true;
                        RDMA_LOG(rdmaio::DEBUG)
                            << fmt::format(
                                    "db[{}]: Steal memtable {} from range {}",
                                    dbid_,
                                    steal_table->memtable_->memtableid(),
                                    steal_from_range->dbid_);
                        steal_from_range->active_memtables_.erase(
                                steal_from_range->active_memtables_.begin() +
                                memtable_index);

                        steal_from_range->number_of_active_memtables_ -= 1;
                        steal_from_range->number_of_immutable_memtables_ += 1;

                        steal_from_range->ScheduleBGTask(-1,
                                                         steal_table->memtable_,
                                                         nullptr, 0, 0,
                                                         options.rand_seed);
                        steal_success = true;
                    }
                    steal_table->mutex_.unlock();
                }
            }
            steal_from_range->range_lock_.Unlock();
            if (steal_success) {
                break;
            }
        }
    }

    bool DBImpl::WriteStaticPartition(const leveldb::WriteOptions &options,
                                      const leveldb::Slice &key,
                                      const leveldb::Slice &value,
                                      uint32_t partition_id,
                                      bool should_wait,
                                      uint64_t last_sequence,
                                      SubRange *subrange) {
        MemTablePartition *partition = partitioned_active_memtables_[partition_id];
        partition->mutex.Lock();
        if (subrange != nullptr) {
            subrange->ninserts += 1;
        }

        MemTable *table = nullptr;
        bool wait = false;
        bool schedule_compaction = false;
        int next_imm_slot = -1;
        while (true) {
            table = partition->memtable;
            next_imm_slot = -1;
            if (table->ApproximateMemoryUsage() <= options_.write_buffer_size) {
                break;
            } else {
                // The table is full.
                if (!partition->available_slots.empty()) {
                    next_imm_slot = partition->available_slots.front();
                    partition->available_slots.pop();
                }
                if (next_imm_slot == -1) {
                    // We have filled up all memtables, but the previous
                    // one is still being compacted, so we wait.
                    if (!should_wait) {
                        partition->mutex.Unlock();
                        return false;
                    }

                    Log(options_.info_log,
                        "Current memtable full; Make room waiting... pid-%u-tid-%lu\n",
                        partition_id, options.thread_id);
                    // Try a different table.
                    partition->background_work_finished_signal_.Wait();
                    if (!wait) {
                        number_of_puts_wait_ += 1;
                    }
                    wait = true;
                } else {
                    // Create a new table.
                    RDMA_ASSERT(partitioned_imms_[next_imm_slot] == 0);
                    partitioned_imms_[next_imm_slot] = table->memtableid();
                    uint32_t memtable_id = memtable_id_seq_.fetch_add(1);
                    partition->closed_log_files.push_back(table->memtableid());
                    table = new MemTable(internal_comparator_, memtable_id,
                                         db_profiler_);
                    RDMA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                    versions_->mid_table_mapping_[memtable_id].SetMemTable(
                            table);
                    partition->memtable = table;
                    schedule_compaction = true;
                    number_of_immutable_memtables_.fetch_add(1);// += 1;
                    break;
                }
            }
        }

        if (!wait) {
            number_of_puts_no_wait_ += 1;
        }
        if (wait) {
            Log(options_.info_log,
                "Make room; resuming... pid-%u-tid-%lu\n",
                partition_id, options.thread_id);
        }
        uint32_t memtable_id = table->memtableid();
        table->Add(last_sequence, ValueType::kTypeValue, key, value);
        versions_->mid_table_mapping_[memtable_id].nentries_ += 1;
        if (table_locator_ != nullptr) {
            table_locator_->Insert(key, options.hash, table->memtableid());
        }
        partition->mutex.Unlock();

        if (schedule_compaction) {
            ScheduleBGTask(-1,
                           versions_->mid_table_mapping_[partitioned_imms_[next_imm_slot]].memtable_,
                           nullptr,
                           partition_id,
                           next_imm_slot, options.rand_seed);
        }
        return true;
    }

    Status DBImpl::WriteStaticPartition(const WriteOptions &options,
                                        const Slice &key,
                                        const Slice &val) {
        uint64_t last_sequence = versions_->last_sequence_.fetch_add(1);
        uint32_t partition_id =
                rand_r(options.rand_seed) %
                partitioned_active_memtables_.size();
        if (options_.num_memtable_partitions > 1) {
            int tries = 2;
            int i = 0;
            while (i < tries) {
                if (WriteStaticPartition(options, key, val, partition_id, false,
                                         last_sequence, nullptr)) {
                    return Status::OK();
                }
                i++;
                partition_id = (partition_id + 1) %
                               partitioned_active_memtables_.size();
            }
        }
        partition_id =
                (partition_id + 1) % partitioned_active_memtables_.size();
        RDMA_ASSERT(
                WriteStaticPartition(options, key, val, partition_id, true,
                                     last_sequence, nullptr));
        return Status::OK();
    }

    void DBImpl::PerformSubRangeReorganization() {
        subrange_manager_->PerformSubRangeReorganization(processed_writes_);
    }

    Status DBImpl::WriteSubrange(const leveldb::WriteOptions &options,
                                 const leveldb::Slice &key,
                                 const leveldb::Slice &val) {
        uint64_t last_sequence = versions_->last_sequence_.fetch_add(1);
        if (processed_writes_ > SUBRANGE_WARMUP_NPUTS &&
            processed_writes_ % SUBRANGE_REORG_INTERVAL == 0) {
            // wake up reorg thread.
            EnvBGTask task = {};
            task.db = this;
            reorg_thread_->Schedule(task);
        }
        SubRange *subrange = nullptr;
        int subrange_id = subrange_manager_->SearchSubranges(options, key, val,
                                                           &subrange);
        RDMA_ASSERT(subrange_id >= 0);
        RDMA_ASSERT(WriteStaticPartition(options, key, val, subrange_id,
                                         true,
                                         last_sequence,
                                         subrange));
        return Status::OK();
    }

    Status DBImpl::Write(const WriteOptions &options, const Slice &key,
                         const Slice &val) {
        uint64_t last_sequence = versions_->last_sequence_.fetch_add(1);

        std::vector<MemTable *> full_memtables;
        AtomicMemTable *atomic_memtable = nullptr;
        bool wait = false;
        bool all_busy = true;
        bool enable_stickness = true;

        range_lock_.Lock();
        int expected_share =
                round(((double) processed_writes_ /
                       (double) options.total_writes) *
                      nova::NovaConfig::config->num_memtables);
        int actual_share = 0;
        AtomicMemTable *emptiest_memtable = nullptr;
        uint64_t smallest_size = UINT64_MAX;
        int emptiest_index = 0;

        uint32_t atomic_memtable_index = 0;
        while (true) {
            emptiest_memtable = nullptr;
            smallest_size = UINT64_MAX;
            emptiest_index = -1;
            all_busy = true;
            atomic_memtable = nullptr;
            enable_stickness = active_memtables_.size() < 5;

            int number_of_retries = std::min((size_t) 3,
                                             active_memtables_.size());
            RDMA_ASSERT(full_memtables.empty());
            atomic_memtable_index = 0;
            if (!enable_stickness) {
                atomic_memtable_index = rand_r(options.rand_seed);
            }

            uint32_t first_memtable_id = 0;
            if (!active_memtables_.empty()) {
                first_memtable_id = active_memtables_[0]->memtable_->memtableid();
            }
            for (int i = 0; i < number_of_retries; i++) {
                atomic_memtable_index =
                        (atomic_memtable_index + 1) % active_memtables_.size();
                if (i == 1 && enable_stickness) {
                    atomic_memtable_index = rand_r(options.rand_seed) %
                                            active_memtables_.size();
                    if (active_memtables_[atomic_memtable_index]->memtable_->memtableid() ==
                        first_memtable_id) {
                        atomic_memtable_index =
                                (atomic_memtable_index + 1) %
                                active_memtables_.size();
                    }
                }

                RDMA_ASSERT(atomic_memtable_index < active_memtables_.size())
                    << fmt::format("{} {} {} {}", atomic_memtable_index,
                                   active_memtables_.size(),
                                   i, number_of_retries);

                atomic_memtable = active_memtables_[atomic_memtable_index];
                RDMA_ASSERT(atomic_memtable);

                uint64_t ms = atomic_memtable->nentries_;
                if (ms < smallest_size &&
                    !atomic_memtable->is_immutable_) {
                    emptiest_memtable = atomic_memtable;
                    smallest_size = ms;
                    emptiest_index = atomic_memtable_index;
                }

                if (!atomic_memtable->mutex_.try_lock()) {
                    atomic_memtable = nullptr;
                    continue;
                }
                all_busy = false;
                RDMA_ASSERT(atomic_memtable->memtable_);
                RDMA_ASSERT(!atomic_memtable->is_flushed_);
                if (atomic_memtable->memtable_size_ >
                    options_.write_buffer_size ||
                    atomic_memtable->is_immutable_) {
                    atomic_memtable->is_immutable_ = true;

                    if (emptiest_index == atomic_memtable_index) {
                        smallest_size = UINT64_MAX;
                        emptiest_index = -1;
                        emptiest_memtable = nullptr;
                    }

                    if (atomic_memtable->number_of_pending_writes_ == 0) {
                        full_memtables.push_back(atomic_memtable->memtable_);
                        closed_memtable_log_files_.push_back(
                                atomic_memtable->memtable_->memtableid());
                        active_memtables_.erase(
                                active_memtables_.begin() +
                                atomic_memtable_index);

                        number_of_active_memtables_ -= 1;
                        number_of_immutable_memtables_ += 1;

                        if (atomic_memtable_index < emptiest_index) {
                            emptiest_index -= 1;
                        }
                    }
                    atomic_memtable->mutex_.unlock();
                    atomic_memtable = nullptr;
                    continue;
                }
                break;
            }

            if (atomic_memtable) {
                RDMA_ASSERT(atomic_memtable->memtable_->memtableid() ==
                            active_memtables_[atomic_memtable_index]->memtable_->memtableid());
                number_of_puts_no_wait_ += 1;
                range_lock_.Unlock();
                break;
            }

            actual_share = number_of_active_memtables_ +
                           number_of_immutable_memtables_;

            if (all_busy && actual_share >= expected_share &&
                !active_memtables_.empty()) {
                number_of_wait_due_to_contention_ += 1;
                // wait on another random table.
                atomic_memtable_index =
                        (atomic_memtable_index + 1) % active_memtables_.size();
                atomic_memtable = active_memtables_[atomic_memtable_index];
                RDMA_ASSERT(atomic_memtable);

                atomic_memtable->mutex_.lock();
                RDMA_ASSERT(atomic_memtable->memtable_);
                RDMA_ASSERT(!atomic_memtable->is_flushed_);

                if (atomic_memtable->memtable_size_ >
                    options_.write_buffer_size ||
                    atomic_memtable->is_immutable_) {
                    atomic_memtable->is_immutable_ = true;

                    if (emptiest_index == atomic_memtable_index) {
                        smallest_size = UINT64_MAX;
                        emptiest_index = -1;
                        emptiest_memtable = nullptr;
                    }

                    if (atomic_memtable->number_of_pending_writes_ == 0) {
                        full_memtables.push_back(atomic_memtable->memtable_);
                        closed_memtable_log_files_.push_back(
                                atomic_memtable->memtable_->memtableid());
                        active_memtables_.erase(
                                active_memtables_.begin() +
                                atomic_memtable_index);

                        number_of_active_memtables_ -= 1;
                        number_of_immutable_memtables_ += 1;

                        if (atomic_memtable_index < emptiest_index) {
                            emptiest_index -= 1;
                        }
                    }
                    atomic_memtable->mutex_.unlock();
                    atomic_memtable = nullptr;
                }
            }

            if (atomic_memtable) {
                RDMA_ASSERT(atomic_memtable->memtable_->memtableid() ==
                            active_memtables_[atomic_memtable_index]->memtable_->memtableid());
                range_lock_.Unlock();
                break;
            }

            // is full.
            bool has_available_memtable = false;
            bool pin = false;

            RDMA_ASSERT(number_of_available_pinned_memtables_ >= 0);
            if (number_of_available_pinned_memtables_ > 0) {
                has_available_memtable = true;
                pin = true;
                number_of_available_pinned_memtables_--;
            } else {
                if (actual_share < expected_share) {
                    options_.memtable_pool->mutex_.lock();
                    if (options_.memtable_pool->num_available_memtables_ > 0) {
                        has_available_memtable = true;
                        options_.memtable_pool->num_available_memtables_ -= 1;
                    }
                    options_.memtable_pool->mutex_.unlock();
                }
            }

            if (has_available_memtable) {
                number_of_active_memtables_ += 1;
                uint32_t memtable_id = memtable_id_seq_.fetch_add(1);
                MemTable *new_table = new MemTable(internal_comparator_,
                                                   memtable_id,
                                                   db_profiler_);
                if (pin) {
                    new_table->is_pinned_ = true;
                }
                RDMA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                versions_->mid_table_mapping_[memtable_id].SetMemTable(
                        new_table);
                atomic_memtable = &versions_->mid_table_mapping_[memtable_id];
                active_memtables_.push_back(atomic_memtable);

                atomic_memtable->mutex_.lock();
                range_lock_.Unlock();
                break;
            } else {
                if (nova::NovaConfig::config->num_memtables >
                    2 * dbs_.size()) {
                    StealMemTable(options);
                }
                if (emptiest_memtable) {
                    RDMA_ASSERT(emptiest_index >= 0);
                    atomic_memtable = emptiest_memtable;

                    atomic_memtable->mutex_.lock();
                    RDMA_ASSERT(atomic_memtable->memtable_);
                    RDMA_ASSERT(!atomic_memtable->is_flushed_);
                    RDMA_ASSERT(!active_memtables_.empty());
                    RDMA_ASSERT(emptiest_index < active_memtables_.size())
                        << fmt::format("{} {} {}",
                                       atomic_memtable->memtable_->memtableid(),
                                       emptiest_index,
                                       active_memtables_.size());

                    if (atomic_memtable->memtable_size_ >
                        options_.write_buffer_size ||
                        atomic_memtable->is_immutable_) {
                        atomic_memtable->is_immutable_ = true;

                        if (atomic_memtable->number_of_pending_writes_ == 0) {
                            full_memtables.push_back(
                                    atomic_memtable->memtable_);
                            closed_memtable_log_files_.push_back(
                                    atomic_memtable->memtable_->memtableid());
                            active_memtables_.erase(
                                    active_memtables_.begin() +
                                    emptiest_index);

                            number_of_active_memtables_ -= 1;
                            number_of_immutable_memtables_ += 1;
                        }

                        atomic_memtable->mutex_.unlock();
                        atomic_memtable = nullptr;

                        smallest_size = UINT64_MAX;
                        emptiest_index = -1;
                        emptiest_memtable = nullptr;
                    }
                }

                for (auto imm : full_memtables) {
                    ScheduleBGTask(-1, imm, nullptr, 0, 0, options.rand_seed);
                }
                full_memtables.clear();

                if (atomic_memtable) {
                    range_lock_.Unlock();
                    break;
                }
                number_of_puts_wait_++;
                RDMA_LOG(rdmaio::DEBUG)
                    << fmt::format("db[{}]: Insert {} wait for pool",
                                   dbid_, key.ToString());
                Log(options_.info_log,
                    "Current memtable full; Make room waiting... tid-%lu\n",
                    options.thread_id);
                wait = true;
                memtable_available_signal_.Wait();
            }
        }

        if (wait) {
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("db[{}]: Insert {} resume",
                               dbid_, key.ToString());
            Log(options_.info_log,
                "Make room; resuming... tid-%lu\n", options.thread_id);
        }


        RDMA_ASSERT(atomic_memtable);
        RDMA_ASSERT(!atomic_memtable->is_immutable_);
        RDMA_ASSERT(!atomic_memtable->is_flushed_);
        RDMA_ASSERT(atomic_memtable->memtable_);
        atomic_memtable->memtable_size_ += (key.size() + val.size());

        if (nova::NovaConfig::config->log_record_mode ==
            nova::NovaLogRecordMode::LOG_RDMA && !options.local_write) {
            // this memtable is selected. Replicate the log records first.
            // Increment the pending writes counter.
            atomic_memtable->number_of_pending_writes_ += 1;
            atomic_memtable->mutex_.unlock();

            auto dc = reinterpret_cast<leveldb::NovaBlockCCClient *>(options.dc_client);
            RDMA_ASSERT(dc);
            dc->set_dbid(dbid_);
            LevelDBLogRecord log_record = {};
            log_record.sequence_number = last_sequence;
            log_record.key = key;
            log_record.value = val;
            options.dc_client->InitiateReplicateLogRecords(
                    fmt::format("{}-{}-{}", server_id_, dbid_,
                                atomic_memtable->memtable_->memtableid()),
                    options.thread_id, dbid_,
                    atomic_memtable->memtable_->memtableid(),
                    options.rdma_backing_mem, log_record,
                    options.replicate_log_record_states);
            dc->Wait();
            atomic_memtable->mutex_.lock();
            atomic_memtable->number_of_pending_writes_ -= 1;
        }

        atomic_memtable->memtable_->Add(last_sequence, ValueType::kTypeValue,
                                        key, val);
        atomic_memtable->nentries_ += 1;
        if (table_locator_ != nullptr) {
            table_locator_->Insert(key, options.hash,
                                   atomic_memtable->memtable_->memtableid());
        }
        uint32_t full_memtable_id = 0;
        if (atomic_memtable->number_of_pending_writes_ == 0) {
            if (atomic_memtable->memtable_size_ >
                options_.write_buffer_size ||
                atomic_memtable->is_immutable_) {
                atomic_memtable->is_immutable_ = true;
                full_memtable_id = atomic_memtable->memtable_->memtableid();
            }
        }
        atomic_memtable->mutex_.unlock();

        if (full_memtable_id != 0) {
            range_lock_.Lock();
            for (int i = 0; i < active_memtables_.size(); i++) {
                if (active_memtables_[i]->memtable_->memtableid() ==
                    full_memtable_id) {
                    atomic_memtable_index = i;

                    full_memtables.push_back(atomic_memtable->memtable_);
                    closed_memtable_log_files_.push_back(
                            atomic_memtable->memtable_->memtableid());
                    active_memtables_.erase(
                            active_memtables_.begin() + atomic_memtable_index);
                    number_of_active_memtables_ -= 1;
                    number_of_immutable_memtables_ += 1;
                    break;
                }
            }
            range_lock_.Unlock();
        }

        for (auto imm : full_memtables) {
            ScheduleBGTask(-1, imm, nullptr, 0, 0, options.rand_seed);
        }
        return Status::OK();
    }

    void DBImpl::QueryDBStats(leveldb::DBStats *db_stats) {
        Version *current = nullptr;
        uint32_t vid = 0;
        while (current == nullptr) {
            vid = versions_->current_version_id();
            RDMA_ASSERT(vid < MAX_LIVE_MEMTABLES) << vid;
            current = versions_->versions_[vid].Ref();
        }
        RDMA_ASSERT(current->version_id() == vid);
        if (options_.enable_subranges) {
            SubRanges *ref = subrange_manager_->latest_subranges_;
            db_stats->num_major_reorgs = subrange_manager_->num_major_reorgs;
            db_stats->num_minor_reorgs = subrange_manager_->num_minor_reorgs;
            db_stats->num_skipped_major_reorgs = subrange_manager_->num_skipped_major_reorgs;
            db_stats->num_skipped_minor_reorgs = subrange_manager_->num_skipped_minor_reorgs;
            db_stats->num_minor_reorgs_for_dup = subrange_manager_->num_minor_reorgs_for_dup;

            if (nova::NovaConfig::config->client_access_pattern == "uniform") {
                uint64_t lower = 0;
                uint64_t upper = 0;
                uint64_t keys = 0;
                uint64_t fair_nkeys_per_subrange =
                        10000000.0 / options_.num_memtable_partitions;
                uint64_t max_keys = 0;
                for (int i = 0; i < ref->subranges.size(); i++) {
                    SubRange &sr = ref->subranges[i];
                    nova::str_to_int(sr.lower.data(), &lower, sr.lower.size());
                    nova::str_to_int(sr.upper.data(), &upper, sr.upper.size());

                    keys = upper - lower;
                    if (sr.upper_inclusive) {
                        keys += 1;
                    }
                    if (!sr.lower_inclusive) {
                        keys -= 1;
                    }
                    max_keys = std::max(max_keys, keys);
                }
                db_stats->maximum_load_imbalance =
                        ((double) max_keys / (double) fair_nkeys_per_subrange) *
                        100.0 - 100.0;
            } else {
                // Zipfian.
                uint64_t lower = 0;
                uint64_t upper = 0;
                uint64_t fair_naccesses_per_subrange =
                        nova::NovaConfig::config->zipfian_dist.sum /
                        options_.num_memtable_partitions;
                uint64_t max_accesses = 0;
                std::map<uint64_t, uint64_t> duplicated_keys;
                for (int i = 0; i < ref->subranges.size(); i++) {
                    SubRange &as = ref->subranges[i];
                    if (as.num_duplicates == 0) {
                        continue;
                    }
                    uint64_t lower;
                    uint64_t upper;
                    nova::str_to_int(as.lower.data(), &lower, as.lower.size());
                    nova::str_to_int(as.upper.data(), &upper, as.upper.size());
                    if (!as.lower_inclusive) {
                        lower++;
                    }
                    if (!as.upper_inclusive) {
                        upper -= 1;
                    }
                    for (uint64_t k = lower; k <= upper; k++) {
                        duplicated_keys[k] += 1;
                    }
                }
                for (int i = 0; i < ref->subranges.size(); i++) {
                    uint64_t accesses = 0;
                    SubRange &sr = ref->subranges[i];
                    nova::str_to_int(sr.lower.data(), &lower, sr.lower.size());
                    nova::str_to_int(sr.upper.data(), &upper, sr.upper.size());

                    if (!sr.upper_inclusive) {
                        upper -= 1;
                    }
                    if (!sr.lower_inclusive) {
                        lower += 1;
                    }
                    for (uint64_t key = lower; key <= upper; key++) {
                        if (duplicated_keys.find(key) !=
                            duplicated_keys.end()) {
                            accesses +=
                                    nova::NovaConfig::config->zipfian_dist.accesses[key] /
                                    duplicated_keys[key];
                        } else {
                            accesses += nova::NovaConfig::config->zipfian_dist.accesses[key];
                        }
                    }
                    max_accesses = std::max(max_accesses, accesses);
                }
                db_stats->maximum_load_imbalance =
                        ((double) max_accesses /
                         (double) fair_naccesses_per_subrange) *
                        100.0 - 100.0;
            }
        }
        current->QueryStats(db_stats);
        versions_->versions_[vid].Unref(dbname_);
    }

    bool DBImpl::GetProperty(const Slice &property, std::string *value) {
        value->clear();

        MutexLock l(&mutex_);
        Slice in = property;
        Slice prefix("leveldb.");
        if (!in.starts_with(prefix)) return false;
        in.remove_prefix(prefix.size());

        if (in.starts_with("num-files-at-level")) {
            in.remove_prefix(strlen("num-files-at-level"));
            uint64_t level;
            bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
            if (!ok || level >= config::kNumLevels) {
                return false;
            } else {
                char buf[100];
                snprintf(buf, sizeof(buf), "%d",
                         versions_->NumLevelFiles(static_cast<int>(level)));
                *value = buf;
                return true;
            }
        } else if (in == "stats") {
            return true;
        } else if (in == "sstables") {
            *value = versions_->current()->DebugString();
            return true;
        } else if (in == "approximate-memory-usage") {
            return true;
        }

        return false;
    }

    void
    DBImpl::GetApproximateSizes(const Range *range, int n, uint64_t *sizes) {
        // TODO(opt): better implementation
        MutexLock l(&mutex_);
        Version *v = versions_->current();
        for (int i = 0; i < n; i++) {
            // Convert user_key into a corresponding internal key.
            InternalKey k1(range[i].start, kMaxSequenceNumber,
                           kValueTypeForSeek);
            InternalKey k2(range[i].limit, kMaxSequenceNumber,
                           kValueTypeForSeek);
            uint64_t start = versions_->ApproximateOffsetOf(v, k1);
            uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
            sizes[i] = (limit >= start ? limit - start : 0);
        }
    }

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
    Status
    DB::Put(const WriteOptions &opt, const Slice &key, const Slice &value) {
        return Write(opt, key, value);
    }

    Status DB::Delete(const WriteOptions &opt, const Slice &key) {
        return Write(opt, key, Slice());
    }

    DB::~DB() = default;

    Status
    DB::Open(const Options &options, const std::string &dbname, DB **dbptr) {
        *dbptr = nullptr;
        DBImpl *impl = new DBImpl(options, dbname);
        impl->mutex_.Lock();
        impl->server_id_ = nova::NovaConfig::config->my_server_id;

        if (!options.debug) {
            std::string manifest_file_name = DescriptorFileName(dbname, 0);
            impl->manifest_file_ = new NovaCCMemFile(options.env,
                                                     impl->options_, 0,
                                                     options.mem_manager,
                                                     options.dc_client,
                                                     impl->dbname_, 0,
                                                     options.max_file_size,
                                                     &impl->rand_seed_,
                                                     manifest_file_name);
        }


        for (int i = 0; i < MAX_LIVE_MEMTABLES; i++) {
            impl->versions_->mid_table_mapping_[i].nentries_ = 0;
        }

        if (options.memtable_type == MemTableType::kMemTablePool) {
            for (int i = 0; i < impl->min_memtables_; i++) {
                uint32_t memtable_id = impl->memtable_id_seq_.fetch_add(1);
                MemTable *new_table = new MemTable(impl->internal_comparator_,
                                                   memtable_id,
                                                   impl->db_profiler_);
                new_table->is_pinned_ = true;
                RDMA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                impl->versions_->mid_table_mapping_[memtable_id].SetMemTable(
                        new_table);
                impl->active_memtables_.push_back(
                        &impl->versions_->mid_table_mapping_[memtable_id]);
                RDMA_ASSERT(
                        options.memtable_pool->num_available_memtables_ >= 1);
                options.memtable_pool->num_available_memtables_ -= 1;

            }
            options.memtable_pool->range_cond_vars_[impl->dbid_] = &impl->memtable_available_signal_;
            impl->number_of_active_memtables_ = impl->min_memtables_;
        } else {
            impl->partitioned_active_memtables_.resize(
                    options.num_memtable_partitions);
            impl->partitioned_imms_.resize(options.num_memtables -
                                           options.num_memtable_partitions);

            for (int i = 0; i < impl->partitioned_imms_.size(); i++) {
                impl->partitioned_imms_[i] = 0;
            }

            uint32_t nslots = (options.num_memtables -
                               options.num_memtable_partitions) /
                              options.num_memtable_partitions;
            uint32_t remainder = (options.num_memtables -
                                  options.num_memtable_partitions) %
                                 options.num_memtable_partitions;
            uint32_t slot_id = 0;
            for (int i = 0; i < options.num_memtable_partitions; i++) {
                uint64_t memtable_id = impl->memtable_id_seq_.fetch_add(1);

                MemTable *table = new MemTable(impl->internal_comparator_,
                                               memtable_id,
                                               impl->db_profiler_);
                RDMA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                impl->versions_->mid_table_mapping_[memtable_id].SetMemTable(
                        table);
                impl->partitioned_active_memtables_[i] = new MemTablePartition;
                impl->partitioned_active_memtables_[i]->memtable = table;
                impl->partitioned_active_memtables_[i]->partition_id = i;
                uint32_t slots = nslots;
                if (remainder > 0) {
                    slots += 1;
                    remainder--;
                }
                impl->partitioned_active_memtables_[i]->imm_slots.resize(slots);
                for (int j = 0; j < slots; j++) {
                    impl->partitioned_active_memtables_[i]->imm_slots[j] =
                            slot_id + j;
                    impl->partitioned_active_memtables_[i]->available_slots.push(
                            slot_id + j);
                }
                slot_id += slots;
            }
            RDMA_ASSERT(slot_id == options.num_memtables -
                                   options.num_memtable_partitions);
            slot_id = 0;
            for (int i = 0; i < options.num_memtable_partitions; i++) {
                for (int j = 0; j <
                                impl->partitioned_active_memtables_[i]->imm_slots.size(); j++) {
                    RDMA_ASSERT(slot_id ==
                                impl->partitioned_active_memtables_[i]->imm_slots[j]);
                    slot_id++;
                }
            }
            impl->number_of_active_memtables_ = impl->partitioned_active_memtables_.size();
        }

        impl->number_of_available_pinned_memtables_ = 0;
        impl->number_of_memtable_hits_ = 0;
        impl->number_of_gets_ = 0;
        impl->number_of_wait_due_to_contention_ = 0;
        impl->number_of_steals_ = 0;
        impl->number_of_immutable_memtables_ = 0;
        impl->processed_writes_ = 0;
        impl->number_of_puts_no_wait_ = 0;
        impl->number_of_puts_wait_ = 0;

        if (options.enable_subranges) {
            impl->subrange_manager_ = new SubRangeManager(impl->manifest_file_,
                                                      dbname,
                                                      impl->versions_,
                                                      impl->options_,
                                                      impl->user_comparator_,
                                                      &impl->partitioned_active_memtables_,
                                                      &impl->partitioned_imms_);
        }
        impl->mutex_.Unlock();
        *dbptr = impl;
        return Status::OK();
    }

    Snapshot::~Snapshot() = default;

    Status DestroyDB(const std::string &dbname, const Options &options) {
        Env *env = options.env;
        std::vector<std::string> filenames;
        Status result = env->GetChildren(dbname, &filenames);
        if (!result.ok()) {
            // Ignore error in case directory does not exist
            return Status::OK();
        }

        FileLock *lock;
        const std::string lockname = LockFileName(dbname);
        result = env->LockFile(lockname, &lock);
        if (result.ok()) {
            uint64_t number;
            FileType type;
            for (size_t i = 0; i < filenames.size(); i++) {
                if (ParseFileName(filenames[i], &number, &type) &&
                    type != kDBLockFile) {  // Lock file will be deleted at end
                    Status del = env->DeleteFile(dbname + "/" + filenames[i]);
                    if (result.ok() && !del.ok()) {
                        result = del;
                    }
                }
            }
            env->UnlockFile(lock);  // Ignore error since state is already gone
            env->DeleteFile(lockname);
            env->DeleteDir(
                    dbname);  // Ignore error in case dir contains other files
        }
        return result;
    }

}  // namespace leveldb