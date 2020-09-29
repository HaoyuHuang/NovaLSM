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
#include "ltc/storage_selector.h"

namespace leveldb {
    namespace {
        uint64_t time_diff(timeval t1, timeval t2) {
            return (t2.tv_sec - t1.tv_sec) * 1000000 +
                   (t2.tv_usec - t1.tv_usec);
        }
    }

    const int kNumNonTableCacheFiles = 10;

    std::string ReconstructReplicasStats::DebugString() const {
        return fmt::format("{},{},{},{},{},{},{},{}",
                           total_failed_metafiles_bytes,
                           total_failed_datafiles_bytes,
                           total_num_failed_metafiles,
                           total_num_failed_datafiles, recover_metafiles_bytes,
                           recover_datafiles_bytes, recover_num_metafiles,
                           recover_num_datafiles);
    }

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
              options_(SanitizeOptions(dbname, &internal_comparator_, &internal_filter_policy_, raw_options)),
              owns_info_log_(options_.info_log != raw_options.info_log),
              owns_cache_(options_.block_cache != raw_options.block_cache),
              dbname_(dbname),
              db_profiler_(new DBProfiler(raw_options.enable_tracing, raw_options.trace_file_path)),
              table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_), db_profiler_)),
              db_lock_(nullptr),
              shutting_down_(false),
              seed_(0),
              manual_compaction_(nullptr),
              versions_(new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_)),
              bg_compaction_threads_(raw_options.bg_compaction_threads),
              bg_flush_memtable_threads_(raw_options.bg_flush_memtable_threads),
              reorg_thread_(raw_options.reorg_thread),
              compaction_coordinator_thread_(raw_options.compaction_coordinator_thread),
              memtable_available_signal_(&range_lock_),
              l0_stop_write_signal_(&l0_stop_write_mutex_),
              user_comparator_(raw_options.comparator) {
        is_loading_db_ = false;
        memtable_id_seq_ = 100;
        start_coordinated_compaction_ = false;
        terminate_coordinated_compaction_ = false;
        start_compaction_ = true;
        if (options_.enable_lookup_index) {
            lookup_index_ = new LookupIndex(
                    options_.upper_key - options_.lower_key);
            for (int i = 0; i < options_.upper_key - options_.lower_key; i++) {
                lookup_index_->Insert(Slice(), i, 0);
            }
        }
        nova::ParseDBIndexFromDBName(dbname_, &dbid_);
    }

    DBImpl::~DBImpl() {
        // Wait for background work to finish.
        if (db_lock_ != nullptr) {
//            env_->UnlockFile("");
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
        table_cache_->Evict(file_number, false);
    }

    void DBImpl::DeleteObsoleteVersions(leveldb::EnvBGThread *bg_thread) {
        mutex_.AssertHeld();
        versions_->DeleteObsoleteVersions();
    }

    void DBImpl::ObtainLookupIndexEdits(leveldb::CompactionState *state,
                                        std::unordered_map<uint32_t, leveldb::MemTableL0FilesEdit> *memtableid_l0fns) {
        auto c = state->compaction;
        if (c->level() != 0) {
            return;
        }
        if (c->target_level() == 0) {
            // Merge wide L0 SSTables.
            // For each table Id: remove the input file. add all output files.
            for (int i = 0; i < c->inputs_[0].size(); i++) {
                auto f = c->inputs_[0][i];
                for (auto memtableid : f->memtable_ids) {
                    (*memtableid_l0fns)[memtableid].dbid_ = dbid_;
                    (*memtableid_l0fns)[memtableid].remove_fns.insert(f->number);
                    for (const auto &out : state->outputs) {
                        (*memtableid_l0fns)[memtableid].add_fns.insert(out.number);
                    }
                }
            }

            // for each output, add input memtableids to its memtableids.
            for (FileMetaData &out : state->outputs) {
                for (int i = 0; i < c->inputs_[0].size(); i++) {
                    auto input = c->inputs_[0][i];
                    out.memtable_ids.insert(input->memtable_ids.begin(), input->memtable_ids.end());
                }
            }
        }
        if (c->target_level() == 1) {
            // Compact L0 SSTables to L1.
            for (int i = 0; i < c->inputs_[0].size(); i++) {
                auto f = c->inputs_[0][i];
                NOVA_ASSERT(f->memtable_ids.size() > 0);
                for (auto memtableid : f->memtable_ids) {
                    (*memtableid_l0fns)[memtableid].dbid_ = dbid_;
                    NOVA_ASSERT(memtableid < MAX_LIVE_MEMTABLES);
                    (*memtableid_l0fns)[memtableid].remove_fns.insert(f->number);
                }
            }
        }
        if (c->target_level() == 0 || c->target_level() == 1) {
            for (int i = 0; i < c->inputs_[1].size(); i++) {
                auto f = c->inputs_[1][i];
                if (f->memtable_ids.empty()) {
                    continue;
                }
                for (auto memtableid : f->memtable_ids) {
                    (*memtableid_l0fns)[memtableid].dbid_ = dbid_;
                    NOVA_ASSERT(memtableid < MAX_LIVE_MEMTABLES);
                    (*memtableid_l0fns)[memtableid].remove_fns.insert(f->number);
                }
            }
        }
    }

    void DBImpl::UpdateFileMetaReplicaLocations(
            const std::vector<leveldb::ReplicationPair> &results, uint32_t stoc_server_id, int level,
            StoCClient *client) {
        std::unordered_map<uint64_t, FileMetaData> fn_meta_to_update;
        mutex_.Lock();
        Version *current = versions_->current();
        NOVA_ASSERT(current);
        uint32_t missing_fns = 0;
        uint32_t updated_fns = 0;
        VersionEdit edit;
        edit.SetUpdateReplicaLocations(true);
        for (auto result : results) {
            if (current->fn_files_.find(result.sstable_file_number) == current->fn_files_.end()) {
                missing_fns += 1;
                continue;
            }
            updated_fns += 1;
            auto metadata = current->fn_files_[result.sstable_file_number];
            if (fn_meta_to_update.find(metadata->number) == fn_meta_to_update.end()) {
                fn_meta_to_update[metadata->number] = *metadata;
            }
            FileMetaData &new_meta = fn_meta_to_update[metadata->number];
            // Update.
            NOVA_ASSERT(result.replica_id < new_meta.block_replica_handles.size());
            FileReplicaMetaData &replica = new_meta.block_replica_handles[result.replica_id];
            if (result.internal_type == FileInternalType::kFileMetadata) {
                replica.meta_block_handle.server_id = result.dest_stoc_id;
                replica.meta_block_handle.stoc_file_id = result.dest_stoc_file_id;
            } else {
                replica.data_block_group_handles[0].server_id = result.dest_stoc_id;
                replica.data_block_group_handles[0].stoc_file_id = result.dest_stoc_file_id;
            }
        }

        for (const auto &it : fn_meta_to_update) {
            auto metadata = it.second;
            edit.AddFile(level, metadata.memtable_ids, metadata.number, metadata.file_size,
                         metadata.converted_file_size, metadata.flush_timestamp, metadata.smallest,
                         metadata.largest, metadata.block_replica_handles, metadata.parity_block_handle);
        }

        NOVA_LOG(rdmaio::INFO)
            << fmt::format("DB[{}]: Update replica location: missing:{}. Updated:{}", dbid_, missing_fns, updated_fns);
        // Include the latest version.
        Version *v = new Version(&internal_comparator_, table_cache_, &options_,
                                 versions_->version_id_seq_.fetch_add(1), versions_);
        Status s = versions_->LogAndApply(&edit, v, true, client);
        NOVA_ASSERT(s.ok());

        if (range_index_manager_) {
            RangeIndexVersionEdit range_edit = {};
            range_edit.lsm_version_id = v->version_id();
            range_index_manager_->AppendNewVersion(&scan_stats, range_edit);
            range_index_manager_->DeleteObsoleteVersions();
        }
        mutex_.Unlock();
        versions_->AppendChangesToManifest(&edit, manifest_file_, options_.manifest_stoc_ids);
    }

    uint32_t DBImpl::EncodeDBMetadata(char *buf, nova::StoCInMemoryLogFileManager *log_manager, uint32_t cfg_id) {
        // dump the latest version, subranges, log files, range index, lookup index, and table id mapping.
        uint32_t msg_size = 1 + 4 + 4 + 4 + 8 + 8 + 8;
        // Lock the database and all memtable partitions.
        // This ensures a consistent snapshot of the database.
        mutex_.Lock();
        NOVA_LOG(rdmaio::DEBUG) << fmt::format("Acquire db lock {}", dbid_);
        for (int i = 0; i < partitioned_active_memtables_.size(); i++) {
            partitioned_active_memtables_[i]->mutex.Lock();
            NOVA_LOG(rdmaio::DEBUG
            ) << fmt::format("Acquire db lock {}:{}", dbid_, i);
        }

        AtomicVersion *atomic_version = versions_->versions_[versions_->current_version_id()];
        Version *v = atomic_version->Ref();
        // Version
        uint32_t version_size = v->Encode(buf + msg_size);
        msg_size += version_size;
        // Subranges
        SubRanges *srs = subrange_manager_->latest_subranges_;
        uint32_t srs_size = srs->Encode(buf + msg_size);
        msg_size += srs_size;

        // memtables
        uint32_t memtables_size = EncodeMemTablePartitions(buf + msg_size);
        msg_size += memtables_size;
        // log files
        uint32_t logfile_size = log_manager->EncodeLogFiles(buf + msg_size, dbid_);
        msg_size += logfile_size;

        // lookup index
        uint32_t lookup_index_size = lookup_index_->Encode(buf + msg_size);
        msg_size += lookup_index_size;
        uint32_t tableid_mapping_size = versions_->EncodeTableIdMapping(
                buf + msg_size, memtable_id_seq_);
        msg_size += tableid_mapping_size;

        // range index
        auto range_index = range_index_manager_->current();
        uint32_t range_index_size = range_index->Encode(buf + msg_size);
        NOVA_LOG(rdmaio::INFO) << fmt::format("Version: {}", v->DebugString());
        NOVA_LOG(rdmaio::INFO) << fmt::format("SRS: {}", srs->DebugString());
        NOVA_LOG(rdmaio::INFO) << fmt::format("Range index: {}", range_index->DebugString());
        msg_size += range_index_size;
        {
            uint32_t header_size = 1;
            buf[0] = StoCRequestType::LTC_MIGRATION;
            header_size += EncodeFixed32(buf + header_size, cfg_id);
            header_size += EncodeFixed32(buf + header_size, dbid_);
            header_size += EncodeFixed32(buf + header_size, v->version_id_);
            header_size += EncodeFixed64(buf + header_size, versions_->last_sequence_);
            header_size += EncodeFixed64(buf + header_size, versions_->next_file_number_);
            header_size += EncodeFixed64(buf + header_size, memtable_id_seq_);
        }
        for (int i = 0; i < partitioned_active_memtables_.size(); i++) {
            partitioned_active_memtables_[i]->mutex.Unlock();
        }
        mutex_.Unlock();
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("{}-{}: v:{} srs:{} mp:{} log:{} lookupidx:{} tid:{} rangeidx:{} {} {} {} {}", msg_size,
                           options_.max_stoc_file_size, version_size, srs_size, memtables_size, logfile_size,
                           lookup_index_size, tableid_mapping_size, range_index_size, dbid_, v->version_id_,
                           versions_->last_sequence_, versions_->next_file_number_);
        NOVA_ASSERT(msg_size < options_.max_stoc_file_size)
            << fmt::format("{}-{}: v:{} srs:{} mp:{} log:{} lookupidx:{} tid:{} rangeidx:{}", msg_size,
                           options_.max_stoc_file_size, version_size, srs_size, memtables_size, logfile_size,
                           lookup_index_size, tableid_mapping_size, range_index_size);
        return msg_size;
    }

    void
    DBImpl::RecoverDBMetadata(const Slice &buf, uint32_t version_id, uint64_t last_sequence, uint64_t next_file_number,
                              uint64_t memtable_id_seq, nova::StoCInMemoryLogFileManager *log_manager,
                              std::unordered_map<uint32_t, leveldb::MemTableLogFilePair> *mid_table_map) {
        Slice tmp = buf;
        uint32_t size = tmp.size();
        memtable_id_seq_ = memtable_id_seq;
        versions_->Restore(&tmp, version_id, last_sequence, next_file_number);
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Decoded {} bytes: db:{}, version: {}", size - tmp.size(), dbid_,
                           versions_->current()->DebugString());
        size = tmp.size();

        SubRanges *srs = new SubRanges;
        NOVA_ASSERT(srs->Decode(&tmp));
        subrange_manager_->latest_subranges_.store(srs);
        subrange_manager_->ComputeCompactionThreadsAssignment(srs);
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Decoded {} bytes: db:{}, SRS: {}", size - tmp.size(), dbid_, srs->DebugString());
        size = tmp.size();

        DecodeMemTablePartitions(&tmp, mid_table_map);
        NOVA_LOG(rdmaio::INFO) << fmt::format("Decoded {} bytes: db:{}, MemtablePartition", size - tmp.size(), dbid_);
        size = tmp.size();

        NOVA_ASSERT(log_manager->DecodeLogFiles(&tmp, mid_table_map));
        NOVA_LOG(rdmaio::INFO) << fmt::format("Decoded {} bytes: db:{}, Log manager", size - tmp.size(), dbid_);
        size = tmp.size();

        lookup_index_->Decode(&tmp);
        NOVA_LOG(rdmaio::INFO) << fmt::format("Decoded {} bytes: db:{}, Lookup index", size - tmp.size(), dbid_);
        size = tmp.size();

        versions_->DecodeTableIdMapping(&tmp, internal_comparator_, mid_table_map);
        NOVA_LOG(rdmaio::INFO) << fmt::format("Decoded {} bytes: db:{}, TableIdMapping", size - tmp.size(), dbid_);
        size = tmp.size();

        RangeIndex *range_index = new RangeIndex(&scan_stats, 0, versions_->current_version_id());
        range_index->Decode(&tmp);
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Decoded {} bytes: db:{}, Range idx: {}", size - tmp.size(), dbid_,
                           range_index->DebugString());
        size = tmp.size();
        // Remove memtables that do not exist in tableid-mapping.

        for (int j = 0; j < range_index->ranges_.size(); j++) {
            const auto &range = range_index->ranges_[j];
            auto &range_table = range_index->range_tables_[j];
            range_table.memtable_ids.clear();

            for (int i = 0; i < srs->subranges.size(); i++) {
                const auto &sr = srs->subranges[i];
                const auto &partition = partitioned_active_memtables_[i];
                if (range.upper_int() < sr.tiny_ranges[0].lower_int() ||
                    range.lower_int() > sr.tiny_ranges[sr.tiny_ranges.size() - 1].upper_int()) {
                    continue;
                }
                // overlapping.
                if (partition->active_memtable) {
                    range_table.memtable_ids.insert(partition->active_memtable->memtableid());
                }
                for (auto mid : partition->immutable_memtable_ids) {
                    range_table.memtable_ids.insert(mid);
                }
            }
        }

        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Updated db:{}, Range idx: {}", dbid_, range_index->DebugString());

        NOVA_ASSERT(versions_->current_version_id() == range_index->lsm_version_id_)
            << fmt::format("{}-{}", versions_->current_version_id(), range_index->lsm_version_id_);
        NOVA_ASSERT(range_index_manager_);
        range_index_manager_->Initialize(range_index);

        // Mark immutable memtables as immutable.
        for (int i = 0; i < partitioned_active_memtables_.size(); i++) {
            auto partition = partitioned_active_memtables_[i];
            for (int j = 0; j < partition->immutable_memtable_ids.size(); j++) {
                uint32_t mid = partition->immutable_memtable_ids[j];
                versions_->mid_table_mapping_[mid]->is_immutable_ = true;
            }
        }
    }

    void
    DBImpl::UpdateLookupIndex(uint32_t version_id,
                              const std::unordered_map<uint32_t, MemTableL0FilesEdit> &edits) {
        for (const auto &it : edits) {
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("update lookup index mid:{} {}", it.first,
                               it.second.DebugString());
            versions_->mid_table_mapping_[it.first]->UpdateL0Files(version_id, it.second);
        }
    }

    void DBImpl::DeleteFiles(EnvBGThread *bg_thread,
                             std::vector<std::string> &files_to_delete,
                             std::unordered_map<uint32_t, std::vector<SSTableStoCFilePair>> &server_pairs) {
        // While deleting all files unblock other threads. All files being deleted
        // have unique names which will not collide with newly created files and
        // are therefore safe to delete while allowing other threads to proceed.
        for (const std::string &filename : files_to_delete) {
            env_->DeleteFile(dbname_ + "/" + filename);
        }
        for (auto &it : server_pairs) {
            bg_thread->stoc_client()->InitiateDeleteTables(it.first,
                                                           it.second);
            for (auto &tble : it.second) {
                NOVA_LOG(rdmaio::DEBUG)
                    << fmt::format("Delete File {} {}", tble.stoc_file_id,
                                   tble.sstable_name);
            }
        }
    }

    void DBImpl::ObtainObsoleteFiles(EnvBGThread *bg_thread,
                                     std::vector<std::string> *files_to_delete,
                                     std::unordered_map<uint32_t, std::vector<SSTableStoCFilePair>> *server_pairs,
                                     uint32_t compacting_version_id) {
        mutex_.AssertHeld();
        if (!bg_error_.ok()) {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return;
        }
        // Make a set of all of the live files
        std::set<uint64_t> live;
        versions_->AddLiveFiles(&live, compacting_version_id);
        auto it = compacted_tables_.begin();
        uint32_t success = 0;
        while (it != compacted_tables_.end()) {
            uint64_t fn = it->first;
            FileMetaData &meta = it->second;
            if (live.find(fn) != live.end()) {
                // Do not remove if it is still alive.
                it++;
                continue;
            }
            // The file can be deleted.
            table_cache_->Evict(meta.number, false);
            ObtainStoCFilesOfSSTable(files_to_delete, server_pairs, meta);
            success += 1;
            it = compacted_tables_.erase(it);
        }
        if (success) {
            NOVA_LOG(rdmaio::INFO)
                << fmt::format("Delete files. Success:{} Failed:{}", success,
                               compacted_tables_.size());
        }
    }

    void DBImpl::ObtainStoCFilesOfSSTable(std::vector<std::string> *files_to_delete,
                                          std::unordered_map<uint32_t, std::vector<SSTableStoCFilePair>> *server_pairs,
                                          const FileMetaData &meta) const {
        // Delete metadata file.
        for (int replica_id = 0; replica_id <
                                 nova::NovaConfig::config->number_of_sstable_metadata_replicas; replica_id++) {
            SSTableStoCFilePair pair = {};
            pair.sstable_name = TableFileName(this->dbname_, meta.number, FileInternalType::kFileMetadata, replica_id);
            pair.stoc_file_id = meta.block_replica_handles[replica_id].meta_block_handle.stoc_file_id;
            (*server_pairs)[meta.block_replica_handles[replica_id].meta_block_handle.server_id].push_back(pair);
        }
        // Delete data files.
        for (int replica_id = 0; replica_id <
                                 nova::NovaConfig::config->number_of_sstable_data_replicas; replica_id++) {
            auto handles = meta.block_replica_handles[replica_id].data_block_group_handles;
            for (int i = 0; i < handles.size(); i++) {
                SSTableStoCFilePair pair = {};
                pair.sstable_name = TableFileName(this->dbname_, meta.number, FileInternalType::kFileData, replica_id);
                pair.stoc_file_id = handles[i].stoc_file_id;
                (*server_pairs)[handles[i].server_id].push_back(pair);
            }
            files_to_delete->push_back(
                    TableFileName(this->dbname_, meta.number, FileInternalType::kFileData, replica_id));
        }
        // Delete parity file.
        if (nova::NovaConfig::config->use_parity_for_sstable_data_blocks) {
            auto handle = meta.parity_block_handle;
            SSTableStoCFilePair pair = {};
            pair.sstable_name = TableFileName(this->dbname_, meta.number, FileInternalType::kFileParity, 0);
            pair.stoc_file_id = handle.stoc_file_id;
            (*server_pairs)[handle.server_id].push_back(pair);
        }
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
        NOVA_LOG(rdmaio::INFO)
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
            Slice slice(buf, nova::NovaConfig::config->max_stoc_file_size);
            leveldb::LevelDBLogRecord record;
            while (nova::DecodeLogRecord(&slice, &record)) {
                memtable->Add(record.sequence_number,
                              leveldb::ValueType::kTypeValue, record.key, record.value);
                recovered_log_records += 1;
                max_sequence_number = std::max(max_sequence_number,
                                               record.sequence_number);
            }
        }
        NOVA_ASSERT(memtables_.size() == log_replicas_.size());

        timeval end{};
        gettimeofday(&end, nullptr);
        recovery_time = time_diff(start, end);
    }

    Status DBImpl::Recover() {
        timeval start = {};
        gettimeofday(&start, nullptr);
        uint32_t stoc_id = options_.manifest_stoc_ids[0];
        std::string manifest = DescriptorFileName(dbname_, 0, 0);
        auto client = reinterpret_cast<StoCBlockClient *> (options_.stoc_client);
        uint32_t manifest_file_size = nova::NovaConfig::config->manifest_file_size;
        uint32_t scid = options_.mem_manager->slabclassid(0, manifest_file_size);
        char *buf = options_.mem_manager->ItemAlloc(0, scid);
        NOVA_ASSERT(buf);
        memset(buf, 0, manifest_file_size);
        StoCBlockHandle handle = {};
        handle.server_id = stoc_id;
        handle.stoc_file_id = 0;
        handle.offset = 0;
        handle.size = manifest_file_size;

        NOVA_LOG(rdmaio::INFO) << fmt::format(
                    "Recover the latest verion from manifest file {} at StoC-{}, off:{}",
                    manifest, stoc_id, (uint64_t) buf);
        uint32_t req_id = client->InitiateReadDataBlock(handle, 0,
                                                        manifest_file_size,
                                                        buf,
                                                        manifest_file_size,
                                                        manifest, false);
        client->Wait();
        StoCResponse response;
        NOVA_ASSERT(client->IsDone(req_id, &response, nullptr));

        std::unordered_map<std::string, uint64_t> logfile_buf;
//        rdma::CCFragment *frag = rdma::NovaConfig::config->db_fragment[dbid_];
//        if (!frag->log_replica_stoc_ids.empty()) {
//            uint32_t stoc_server_id = rdma::NovaConfig::config->stoc_servers[frag->log_replica_stoc_ids[0]].server_id;
//            RDMA_LOG(rdmaio::INFO) << fmt::format(
//                        "Recover the latest memtables from log files at StoC-{}",
//                        stoc_server_id);
//            client->InitiateQueryLogFile(stoc_server_id,
//                                         rdma::NovaConfig::config->my_server_id,
//                                         dbid_, &logfile_buf);
//            client->Wait();UpdateFileMetaReplicaLocations
//        }
        for (auto &it : logfile_buf) {
            NOVA_LOG(rdmaio::INFO) << fmt::format("log file {}:{}", it.first, it.second);
        }
        // Recover log records.
        std::vector<SubRange> subrange_edits;
        NOVA_ASSERT(versions_->Recover(Slice(buf, manifest_file_size), &subrange_edits).ok());
        NOVA_LOG(rdmaio::INFO) << fmt::format("Recovered Version: {}", versions_->current()->DebugString());
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Recovered lsn:{} lfn:{}", versions_->last_sequence_, versions_->NextFileNumber());
        versions_->last_sequence_.fetch_add(1);

        // Inform all StoCs of the mapping between a file and stoc file id.
        const std::vector<std::vector<FileMetaData *>> &files = versions_->current()->files_;
        std::unordered_map<uint32_t, std::unordered_map<std::string, uint32_t>> stoc_fn_stocfileid;
        for (int level = 0; level < options_.level; level++) {
            for (int i = 0; i < files[level].size(); i++) {
                auto meta = files[level][i];
                for (int replica_id = 0; replica_id < meta->block_replica_handles.size(); replica_id++) {
                    std::string metafilename = TableFileName(dbname_, meta->number, FileInternalType::kFileMetadata,
                                                             replica_id);
                    auto meta_handle = meta->block_replica_handles[replica_id].meta_block_handle;
                    stoc_fn_stocfileid[meta_handle.server_id][metafilename] = meta_handle.stoc_file_id;
                }

                if (nova::NovaConfig::config->number_of_sstable_data_replicas == 1) {
                    std::string filename = TableFileName(dbname_, meta->number, FileInternalType::kFileData, 0);
                    for (auto &data_block : meta->block_replica_handles[0].data_block_group_handles) {
                        stoc_fn_stocfileid[data_block.server_id][filename] = data_block.stoc_file_id;
                    }
                } else {
                    for (int replica_id = 0; replica_id < meta->block_replica_handles.size(); replica_id++) {
                        std::string filename = TableFileName(dbname_, meta->number, FileInternalType::kFileData,
                                                             replica_id);
                        for (auto &data_block : meta->block_replica_handles[replica_id].data_block_group_handles) {
                            stoc_fn_stocfileid[data_block.server_id][filename] = data_block.stoc_file_id;
                        }
                    }
                }
                if (meta->parity_block_handle.stoc_file_id != 0) {
                    std::string filename = TableFileName(dbname_, meta->number, FileInternalType::kFileParity, 0);
                    stoc_fn_stocfileid[meta->parity_block_handle.server_id][filename] = meta->parity_block_handle.stoc_file_id;
                }
            }
        }

        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Recover Start Install FileStoCFile mapping size:{}", stoc_fn_stocfileid.size());
        for (const auto &mapping : stoc_fn_stocfileid) {
            NOVA_LOG(rdmaio::INFO)
                << fmt::format("Recover Install FileStoCFile mapping {} size:{}", mapping.first, mapping.second.size());
            auto all_mapping_it = mapping.second.begin();
            std::unordered_map<std::string, uint32_t> fnstocfid;
            while (all_mapping_it != mapping.second.end()) {
                if (fnstocfid.size() == 2000) {
                    client->InitiateInstallFileNameStoCFileMapping(mapping.first, fnstocfid);
                    client->Wait();
                    fnstocfid.clear();
                }
                fnstocfid[all_mapping_it->first] = all_mapping_it->second;
                all_mapping_it++;
            }
            if (!fnstocfid.empty()) {
                client->InitiateInstallFileNameStoCFileMapping(mapping.first, fnstocfid);
                client->Wait();
            }
        }

        // Fetch metadata blocks.
        std::vector<const FileMetaData *> meta_files;
        for (int level = 0; level < options_.level; level++) {
            for (int i = 0; i < files[level].size(); i++) {
                meta_files.push_back(files[level][i]);
            }
        }
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Recover Start Fetching meta blocks size:{}",
                           meta_files.size());
        FetchMetadataFilesInParallel(meta_files, dbname_, options_, client, env_);
        // Rebuild lookup index.
        ReadOptions ro;
        ro.mem_manager = options_.mem_manager;
        ro.stoc_client = options_.stoc_client;
        ro.thread_id = 0;
        ro.hash = 0;
        uint32_t memtableid = 0;
        for (int i = 0; i < files[0].size(); i++) {
            auto meta = files[0][i];
            NOVA_LOG(rdmaio::INFO)
                << fmt::format("Recover L0 data file {} ", meta->DebugString());
            auto it = table_cache_->NewIterator(
                    AccessCaller::kCompaction, ro, meta, meta->number,
                    meta->SelectReplica(), 0, meta->converted_file_size);
            it->SeekToFirst();
            while (it->Valid()) {
                uint64_t hash;
                Slice user_key = ExtractUserKey(it->key());
                nova::str_to_int(user_key.data(), &hash, user_key.size());
                if (lookup_index_) {
                    lookup_index_->Insert(user_key, hash, memtableid);
                }

                AtomicMemTable *mem = versions_->mid_table_mapping_[memtableid];
                mem->l0_file_numbers_.insert(meta->number);
                mem->is_immutable_ = true;
                mem->is_flushed_ = true;
                it->Next();
            }
            memtableid++;
            delete it;
            table_cache_->Evict(meta->number, true);
        }

        if (options_.enable_subranges) {
            if (!subrange_edits.empty()) {
                auto new_srs = new SubRanges(subrange_edits);
                new_srs->AssertSubrangeBoundary(user_comparator_);
                subrange_manager_->latest_subranges_.store(new_srs);
                subrange_manager_->ComputeCompactionThreadsAssignment(new_srs);
            }

            auto latest_srs = subrange_manager_->latest_subranges_.load();
            if (latest_srs->subranges.size() < options_.num_memtable_partitions) {
                NOVA_LOG(rdmaio::INFO) << "Construct subranges with uniform distribution.";
                subrange_manager_->ConstructSubrangesWithUniform(user_comparator_);
            }
        }


        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Recovered Subranges: {}", subrange_manager_->latest_subranges_.load()->DebugString());

        if (options_.enable_range_index) {
            RangeIndex *init = new RangeIndex(&scan_stats, 0,
                                              versions_->current_version_id());
            if (options_.enable_subranges) {
                auto new_srs = subrange_manager_->latest_subranges_.load();
                std::string last_key;
                int range_index_id = -1;
                for (int i = 0; i < new_srs->subranges.size(); i++) {
                    const auto &sr = new_srs->subranges[i];
                    if (last_key == sr.tiny_ranges[0].lower) {
                        init->range_tables_[range_index_id].memtable_ids.insert(
                                partitioned_active_memtables_[i]->active_memtable->memtableid());
                        continue;
                    }
                    Range r = {};
                    r.lower = sr.tiny_ranges[0].lower;
                    r.upper = sr.tiny_ranges[sr.tiny_ranges.size() - 1].upper;
                    init->ranges_.push_back(r);
                    RangeTables tables = {};
                    tables.memtable_ids.insert(
                            partitioned_active_memtables_[i]->active_memtable->memtableid());
                    init->range_tables_.push_back(tables);
                    last_key = sr.tiny_ranges[0].lower;
                    range_index_id += 1;
                }
            } else {
                Range r = {};
                r.lower = std::to_string(options_.lower_key);
                r.upper = std::to_string(options_.upper_key);
                init->ranges_.push_back(r);
                RangeTables tables = {};
                for (int i = 0; i < partitioned_active_memtables_.size(); i++) {
                    tables.memtable_ids.insert(
                            partitioned_active_memtables_[i]->active_memtable->memtableid());
                }
                init->range_tables_.push_back(tables);
            }
            range_index_manager_->Initialize(init);
            NOVA_LOG(rdmaio::INFO) << init->DebugString();
        }

        for (const auto &logfile : logfile_buf) {
            uint32_t index = logfile.first.find_last_of('-');
            uint32_t log_file_number = std::stoi(
                    logfile.first.substr(index + 1));
            versions_->MarkFileNumberUsed(log_file_number);
        }
        options_.mem_manager->FreeItem(0, buf, scid);

        uint32_t recovered_log_records = 0;
        timeval rdma_read_complete = {};
        gettimeofday(&rdma_read_complete, nullptr);

        NOVA_ASSERT(RecoverLogFile(logfile_buf, &recovered_log_records, &rdma_read_complete).ok());
        timeval end = {};
        gettimeofday(&end, nullptr);

        NOVA_LOG(rdmaio::INFO)
            << fmt::format("Total recovery duration: {},{},{},{},{}",
                           logfile_buf.size(),
                           recovered_log_records,
                           time_diff(start, rdma_read_complete),
                           time_diff(rdma_read_complete, end),
                           time_diff(start, end));
        return Status::OK();
    }

    Status
    DBImpl::RecoverLogFile(
            const std::unordered_map<std::string, uint64_t> &logfile_buf,
            uint32_t *recovered_log_records,
            timeval *rdma_read_complete) {
//        if (logfile_buf.empty()) {
//            return Status::OK();
//        }
//
//        auto client = reinterpret_cast<StoCBlockClient *> (options_.stoc_client);
//        std::vector<NovaCCRecoveryThread *> recovery_threads;
//        uint32_t memtable_per_thread =
//                logfile_buf.size() / options_.num_recovery_thread;
//        uint32_t remainder = logfile_buf.size() % options_.num_recovery_thread;
//        NOVA_ASSERT(logfile_buf.size() < partitioned_active_memtables_.size());
//        uint32_t memtable_index = 0;
//        uint32_t thread_id = 0;
//        while (memtable_index < logfile_buf.size()) {
//            std::vector<MemTable *> memtables;
//            for (int j = 0; j < memtable_per_thread; j++) {
//                memtables.push_back(
//                        partitioned_active_memtables_[memtable_index]->memtable);
//                memtable_index++;
//            }
//            if (remainder > 0) {
//                memtable_index++;
//                remainder--;
//            }
//            NovaCCRecoveryThread *thread = new NovaCCRecoveryThread(
//                    thread_id, memtables, options_.mem_manager);
//            recovery_threads.push_back(thread);
//            thread_id++;
//        }
//
//        // Pin each recovery thread to a core.
//        std::vector<std::thread> threads;
//        for (int i = 0; i < recovery_threads.size(); i++) {
//            threads.emplace_back(&NovaCCRecoveryThread::Recover,
//                                 recovery_threads[i]);
//            cpu_set_t cpuset;
//            CPU_ZERO(&cpuset);
//            CPU_SET(i, &cpuset);
//            int rc = pthread_setaffinity_np(threads[i].native_handle(),
//                                            sizeof(cpu_set_t), &cpuset);
//            NOVA_ASSERT(rc == 0) << rc;
//        }
//
//        std::vector<char *> rdma_bufs;
//        std::vector<uint32_t> reqs;
//        nova::LTCFragment *frag = nova::NovaConfig::config->cfgs[0]->db_fragment[dbid_];
//        uint32_t stoc_server_id = nova::NovaConfig::config->stoc_servers[frag->log_replica_stoc_ids[0]].server_id;
//        for (auto &replica : logfile_buf) {
//            uint32_t scid = options_.mem_manager->slabclassid(0,
//                                                              options_.max_log_file_size);
//            char *rdma_buf = options_.mem_manager->ItemAlloc(0, scid);
//            NOVA_ASSERT(rdma_buf);
//            rdma_bufs.push_back(rdma_buf);
//            uint32_t reqid = client->InitiateReadInMemoryLogFile(rdma_buf,
//                                                                 stoc_server_id,
//                                                                 replica.second,
//                                                                 options_.max_log_file_size);
//            reqs.push_back(reqid);
//        }
//
//        // Wait for all RDMA READ to complete.
//        for (auto &replica : logfile_buf) {
//            client->Wait();
//        }
//
//        for (int i = 0; i < reqs.size(); i++) {
//            leveldb::StoCResponse response;
//            NOVA_ASSERT(client->IsDone(reqs[i], &response, nullptr));
//        }
//
//        gettimeofday(rdma_read_complete, nullptr);

        // put all rdma foreground to sleep.
//        for (int i = 0; i < rdma_threads_.size(); i++) {
//            auto *thread = reinterpret_cast<nova::RDMAMsgHandler *>(rdma_threads_[i]);
//            thread->should_pause = true;
//        }

        // Divide.
//        NOVA_LOG(rdmaio::INFO)
//            << fmt::format(
//                    "Start recovery: memtables:{} memtable_per_thread:{}",
//                    logfile_buf.size(),
//                    memtable_per_thread);
//
//        std::vector<char *> replicas;
//        thread_id = 0;
//        for (int i = 0; i < logfile_buf.size(); i++) {
//            if (replicas.size() == memtable_per_thread) {
//                recovery_threads[thread_id]->log_replicas_ = replicas;
//                replicas.clear();
//                thread_id += 1;
//            }
//            replicas.push_back(rdma_bufs[i]);
//        }
//
//        if (!replicas.empty()) {
//            recovery_threads[thread_id]->log_replicas_ = replicas;
//        }
//
//        for (int i = 0;
//             i < options_.num_recovery_thread; i++) {
//            sem_post(&recovery_threads[i]->sem_);
//        }
//        NOVA_LOG(rdmaio::INFO)
//            << fmt::format("Start recovery: recovery threads:{}",
//                           recovery_threads.size());
//        for (int i = 0; i < recovery_threads.size(); i++) {
//            threads[i].join();
//        }
//
//        uint32_t log_records = 0;
//        for (int i = 0; i < recovery_threads.size(); i++) {
//            auto recovery = recovery_threads[i];
//            log_records += recovery->recovered_log_records;
//
//            NOVA_LOG(rdmaio::INFO)
//                << fmt::format("recovery duration of {}: {},{},{},{}",
//                               i,
//                               recovery->log_replicas_.size(),
//                               recovery->recovered_log_records,
//                               recovery->new_memtable_time,
//                               recovery->recovery_time);
//        }
//        *recovered_log_records = log_records;
//
//        uint64_t max_sequence = 0;
//        for (auto &recovery : recovery_threads) {
//            max_sequence = std::max(max_sequence,
//                                    recovery->max_sequence_number);
//        }
//
//        if (versions_->LastSequence() < max_sequence) {
//            versions_->SetLastSequence(max_sequence);
//        }
//
//        for (int i = 0; i < rdma_threads_.size(); i++) {
//            auto *thread = reinterpret_cast<nova::RDMAMsgHandler *>(rdma_threads_[i]);
//            thread->should_pause = false;
//            sem_post(&thread->sem_);
//        }
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
            NOVA_ASSERT(s.ok()) << s.ToString();
            delete iter;
            // Note that if file_size is zero, the file has been deleted and
            // should not be added to the manifest.
            int level = 0;
            if (meta.file_size > 0) {
                NOVA_ASSERT(imm->memtableid() != 0);
                edit.AddFile(level, {imm->memtableid()},
                             meta.number,
                             meta.file_size,
                             meta.converted_file_size,
                             meta.flush_timestamp,
                             meta.smallest,
                             meta.largest,
                             meta.block_replica_handles, meta.parity_block_handle);
            }
        }
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_ids);
        Version *v = new Version(&internal_comparator_, table_cache_, &options_,
                                 versions_->version_id_seq_.fetch_add(1),
                                 versions_);
        mutex_.Lock();
        Status s = versions_->LogAndApply(&edit, v, true);
        NOVA_ASSERT(s.ok());
        mutex_.Unlock();

        std::unordered_map<uint32_t, std::vector<EnvBGTask>> pid_tasks;
        for (auto &task : tasks) {
            pid_tasks[task.memtable_partition_id].push_back(task);
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            auto atomic_imm = versions_->mid_table_mapping_[imm->memtableid()];
            atomic_imm->is_immutable_ = true;
            atomic_imm->SetFlushed(dbname_, {imm->meta().number},
                                   v->version_id());
        }

        for (auto &it : pid_tasks) {
            // New verion is installed. Then remove it from the immutable memtables.
            MemTablePartition *p = partitioned_active_memtables_[it.first];
            p->mutex.Lock();
            bool no_slots = p->available_slots.empty();
            for (auto &task : it.second) {
                NOVA_ASSERT(task.imm_slot < partitioned_imms_.size());
                NOVA_ASSERT(partitioned_imms_[task.imm_slot] != 0);
                partitioned_imms_[task.imm_slot] = 0;
                p->available_slots.push(task.imm_slot);
                NOVA_ASSERT(p->available_slots.size() <= p->imm_slots.size());
            }
            if (no_slots) {
                p->background_work_finished_signal_.SignalAll();
            }
            p->mutex.Unlock();
            number_of_immutable_memtables_.fetch_add(-it.second.size());
        }
    }

    void DBImpl::CompactMemTableStaticPartition(leveldb::EnvBGThread *bg_thread,
                                                const std::vector<leveldb::EnvBGTask> &tasks,
                                                VersionEdit *edit,
                                                bool prune_memtable) {
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            NOVA_ASSERT(imm);
            FileMetaData &meta = imm->meta();
            meta.number = versions_->NewFileNumber();
            meta.flush_timestamp = versions_->last_sequence_;
            meta.level = 0;
            Status s;
            Iterator *iter = imm->NewIterator(TraceType::IMMUTABLE_MEMTABLE, AccessCaller::kCompaction);
            nova::NovaGlobalVariables::global.generated_memtable_sizes += imm->ApproximateMemoryUsage();
            s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta, bg_thread, prune_memtable);
            NOVA_ASSERT(s.ok()) << s.ToString();
            delete iter;
            // Note that if file_size is zero, the file has been deleted and
            // should not be added to the manifest.
            int level = 0;
            NOVA_ASSERT(imm->memtableid() != 0);
            edit->AddFile(level, {imm->memtableid()},
                          meta.number,
                          meta.file_size,
                          meta.converted_file_size,
                          meta.flush_timestamp,
                          meta.smallest,
                          meta.largest,
                          meta.block_replica_handles, meta.parity_block_handle);
            nova::NovaGlobalVariables::global.written_memtable_sizes += meta.file_size;
        }
    }

    bool DBImpl::CompactMultipleMemTablesStaticPartitionToMemTable(
            int partition_id, leveldb::EnvBGThread *bg_thread,
            const std::vector<leveldb::EnvBGTask> &tasks,
            std::vector<uint32_t> *closed_memtable_log_files) {
        std::vector<Iterator *> iterators;
        CompactionStats stats;
        std::set<uint32_t> immids;
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            NOVA_ASSERT(imm);
            Iterator *iter = imm->NewIterator(TraceType::IMMUTABLE_MEMTABLE, AccessCaller::kCompaction);
            iterators.push_back(iter);
            stats.input_source.num_files += 1;
            stats.input_source.file_size += imm->ApproximateMemoryUsage();
            immids.insert(imm->memtableid());
            nova::NovaGlobalVariables::global.generated_memtable_sizes += imm->ApproximateMemoryUsage();
        }
        Iterator *it = NewMergingIterator(&internal_comparator_, &iterators[0], iterators.size());
        SubRanges *subranges = nullptr;
        if (subrange_manager_) {
            subranges = subrange_manager_->latest_subranges_;
        }
        CompactionState *state = new CompactionState(nullptr, subranges, versions_->last_sequence_);
        std::function<uint64_t(void)> fn_generator = std::bind(
                &VersionSet::NewFileNumber, versions_);
        CompactionJob job(fn_generator, env_, dbname_, user_comparator_,
                          options_, bg_thread, table_cache_);
        uint32_t memtable_id = memtable_id_seq_.fetch_add(1);
        MemTable *output_memtable = new MemTable(internal_comparator_, memtable_id,
                                                 db_profiler_, true);
        NOVA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
        auto atomic_output_memtable = versions_->mid_table_mapping_[memtable_id];
        atomic_output_memtable->SetMemTable(flush_order_->latest_generation_id, output_memtable);

        std::vector<LevelDBLogRecord> log_records;
        auto fn_add_to_memtable = [&](const ParsedInternalKey &ikey, const Slice &value) {
            output_memtable->Add(ikey.sequence, ValueType::kTypeValue, ikey.user_key, value);
            if (nova::NovaConfig::config->cfgs.size() == 1) {
                uint64_t key;
                nova::str_to_int(ikey.user_key.data(), &key, ikey.user_key.size());
                uint32_t current_mid = lookup_index_->Lookup(ikey.user_key, key);
                if (immids.find(current_mid) != immids.end()) {
                    lookup_index_->CAS(ikey.user_key, key, current_mid, memtable_id);
                }
            }
            LevelDBLogRecord log_record = {};
            log_record.sequence_number = ikey.sequence;
            log_record.key = ikey.user_key;
            log_record.value = value;
            log_records.push_back(std::move(log_record));
        };
        job.CompactTables(state, it, &stats, true, kCompactInputMemTables, kCompactOutputMemTables, fn_add_to_memtable);
        {
            leveldb::WriteOptions wo;
            wo.stoc_client = bg_thread->stoc_client();
            wo.local_write = false;
            wo.thread_id = bg_thread->thread_id();
            wo.rand_seed = bg_thread->rand_seed();
            wo.hash = 0;
            wo.replicate_log_record_states = new leveldb::StoCReplicateLogRecordState[nova::NovaConfig::config->servers.size()];
            for (int i = 0; i < nova::NovaConfig::config->servers.size(); i++) {
                wo.replicate_log_record_states[i].cfgid = nova::NovaConfig::config->current_cfg_id;
                wo.replicate_log_record_states[i].result = leveldb::StoCReplicateLogRecordResult::REPLICATE_LOG_RECORD_NONE;
                wo.replicate_log_record_states[i].rdma_wr_id = -1;
            }
            uint32_t scid = bg_thread->mem_manager()->slabclassid(0, nova::NovaConfig::config->max_stoc_file_size);
            char *backing_mem = bg_thread->mem_manager()->ItemAlloc(0, scid);
            wo.rdma_backing_mem = backing_mem;
            wo.rdma_backing_mem_size = nova::NovaConfig::config->max_stoc_file_size;
            wo.is_loading_db = false;
            GenerateLogRecord(wo, log_records, output_memtable->memtableid());
            bg_thread->mem_manager()->FreeItem(0, backing_mem, scid);
        }
        iterators.clear();

        // Add output memtable to the memtable partition.
        MemTablePartition *p = partitioned_active_memtables_[partition_id];
        int start_id = 0;

        if (nova::NovaConfig::config->cfgs.size() > 1) {
            p->mutex.Lock();
            {
                if (lookup_index_) {
                    auto new_memtable_it = output_memtable->NewIterator(TraceType::IMMUTABLE_MEMTABLE,
                                                                        AccessCaller::kCompaction);
                    while (new_memtable_it->Valid()) {
                        Slice ukey = ExtractUserKey(new_memtable_it->key());
                        // Update lookup index.
                        uint64_t key;
                        nova::str_to_int(ukey.data(), &key, ukey.size());
                        uint32_t current_mid = lookup_index_->Lookup(ukey, key);
                        if (immids.find(current_mid) != immids.end()) {
                            lookup_index_->CAS(ukey, key, current_mid, memtable_id);
                        }
                        new_memtable_it->Next();
                    }
                    delete new_memtable_it;
                }
            }
        }
        // Update range index.
        if (range_index_manager_) {
            RangeIndexVersionEdit range_edit;
            range_edit.sr = &subranges->subranges[partition_id];
            range_edit.removed_memtables = immids;
            range_edit.new_memtable_id = memtable_id;
            range_edit.add_new_memtable = true;
            range_index_manager_->AppendNewVersion(&scan_stats, range_edit);
            range_index_manager_->DeleteObsoleteVersions();
        }

        // The lookup index is updated before this so that new gets will reference the new memtable.
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            auto atomic_imm = versions_->mid_table_mapping_[imm->memtableid()];
            atomic_imm->is_immutable_ = true;
            atomic_imm->SetFlushed(dbname_, {}, 0);
        }

        if (nova::NovaConfig::config->cfgs.size() == 1) {
            p->mutex.Lock();
        }
        if (!p->active_memtable) {
            p->active_memtable = output_memtable;
        } else {
            p->slot_imm_id[tasks[start_id].imm_slot] = output_memtable->memtableid();
            partitioned_imms_[tasks[start_id].imm_slot] = output_memtable->memtableid();
            start_id++;
        }
        p->AddMemTable(atomic_output_memtable->generation_id_, output_memtable->memtableid());
        bool no_slots = p->available_slots.empty();
        for (; start_id < tasks.size(); start_id++) {
            const auto &task = tasks[start_id];
            uint32_t immid = partitioned_imms_[task.imm_slot];
            NOVA_ASSERT(immid != 0);
            auto remove_atomic = versions_->mid_table_mapping_[immid];
            p->RemoveMemTable(remove_atomic->generation_id_.load(), immid);
            NOVA_ASSERT(task.imm_slot < partitioned_imms_.size());

            partitioned_imms_[task.imm_slot] = 0;
            p->available_slots.push(task.imm_slot);
            NOVA_ASSERT(p->available_slots.size() <= p->imm_slots.size());
        }
        number_of_immutable_memtables_.fetch_add(-tasks.size() + start_id);
        for (auto &log_file : p->immutable_memtable_ids) {
            closed_memtable_log_files->push_back(log_file);
        }
        p->immutable_memtable_ids.clear();
        if (no_slots) {
            p->background_work_finished_signal_.SignalAll();
        }
        p->mutex.Unlock();
        delete state;
    }

    bool DBImpl::CompactMemTable(EnvBGThread *bg_thread,
                                 const std::vector<EnvBGTask> &tasks) {
        VersionEdit edit;
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            versions_->mid_table_mapping_[imm->memtableid()]->mutex_.lock();
            NOVA_ASSERT(
                    versions_->mid_table_mapping_[imm->memtableid()]->is_immutable_);
            NOVA_ASSERT(
                    !versions_->mid_table_mapping_[imm->memtableid()]->is_flushed_);
            versions_->mid_table_mapping_[imm->memtableid()]->mutex_.unlock();


            FileMetaData &meta = imm->meta();
            meta.number = versions_->NewFileNumber();
            meta.flush_timestamp = versions_->last_sequence_;
            meta.level = 0;
            Status s;
            Iterator *iter = imm->NewIterator(TraceType::IMMUTABLE_MEMTABLE,
                                              AccessCaller::kCompaction);
            s = BuildTable(dbname_, env_, options_, table_cache_, iter,
                           &meta, bg_thread, false);
            NOVA_ASSERT(s.ok()) << s.ToString();
            delete iter;
            // Note that if file_size is zero, the file has been deleted and
            // should not be added to the manifest.
            int level = 0;
            if (meta.file_size > 0) {
                NOVA_ASSERT(imm->memtableid() != 0);
                edit.AddFile(level, {imm->memtableid()},
                             meta.number,
                             meta.file_size,
                             meta.converted_file_size,
                             meta.flush_timestamp,
                             meta.smallest,
                             meta.largest,
                             meta.block_replica_handles, meta.parity_block_handle);
            }
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format(
                        "db[{}]: !!!!!!!!!!!!!!!!!!!!!!!!!!!! Flush memtable-{}",
                        dbid_, imm->memtableid());
        }

        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_ids);
        Version *v = new Version(&internal_comparator_, table_cache_, &options_,
                                 versions_->version_id_seq_.fetch_add(1),
                                 versions_);
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            versions_->mid_table_mapping_[imm->memtableid()]->SetFlushed(
                    dbname_,
                    {imm->meta().number},
                    v->version_id());
        }
        NOVA_ASSERT(v->version_id() < MAX_LIVE_MEMTABLES);
        mutex_.Lock();
        Status s = versions_->LogAndApply(&edit, v, true);
        NOVA_ASSERT(s.ok());
        mutex_.Unlock();

        uint32_t num_available = 0;
        bool wakeup_all = false;
        range_lock_.Lock();
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            if (imm->is_pinned_) {
                number_of_available_pinned_memtables_++;
                NOVA_ASSERT(number_of_available_pinned_memtables_ <=
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
            nova::NovaLogRecordMode::LOG_RDMA &&
            !closed_memtable_log_files.empty()) {
            std::vector<std::string> logs;
            for (const auto &file : closed_memtable_log_files) {
                logs.push_back(nova::LogFileName(dbid_, file));
            }
            bg_thread->stoc_client()->InitiateCloseLogFiles(logs, dbid_);
        }

        options_.memtable_pool->mutex_.lock();
        options_.memtable_pool->num_available_memtables_ += num_available;
        NOVA_ASSERT(options_.memtable_pool->num_available_memtables_ <
                    nova::NovaConfig::config->num_memtables -
                    nova::NovaConfig::config->cfgs[0]->fragments.size());
        options_.memtable_pool->mutex_.unlock();

        if (wakeup_all) {
            for (int i = 0;
                 i < nova::NovaConfig::config->cfgs[0]->fragments.size(); i++) {
                options_.memtable_pool->range_cond_vars_[i]->SignalAll();
            }
        } else {
            options_.memtable_pool->range_cond_vars_[dbid_]->SignalAll();
        }
        return true;
    }

    void DBImpl::RecordBackgroundError(const Status &s) {
        mutex_.AssertHeld();
        if (bg_error_.ok()) {
            bg_error_ = s;
        }
    }

    void DBImpl::ScheduleCompactionTask(int thread_id, void *compaction) {
        EnvBGTask task = {};
        task.db = this;
        task.compaction_task = compaction;
        if (bg_compaction_threads_[thread_id]->Schedule(task)) {
        }
    }

    void DBImpl::ScheduleFileDeletionTask(int thread_id) {
        if (!start_compaction_) {
            return;
        }
        EnvBGTask task = {};
        task.db = this;
        task.delete_obsolete_files = true;
        if (bg_compaction_threads_[thread_id]->Schedule(task)) {
        }
    }

    void DBImpl::ScheduleFlushMemTableTask(int thread_id, uint32_t memtable_id,
                                           MemTable *imm,
                                           uint32_t partition_id,
                                           uint32_t imm_slot,
                                           unsigned int *rand_seed,
                                           bool merge_memtables_without_flushing) {
        auto atomic_memtable = versions_->mid_table_mapping_[memtable_id];
        if (!flush_order_->IsSafeToFlush(partition_id, atomic_memtable->generation_id_)) {
            return;
        }
        bool FALSE = false;
        if (!atomic_memtable->is_scheduled_for_flushing.compare_exchange_strong(FALSE, true,
                                                                                std::memory_order_seq_cst)) {
            return;
        }
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("flush memtable-{} {} {} {}", memtable_id, partition_id, imm_slot,
                           merge_memtables_without_flushing);
        NOVA_ASSERT(imm) << fmt::format("flush memtable-{} {} {} {}", imm->memtableid(), partition_id, imm_slot,
                                        merge_memtables_without_flushing);;
        EnvBGTask task = {};
        task.db = this;
        task.memtable = imm;
        task.merge_memtables_without_flushing = merge_memtables_without_flushing;
        if (imm) {
            task.memtable_size_mb = imm->ApproximateMemoryUsage() / 1024 / 1024;
        }
        task.memtable_partition_id = partition_id;
        task.imm_slot = imm_slot;
        if (bg_flush_memtable_threads_[thread_id]->Schedule(task)) {
        }
    }

    void DBImpl::PerformCompaction(leveldb::EnvBGThread *bg_thread, const std::vector<EnvBGTask> &tasks) {
        std::vector<EnvBGTask> memtable_tasks;
        std::unordered_map<uint32_t, std::vector<EnvBGTask>> pid_mergable_memtables;
        std::vector<EnvBGTask> sstable_tasks;
        bool delete_obsolete_files = false;
        for (auto &task : tasks) {
            if (task.memtable) {
                if (task.merge_memtables_without_flushing) {
                    pid_mergable_memtables[task.memtable_partition_id].push_back(task);
                    continue;
                }
                memtable_tasks.push_back(task);
            }
            if (task.compaction_task) {
                sstable_tasks.push_back(task);
            }
            if (task.delete_obsolete_files) {
                delete_obsolete_files = true;
            }
        }

        if (!pid_mergable_memtables.empty()) {
            NOVA_ASSERT(options_.enable_subranges);
            NOVA_ASSERT(options_.enable_flush_multiple_memtables);
            NOVA_ASSERT(options_.subrange_no_flush_num_keys > 0);
        }
        std::vector<uint32_t> closed_memtable_log_files;
        VersionEdit edit;
        std::vector<std::string> files_to_delete;
        std::unordered_map<uint32_t, std::vector<SSTableStoCFilePair>> server_pairs;
        NOVA_ASSERT(
                options_.memtable_type == MemTableType::kStaticPartition);
        // flush memtables belong to the same memtable partition.
        for (const auto &it : pid_mergable_memtables) {
            CompactMultipleMemTablesStaticPartitionToMemTable(it.first, bg_thread, it.second,
                                                              &closed_memtable_log_files);
        }
        if (!memtable_tasks.empty()) {
            CompactMemTableStaticPartition(bg_thread, memtable_tasks, &edit, options_.enable_flush_multiple_memtables);
            // Include the latest version.
            versions_->AppendChangesToManifest(&edit, manifest_file_, options_.manifest_stoc_ids);
            std::unordered_map<uint32_t, std::vector<EnvBGTask>> pid_tasks;
            RangeIndexVersionEdit range_edit;
            if (range_index_manager_) {
                range_index_manager_->DeleteObsoleteVersions();
            }
            mutex_.Lock();
            Version *v = new Version(&internal_comparator_, table_cache_, &options_,
                                     versions_->version_id_seq_.fetch_add(1), versions_);
            for (auto &task : memtable_tasks) {
                pid_tasks[task.memtable_partition_id].push_back(task);
                MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
                range_edit.replace_memtables[imm->memtableid()] = imm->meta().number;
                NOVA_ASSERT(imm);
                auto atomic_imm = versions_->mid_table_mapping_[imm->memtableid()];
                atomic_imm->is_immutable_ = true;
                atomic_imm->SetFlushed(dbname_, {imm->meta().number}, v->version_id());
            }
            range_edit.lsm_version_id = v->version_id_;
            Status s = versions_->LogAndApply(&edit, v, true);
            if (range_index_manager_) {
                range_index_manager_->AppendNewVersion(&scan_stats, range_edit);
                range_index_manager_->DeleteObsoleteVersions();
            }
            NOVA_ASSERT(s.ok());
            if (!compacted_tables_.empty()) {
                DeleteObsoleteVersions(bg_thread);
                ObtainObsoleteFiles(bg_thread, &files_to_delete, &server_pairs, 0);
            }
            mutex_.Unlock();

            for (const auto &it : pid_tasks) {
                // New verion is installed. Then remove it from the immutable memtables.
                MemTablePartition *p = partitioned_active_memtables_[it.first];
                p->mutex.Lock();
                bool no_slots = p->available_slots.empty();
                for (auto &task : it.second) {
                    uint32_t immid = partitioned_imms_[task.imm_slot];
                    NOVA_ASSERT(immid != 0);
                    auto remove_atomic = versions_->mid_table_mapping_[immid];
                    p->RemoveMemTable(remove_atomic->generation_id_, immid);
                    NOVA_ASSERT(task.imm_slot < partitioned_imms_.size());
                    partitioned_imms_[task.imm_slot] = 0;
                    p->available_slots.push(task.imm_slot);
                    NOVA_ASSERT(p->available_slots.size() <= p->imm_slots.size());
//                    NOVA_LOG(rdmaio::INFO) << p->DebugString();
                    NOVA_ASSERT(p->slot_imm_id.erase(task.imm_slot) == 1)
                        << fmt::format("{}. {}", task.imm_slot, p->DebugString());
                }
                number_of_immutable_memtables_.fetch_add(-it.second.size());
                for (auto &log_file : p->immutable_memtable_ids) {
                    closed_memtable_log_files.push_back(log_file);
                }
                p->immutable_memtable_ids.clear();
                if (no_slots) {
                    p->background_work_finished_signal_.SignalAll();
                }
                p->mutex.Unlock();
            }
        } else if (delete_obsolete_files) {
            mutex_.Lock();
            if (!compacted_tables_.empty()) {
                DeleteObsoleteVersions(bg_thread);
                ObtainObsoleteFiles(bg_thread, &files_to_delete, &server_pairs, 0);
            }
            if (nova::NovaConfig::config->log_record_mode == nova::NovaLogRecordMode::LOG_NONE &&
                !compacted_tables_.empty()) {
                ScheduleFileDeletionTask(bg_thread->thread_id());
            }
            mutex_.Unlock();
        }
        DeleteFiles(bg_thread, files_to_delete, server_pairs);
        // Delete log files.
        if (nova::NovaConfig::config->log_record_mode == nova::NovaLogRecordMode::LOG_RDMA &&
            !closed_memtable_log_files.empty()) {
            std::vector<std::string> logs;
            for (const auto &file : closed_memtable_log_files) {
                logs.push_back(nova::LogFileName(dbid_, file));
            }
            log_manager_->DeleteLogBuf(logs);
            bg_thread->stoc_client()->InitiateCloseLogFiles(logs, dbid_);
        }
        for (const auto &task : sstable_tasks) {
            CompactionState *state = reinterpret_cast<CompactionState *> (task.compaction_task);
            NOVA_ASSERT(state);
            auto c = state->compaction;
            auto input = c->MakeInputIterator(table_cache_, bg_thread);
            CompactionStats stats = state->BuildStats();
            std::function<uint64_t(void)> fn_generator = std::bind(
                    &VersionSet::NewFileNumber, versions_);
            CompactionJob job(fn_generator, env_, dbname_, user_comparator_, options_, bg_thread, table_cache_);
            Status status = job.CompactTables(state, input, &stats, true, kCompactInputSSTables,
                                              kCompactOutputSSTables);
        }
    }

    bool DBImpl::ComputeCompactions(leveldb::Version *current,
                                    std::vector<leveldb::Compaction *> *compactions,
                                    VersionEdit *edit,
                                    RangeIndexVersionEdit *range_edit,
                                    bool *delete_due_to_low_overlap,
                                    std::unordered_map<uint32_t, leveldb::MemTableL0FilesEdit> *memtableid_l0fns) {
        bool moves = false;
        auto frags = nova::NovaConfig::config->cfgs[0];
        if (nova::NovaConfig::config->cfgs.size() == 1 && frags->fragments.size() == 1 &&
            frags->fragments[frags->fragments.size() - 1]->range.key_end == 1000000000 && is_loading_db_) {
            // 1 TB database with one range.
            std::vector<uint64_t> level_size;
            std::vector<uint64_t> max_level_size;
            level_size.resize(options_.level);
            max_level_size.resize(options_.level);
            std::vector<FileMetaData *> l0_files;
            for (int level = 0; level < options_.level; level++) {
                level_size[level] = 0;
            }
            for (int level = 0; level < options_.level; level++) {
                max_level_size[level] = MaxBytesForLevel(options_, level);
                if (level == 0) {
                    max_level_size[level] = 0;
                }
                if (level == options_.level - 1) {
                    max_level_size[level] = UINT64_MAX;
                }
                for (auto file : current->files_[level]) {
                    if (level == 0) {
                        l0_files.push_back(file);
                    }
                    level_size[level] += file->file_size;
                }
            }
            if (!l0_files.empty()) {
                for (int level = 1; level < options_.level; level++) {
                    // move to 'level'.
                    while (!l0_files.empty() && level_size[level] + l0_files[0]->file_size < max_level_size[level]) {
                        level_size[level] += l0_files[0]->file_size;
                        auto file = l0_files[0];
                        Compaction *compact = new Compaction(current, &internal_comparator_, &options_, 0, level);
                        compact->inputs_[0].push_back(file);
                        compactions->push_back(compact);
                        l0_files.erase(l0_files.begin());
                    }
                }
            }
        } else {
            current->ComputeNonOverlappingSet(compactions, delete_due_to_low_overlap);
        }

        if (NOVA_LOG_LEVEL == rdmaio::DEBUG) {
            std::string debug = "Coordinated compaction picks compaction sets: ";
            for (int i = 0; i < compactions->size(); i++) {
                debug += "set " + std::to_string(i) + ": ";
                debug += (*compactions)[i]->DebugString(user_comparator_);
                debug += "\n";
            }
            NOVA_LOG(rdmaio::DEBUG) << debug;
        }
        {
            std::string reason;
            bool valid = current->AssertNonOverlappingSet(*compactions, &reason);
            NOVA_ASSERT(valid) << fmt::format("assertion failed {}", reason);
        }
        if (compactions->empty()) {
            return false;
        }
        auto it = compactions->begin();
        while (it != compactions->end()) {
            auto c = *it;
            if (!c->IsTrivialMove()) {
                it++;
                continue;
            }
            // Move file to next level
            assert(c->num_input_files(0) == 1);
            assert(c->num_input_files(1) == 0);
            FileMetaData *f = c->input(0, 0);
            edit->DeleteFile(c->level(), f->number);
            edit->AddFile(c->target_level(),
                          {},
                          f->number, f->file_size,
                          f->converted_file_size,
                          f->flush_timestamp,
                          f->smallest,
                          f->largest,
                          f->block_replica_handles, f->parity_block_handle);
            std::string output = fmt::format(
                    "Moved #{}@{} to level-{} {} bytes\n",
                    f->number, c->level(), c->target_level(), f->file_size);
            Log(options_.info_log, "%s", output.c_str());
            NOVA_LOG(rdmaio::DEBUG) << output;

            if (c->level() == 0 && c->target_level() == 1) {
                range_edit->removed_l0_sstables.push_back(f->number);
                // Compact L0 SSTables to L1.
                for (int i = 0; i < c->inputs_[0].size(); i++) {
                    auto f = c->inputs_[0][i];
                    NOVA_ASSERT(f->memtable_ids.size() > 0);
                    for (auto memtableid : f->memtable_ids) {
                        NOVA_ASSERT(memtableid < MAX_LIVE_MEMTABLES);
                        (*memtableid_l0fns)[memtableid].remove_fns.insert(f->number);
                    }
                }
            }
            moves = true;
            delete c;
            it = compactions->erase(it);
        }
        return moves;
    }

    void DBImpl::ScheduleFileDeletionTask() {
        if (terminate_coordinated_compaction_) {
            return;
        }

        mutex_.Lock();
        if (!compacted_tables_.empty()) {
            ScheduleFileDeletionTask(dbid_ % bg_compaction_threads_.size());
        }
        mutex_.Unlock();
    }

    void DBImpl::CoordinateMajorCompaction() {
        while (options_.major_compaction_type == kMajorCoordinated ||
               options_.major_compaction_type == kMajorCoordinatedStoC) {
            if (terminate_coordinated_compaction_) {
                break;
            }

            mutex_.Lock();
            Version *current = versions_->current();
            if (!start_coordinated_compaction_) {
                mutex_.Unlock();
                sleep(1);
                continue;
            }
            if (!current->NeedsCompaction()) {
                mutex_.Unlock();
                sleep(1);
                continue;
            }
            NOVA_ASSERT(versions_->versions_[current->version_id()]->Ref() == current);
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("comv-init {} {}", current->version_id_, current->refs_);
            mutex_.Unlock();

            std::vector<Compaction *> compactions;
            std::vector<uint32_t> reqs;
            std::vector<CompactionRequest *> requests;
            SubRanges *subs = nullptr;
            std::vector<CompactionState *> states;

            sem_t completion_signal;
            std::vector<bool> cleaned;
            sem_init(&completion_signal, 0, 0);
            bool delete_due_to_low_overlap = false;
            {
                VersionEdit edit;
                RangeIndexVersionEdit range_edit;
                std::unordered_map<uint32_t, MemTableL0FilesEdit> edits;
                if (ComputeCompactions(current, &compactions, &edit, &range_edit, &delete_due_to_low_overlap, &edits)) {
                    // Contain moves. Cleanup LSM immediately.
                    CleanupLSMCompaction(nullptr, edit, range_edit, edits, nullptr, current->version_id_);
                }
            }
            if (!compactions.empty()) {
                cleaned.resize(compactions.size());
            }
            mutex_compacting_tables.Lock();
            for (int i = 0; i < compactions.size(); i++) {
                for (int which = 0; which < 2; which++) {
                    for (int j = 0; j < compactions[i]->inputs_[which].size(); j++) {
                        compacting_tables_.insert(compactions[i]->inputs_[which][j]->number);
                    }
                }
            }
            mutex_compacting_tables.Unlock();

            for (int i = 0; i < compactions.size(); i++) {
                compactions[i]->complete_signal_ = &completion_signal;
                cleaned[i] = false;
            }
            if (!compactions.empty()) {
                if (subrange_manager_) {
                    subs = subrange_manager_->latest_subranges_;
                }
                uint64_t smallest_snapshot = versions_->LastSequence();
                for (int i = 0; i < compactions.size(); i++) {
                    auto state = new CompactionState(compactions[i], subs, smallest_snapshot);
                    states.push_back(state);
                }
                if (options_.major_compaction_type == kMajorCoordinated) {
                    for (int i = 0; i < states.size(); i++) {
                        int thread_id =
                                EnvBGThread::bg_compaction_thread_id_seq.fetch_add(
                                        1, std::memory_order_relaxed) %
                                bg_compaction_threads_.size();
                        NOVA_LOG(rdmaio::DEBUG) << fmt::format(
                                    "Coordinator schedules compaction at thread-{}",
                                    thread_id);
                        ScheduleCompactionTask(thread_id, states[i]);
                    }
                    // Wait for majors to complete.
                    for (int i = 0; i < compactions.size(); i++) {
                        sem_wait(&completion_signal);
                        for (int j = 0; j < compactions.size(); j++) {
                            if (cleaned[j]) {
                                continue;
                            }
                            if (compactions[j]->is_completed_) {
                                cleaned[j] = true;
                                {
                                    VersionEdit edit = {};
                                    RangeIndexVersionEdit range_edit = {};
                                    std::unordered_map<uint32_t, MemTableL0FilesEdit> edits;
                                    CleanupLSMCompaction(states[j], edit,
                                                         range_edit,
                                                         edits,
                                                         nullptr,
                                                         current->version_id_);
                                }
                            }
                        }
                    }
                } else {
                    auto client = reinterpret_cast<StoCBlockClient *> (compaction_coordinator_thread_->stoc_client());
                    std::vector<uint32_t> selected_storages;
                    StorageSelector selector(&rand_seed_);
                    selector.SelectAvailableStoCsForCompaction(&selected_storages, compactions.size());
                    NOVA_ASSERT(selected_storages.size() == compactions.size());

                    for (int i = 0; i < compactions.size(); i++) {
                        NOVA_LOG(rdmaio::INFO) << fmt::format(
                                    "Coordinator schedules compaction on StoC-{} {}@{} + {}@{}", selected_storages[i],
                                    compactions[i]->inputs_[0].size(), compactions[i]->level(),
                                    compactions[i]->inputs_[1].size(), compactions[i]->target_level());
                        if (selected_storages[i] == nova::NovaConfig::config->my_server_id) {
                            // Schedule on my server.
                            int thread_id =
                                    EnvBGThread::bg_compaction_thread_id_seq.fetch_add(1, std::memory_order_relaxed) %
                                    bg_compaction_threads_.size();
                            ScheduleCompactionTask(thread_id, states[i]);
                            // A placeholder.
                            reqs.push_back(0);
                            auto req = new CompactionRequest;
                            requests.push_back(req);
                            continue;
                        }
                        auto compaction = compactions[i];
                        auto req = new CompactionRequest;
                        req->completion_signal = &completion_signal;
                        req->source_level = compaction->level();
                        req->target_level = compaction->target_level();
                        req->dbname = dbname_;
                        req->smallest_snapshot = smallest_snapshot;
                        if (subs) {
                            req->subranges = subs->subranges;
                        }
                        for (int which = 0; which < 2; which++) {
                            req->inputs[which] = compaction->inputs_[which];
                        }
                        req->guides = compaction->grandparents_;
                        uint32_t req_id = client->InitiateCompaction(
                                selected_storages[i], req);
                        reqs.push_back(req_id);
                        requests.push_back(req);
                    }
                    // Wait for majors to complete.
                    for (int i = 0; i < compactions.size(); i++) {
                        sem_wait(&completion_signal);

                        // Figure out which one completes.
                        for (int j = 0; j < reqs.size(); j++) {
                            if (cleaned[j]) {
                                continue;
                            }
                            bool completed = false;
                            CompactionRequest *compaction_req = nullptr;
                            if (reqs[j] == 0) {
                                NOVA_ASSERT(
                                        selected_storages[j] ==
                                        nova::NovaConfig::config->my_server_id);
                                if (compactions[j]->is_completed_) {
                                    completed = true;
                                }
                            } else {
                                StoCResponse response;
                                if (client->IsDone(reqs[j], &response,
                                                   nullptr)) {
                                    compaction_req = requests[j];
                                    completed = true;
                                }
                            }
                            if (!completed) {
                                continue;
                            }
                            cleaned[j] = true;
                            {
                                VersionEdit edit = {};
                                RangeIndexVersionEdit range_edit = {};
                                std::unordered_map<uint32_t, MemTableL0FilesEdit> edits;
                                CleanupLSMCompaction(states[j], edit,
                                                     range_edit,
                                                     edits,
                                                     compaction_req,
                                                     current->version_id_);
                            }
                        }
                    }
                    uint64_t input_size = 0;
                    uint64_t output_size = 0;
                    uint32_t ninputs = 0;
                    for (int i = 0; i < states.size(); i++) {
                        auto state = states[i];
                        for (int which = 0; which < 2; which++) {
                            ninputs += state->compaction->inputs_[which].size();
                            for (int j = 0; j <
                                            state->compaction->inputs_[which].size(); j++) {
                                input_size += state->compaction->inputs_[which][j]->file_size;
                            }
                        }
                        for (const auto &out : state->outputs) {
                            output_size += out.file_size;
                        }
                    }
                    input_size = input_size / 1024 / 1024;
                    output_size = output_size / 1024 / 1024;
                    NOVA_LOG(rdmaio::INFO)
                        << fmt::format("parallel,{},{},{},{},{}",
                                       compactions.size(),
                                       ninputs / compactions.size(),
                                       input_size, output_size,
                                       input_size - output_size);
                }
            }
            versions_->versions_[current->version_id()]->Unref(dbname_);

            for (int i = 0; i < cleaned.size(); i++) {
                NOVA_ASSERT(cleaned[i]);
            }
            std::vector<std::string> files_to_delete;
            std::unordered_map<uint32_t, std::vector<SSTableStoCFilePair>> server_pairs;
            mutex_.Lock();
            DeleteObsoleteVersions(compaction_coordinator_thread_);
            ObtainObsoleteFiles(compaction_coordinator_thread_,
                                &files_to_delete, &server_pairs, 0);
            if (range_index_manager_) {
                range_index_manager_->DeleteObsoleteVersions();
            }
            if (!compacted_tables_.empty()) {
                ScheduleFileDeletionTask(dbid_ % bg_compaction_threads_.size());
            }
            mutex_.Unlock();
            DeleteFiles(compaction_coordinator_thread_, files_to_delete, server_pairs);

            mutex_compacting_tables.Lock();
            for (int i = 0; i < compactions.size(); i++) {
                for (int which = 0; which < 2; which++) {
                    for (int j = 0;
                         j < compactions[i]->inputs_[which].size(); j++) {
                        compacting_tables_.erase(
                                compactions[i]->inputs_[which][j]->number);
                    }
                }
            }
            mutex_compacting_tables.Unlock();

            for (auto c : compactions) {
                delete c;
            }
            for (auto state : states) {
                delete state;
            }
            for (auto req : requests) {
                req->FreeMemoryLTC();
                delete req;
            }

            if (delete_due_to_low_overlap && compactions.empty()) {
                sleep(1);
            }
        }
    }

    void DBImpl::CleanupLSMCompaction(CompactionState *state,
                                      VersionEdit &edit,
                                      RangeIndexVersionEdit &range_edit,
                                      std::unordered_map<uint32_t, MemTableL0FilesEdit> &edits,
                                      CompactionRequest *compaction_req,
                                      uint32_t compacting_version_id) {
        std::vector<std::string> files_to_delete;
        std::unordered_map<uint32_t, std::vector<SSTableStoCFilePair> > server_pairs;
        if (compaction_req && state) {
            auto client = reinterpret_cast<StoCBlockClient *> (compaction_coordinator_thread_->stoc_client());
            std::vector<const FileMetaData *> metafiles;
            for (auto f : compaction_req->outputs) {
                state->outputs.push_back(*f);
                metafiles.push_back(f);
            }
            // Prefetch metadata files stored on other servers.
            FetchMetadataFilesInParallel(metafiles, dbname_, options_, client, env_);
        }
        if (state) {
            ObtainLookupIndexEdits(state, &edits);
            InstallCompactionResults(state, &edit, state->compaction->target_level());
            if (state->compaction->level() == 0) {
                if (state->compaction->target_level() == 0) {
                    std::vector<uint64_t> newids;
                    for (int i = 0; i < state->outputs.size(); i++) {
                        newids.push_back(state->outputs[i].number);
                    }
                    for (int i = 0; i < state->compaction->inputs_[0].size(); i++) {
                        uint64_t oldid = state->compaction->inputs_[0][i]->number;
                        range_edit.replace_l0_sstables[oldid] = newids;
                    }
                } else {
                    for (int i = 0; i < state->compaction->inputs_[0].size(); i++) {
                        range_edit.removed_l0_sstables.push_back(state->compaction->inputs_[0][i]->number);
                    }
                }
            }
        }
        NOVA_LOG(rdmaio::DEBUG) << edit.DebugString();
        versions_->AppendChangesToManifest(&edit, manifest_file_, options_.manifest_stoc_ids);
        if (range_index_manager_) {
            range_index_manager_->DeleteObsoleteVersions();
        }
        mutex_.Lock();
        Version *v = new Version(&internal_comparator_,
                                 table_cache_,
                                 &options_,
                                 versions_->version_id_seq_.fetch_add(1),
                                 versions_);
        UpdateLookupIndex(v->version_id_, edits);
        range_edit.lsm_version_id = v->version_id_;
        if (state) {
            versions_->AddCompactedInputs(state->compaction, &compacted_tables_);
        }
        versions_->LogAndApply(&edit, v, true);

        uint32_t skip_compacting_version = compacting_version_id;
        if (!versions_->versions_[compacting_version_id]->SetCompaction()) {
            skip_compacting_version = 0;
        }
        DeleteObsoleteVersions(compaction_coordinator_thread_);
        ObtainObsoleteFiles(compaction_coordinator_thread_, &files_to_delete, &server_pairs, skip_compacting_version);
        if (range_index_manager_) {
            range_index_manager_->AppendNewVersion(&scan_stats, range_edit);
            range_index_manager_->DeleteObsoleteVersions();
        }
        mutex_.Unlock();
        DeleteFiles(compaction_coordinator_thread_, files_to_delete, server_pairs);

        for (int i = 0; i < partitioned_active_memtables_.size(); i++) {
            partitioned_active_memtables_[i]->background_work_finished_signal_.SignalAll();
        }
//        l0_stop_write_mutex_.Lock();
//        l0_stop_write_signal_.SignalAll();
//        l0_stop_write_mutex_.Unlock();
    }

    Status DBImpl::InstallCompactionResults(CompactionState *compact,
                                            VersionEdit *edit,
                                            int target_level) {
        // Add compaction outputs
        if (compact->compaction) {
            compact->compaction->AddInputDeletions(edit);
        }
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const FileMetaData &out = compact->outputs[i];
            edit->AddFile(target_level,
                          out.memtable_ids,
                          out.number,
                          out.file_size,
                          out.converted_file_size,
                          versions_->last_sequence_,
                          out.smallest, out.largest,
                          out.block_replica_handles,
                          out.parity_block_handle);
        }
        return Status::OK();
    }

    namespace {
        struct IterState {
            RangeIndex *const range_index = nullptr;
            VersionSet *const versions_ = nullptr;

            IterState(RangeIndex *_range_index, VersionSet *versions)
                    : range_index(_range_index), versions_(versions) {}
        };

        static void CleanupIteratorState(void *arg1, void *arg2) {
            IterState *state = reinterpret_cast<IterState *>(arg1);
            state->range_index->UnRef();
            state->versions_->versions_[state->range_index->lsm_version_id_]->Unref("");
            delete state;
        }
    }  // anonymous namespace

    Iterator *DBImpl::NewInternalIterator(const ReadOptions &options,
                                          SequenceNumber *latest_snapshot,
                                          uint32_t *seed) {
        *latest_snapshot = versions_->last_sequence_;
        *seed = 0;
        NOVA_ASSERT(range_index_manager_);
        std::vector<Iterator *> list;
        RangeIndex *range_index = range_index_manager_->current();
        NOVA_ASSERT(range_index);
        auto atomic_version = versions_->versions_[range_index->lsm_version_id_];
        NOVA_ASSERT(atomic_version) << range_index->lsm_version_id_;
        atomic_version->version->AddIterators(options, range_index, &list,
                                              &scan_stats);
        Iterator *internal_iter =
                NewMergingIterator(&internal_comparator_, &list[0],
                                   list.size());
        IterState *cleanup = new IterState(range_index, versions_);
        internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);
        return internal_iter;
    }

    Status DBImpl::Get(const ReadOptions &options, const Slice &key,
                       std::string *value) {
        number_of_gets_ += 1;
        if (lookup_index_) {
            if (GetWithLookupIndex(options, key, value).ok()) {
                return Status::OK();
            }
        }
        return GetWithRangeIndex(options, key, value);
    }

    Status
    DBImpl::GetWithRangeIndex(const ReadOptions &options, const Slice &key,
                              std::string *value) {
        SequenceNumber snapshot = kMaxSequenceNumber;
        LookupKey lkey(key, snapshot);
        Status s;
        NOVA_ASSERT(range_index_manager_);
        std::vector<Iterator *> list;
        RangeIndex *range_index = range_index_manager_->current();
        NOVA_ASSERT(range_index);
        auto atomic_version = versions_->versions_[range_index->lsm_version_id_];
        NOVA_ASSERT(atomic_version) << range_index->lsm_version_id_;
        int index = 0;
        NOVA_ASSERT(BinarySearch(range_index->ranges_, key, &index,
                                 user_comparator_));
        const RangeTables &range_table = range_index->range_tables_[index];
        // Search memtables.
        for (uint32_t memtableid : range_table.memtable_ids) {
            versions_->mid_table_mapping_[memtableid]->memtable_->Get(lkey, value, &s);
        }
        std::vector<uint64_t> l0fns;
        l0fns.insert(l0fns.begin(), range_table.l0_sstable_ids.begin(),
                     range_table.l0_sstable_ids.end());
        // Search SSTables.
        SequenceNumber latest_seq = 0;
        atomic_version->version->Get(options, l0fns, lkey, &latest_seq,
                                     value,
                                     &number_of_files_to_search_for_get_);
        Version::GetStats stats = {};
        atomic_version->version->Get(options, lkey, &latest_seq, value,
                                     &stats, GetSearchScope::kL1AndAbove,
                                     &number_of_files_to_search_for_get_);
        range_index->UnRef();
        versions_->versions_[range_index->lsm_version_id_]->Unref(dbname_);
        if (!value->empty()) {
            return Status::OK();
        }
        return Status::NotFound("");
    }

    Status
    DBImpl::GetWithLookupIndex(const ReadOptions &options, const Slice &key,
                               std::string *value) {
        Status s = Status::NotFound(Slice());
        std::string tmp;
        SequenceNumber snapshot = kMaxSequenceNumber;
        AtomicMemTable *memtable = nullptr;
        std::vector<uint64_t> l0fns;

        NOVA_ASSERT(lookup_index_);
        uint32_t memtableid = lookup_index_->Lookup(key, options.hash);
        if (memtableid != 0) {
            NOVA_ASSERT(memtableid < MAX_LIVE_MEMTABLES) << memtableid;
            memtable = versions_->mid_table_mapping_[memtableid]->RefMemTable();
        }
        LookupKey lkey(key, snapshot);
        if (memtable != nullptr) {
            NOVA_ASSERT(memtable->memtable_->memtableid() == memtableid);
//            NOVA_ASSERT()
//                << fmt::format("key:{} memtable:{} s:{}",
//                               key.ToString(),
//                               memtable->memtable_->memtableid(),
//                               s.ToString());

            bool found = memtable->memtable_->Get(lkey, value, &s);
            versions_->mid_table_mapping_[memtableid]->Unref(dbname_);
            if (found) {
                number_of_memtable_hits_ += 1;
                return Status::OK();
            } else {
                return Status::NotFound("");
            }
        }

        Version *current = nullptr;
        uint32_t vid = 0;
        SequenceNumber latest_seq = 0;
        auto atomic_memtable = versions_->mid_table_mapping_[memtableid];
        while (true) {
            current = nullptr;
            l0fns.clear();
            while (current == nullptr) {
                vid = versions_->current_version_id();
                NOVA_ASSERT(vid < MAX_LIVE_MEMTABLES) << vid;
                current = versions_->versions_[vid]->Ref();
            }
            NOVA_ASSERT(current->version_id() == vid);
            atomic_memtable->mutex_.lock();
            if (vid >= atomic_memtable->last_version_id_) {
                // good to go.
                l0fns.insert(l0fns.end(), atomic_memtable->l0_file_numbers_.begin(),
                             atomic_memtable->l0_file_numbers_.end());
                atomic_memtable->mutex_.unlock();
                break;
            }
            // A major compaction is installing a new version. Retry.
            atomic_memtable->mutex_.unlock();
            versions_->versions_[vid]->Unref(dbname_);
        }

        if (!l0fns.empty()) {
            s = current->Get(options, l0fns, lkey, &latest_seq, value, &number_of_files_to_search_for_get_);
        }
        NOVA_ASSERT(!s.IsIOError())
            << fmt::format("v:{} status:{} mid:{} version:{}", vid, s.ToString(), memtableid, current->DebugString());
        if (s.IsNotFound()) {
            // Search L1 files.
            Version::GetStats stats = {};
            SequenceNumber l1seq;
            s = current->Get(options, lkey, &l1seq, value, &stats, GetSearchScope::kL1AndAbove,
                             &number_of_files_to_search_for_get_);
//            if (l1seq > latest_seq) {
//                value->assign(l1val);
//            }
        }
        NOVA_ASSERT(s.ok())
            << fmt::format("key:{} val:{} seq:{} status:{} version:{}",
                           key.ToString(), value->size(), latest_seq,
                           s.ToString(),
                           current->DebugString());
        versions_->versions_[vid]->Unref(dbname_);
        return s;
    }

    void DBImpl::StartTracing() {
        if (db_profiler_) {
            db_profiler_->StartTracing();
        }
    }

    void DBImpl::StartCoordinatedCompaction() {
        start_coordinated_compaction_ = true;
    }

    void DBImpl::StopCoordinatedCompaction() {
        start_coordinated_compaction_ = false;
        terminate_coordinated_compaction_ = true;
    }

    void DBImpl::StopCompaction() {
        start_compaction_ = false;
    }

    Iterator *DBImpl::NewIterator(const ReadOptions &options) {
        scan_stats.number_of_scans_ += 1;
        SequenceNumber latest_snapshot;
        uint32_t seed;
        Iterator *iter = NewInternalIterator(options, &latest_snapshot, &seed);
        return NewDBIterator(this, user_comparator(), iter, latest_snapshot, seed,
                             nova::NovaConfig::config->cfgs[options.cfg_id]->fragments[dbid_]->range);
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
            if (o.is_loading_db || !options_.enable_subranges) {
                return WriteStaticPartition(o, key, val);
            }
            return WriteSubrange(o, key, val);
        }
        return WriteMemTablePool(o, key, val);
    }

    Status DBImpl::Delete(const WriteOptions &options, const Slice &key) {
        return DB::Delete(options, key);
    }

    void DBImpl::StealMemTable(const leveldb::WriteOptions &options) {
        uint32_t number_of_tries = std::min((size_t) 3,
                                            nova::NovaConfig::config->cfgs[0]->fragments.size() -
                                            1);
        uint32_t rand_range_index = rand_r(options.rand_seed) %
                                    nova::NovaConfig::config->cfgs[0]->fragments.size();

        for (int i = 0; i < number_of_tries; i++) {
            // steal a memtable from another range.
            rand_range_index = (rand_range_index + 1) %
                               nova::NovaConfig::config->cfgs[0]->fragments.size();
            if (rand_range_index == dbid_) {
                rand_range_index = (rand_range_index + 1) %
                                   nova::NovaConfig::config->cfgs[0]->fragments.size();
            }

            auto steal_from_range = reinterpret_cast<DBImpl *>(nova::NovaConfig::config->cfgs[0]->fragments[rand_range_index]);
            if (!steal_from_range->range_lock_.TryLock()) {
                continue;
            }
            bool steal_success = false;
            NOVA_LOG(rdmaio::DEBUG)
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
                        NOVA_LOG(rdmaio::DEBUG)
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

                        int thread_id =
                                EnvBGThread::bg_flush_memtable_thread_id_seq.fetch_add(
                                        1,
                                        std::memory_order_relaxed) %
                                bg_flush_memtable_threads_.size();

                        steal_from_range->ScheduleFlushMemTableTask(thread_id,
                                                                    steal_table->memtable_id_,
                                                                    steal_table->memtable_,
                                                                    0, 0,
                                                                    options.rand_seed,
                                                                    false);
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

    uint32_t DBImpl::EncodeMemTablePartitions(char *buf) {
        // All partitions are locked already.
        uint32_t msg_size = 0;
        for (int i = 0; i < partitioned_active_memtables_.size(); i++) {
            MemTablePartition *p = partitioned_active_memtables_[i];
            uint32_t memtableid = 0;
            if (p->active_memtable) {
                memtableid = p->active_memtable->memtableid();
            }
            msg_size += EncodeFixed32(buf + msg_size, memtableid);
            msg_size += EncodeFixed32(buf + msg_size, p->immutable_memtable_ids.size());
            for (auto logfile : p->immutable_memtable_ids) {
                msg_size += EncodeFixed32(buf + msg_size, logfile);
            }
            NOVA_LOG(rdmaio::INFO) << fmt::format("Range {} has active-{} and {} pending log files", dbid_, memtableid,
                                                  p->immutable_memtable_ids.size());
            p->immutable_memtable_ids.clear();
        }
        return msg_size;
    }

    void DBImpl::DecodeMemTablePartitions(Slice *buf,
                                          std::unordered_map<uint32_t, leveldb::MemTableLogFilePair> *mid_table_map) {
        auto srs = subrange_manager_->latest_subranges_.load();
        for (int i = 0; i < partitioned_active_memtables_.size(); i++) {
            MemTablePartition *p = partitioned_active_memtables_[i];
            p->mutex.Lock();
            p->active_memtable = nullptr;
            uint32_t memtableid = 0;
            NOVA_ASSERT(DecodeFixed32(buf, &memtableid));
            if (memtableid != 0) {
                if (nova::NovaConfig::config->ltc_migration_policy == nova::LTCMigrationPolicy::IMMEDIATE) {
                    // Mark this table as immutable.
                    MemTable *table = new MemTable(internal_comparator_, memtableid, nullptr, false);
                    NOVA_ASSERT(!p->available_slots.empty());
                    uint32_t slotid = p->available_slots.front();
                    p->available_slots.pop();
                    partitioned_imms_[slotid] = memtableid;
                    p->immutable_memtable_ids.push_back(memtableid);
                    versions_->mid_table_mapping_[memtableid]->SetMemTable(flush_order_->latest_generation_id, table);

                    MemTableLogFilePair pair = {};
                    pair.is_immutable = true;
                    pair.memtable = table;
                    pair.logfile = nova::LogFileName(dbid_, memtableid);
                    pair.partition_id = i;
                    pair.subrange = &srs->subranges[i];
                    pair.imm_slot = slotid;
                    (*mid_table_map)[memtableid] = pair;
                } else {
                    p->active_memtable = new MemTable(internal_comparator_, memtableid, nullptr, false);
                    versions_->mid_table_mapping_[memtableid]->SetMemTable(flush_order_->latest_generation_id,
                                                                           p->active_memtable);
                    MemTableLogFilePair pair = {};
                    pair.is_immutable = false;
                    pair.memtable = p->active_memtable;
                    pair.logfile = nova::LogFileName(dbid_, memtableid);
                    (*mid_table_map)[memtableid] = pair;
                }
            }
            uint32_t size = 0;
            NOVA_ASSERT(DecodeFixed32(buf, &size));
            NOVA_LOG(rdmaio::INFO) << fmt::format("Decode active memtable {} {}", memtableid, size);
            for (int j = 0; j < size; j++) {
                uint32_t imm_memtableid = 0;
                NOVA_ASSERT(DecodeFixed32(buf, &imm_memtableid));
                MemTable *table = new MemTable(internal_comparator_, imm_memtableid, nullptr, false);
                NOVA_ASSERT(!p->available_slots.empty());
                uint32_t slotid = p->available_slots.front();
                p->available_slots.pop();
                partitioned_imms_[slotid] = imm_memtableid;
                p->immutable_memtable_ids.push_back(imm_memtableid);
                versions_->mid_table_mapping_[imm_memtableid]->SetMemTable(flush_order_->latest_generation_id, table);

                MemTableLogFilePair pair = {};
                pair.is_immutable = true;
                pair.memtable = table;
                pair.logfile = nova::LogFileName(dbid_, memtableid);
                pair.partition_id = i;
                pair.subrange = &srs->subranges[i];
                pair.imm_slot = slotid;
                (*mid_table_map)[memtableid] = pair;
            }

            if (nova::NovaConfig::config->ltc_migration_policy == nova::LTCMigrationPolicy::IMMEDIATE &&
                !p->available_slots.empty()) {
                // Create a new active memtable.
                uint32_t new_memtable_id = memtable_id_seq_.fetch_add(1);
                p->active_memtable = new MemTable(internal_comparator_, new_memtable_id, nullptr, true);
                versions_->mid_table_mapping_[new_memtable_id]->SetMemTable(flush_order_->latest_generation_id,
                                                                            p->active_memtable);
            }
            p->mutex.Unlock();
        }
    }

    uint32_t DBImpl::FlushMemTables(bool flush_active_memtable) {
        NOVA_LOG(rdmaio::INFO) << "Flush memtables";
        struct ImmutableTable {
            int thread_id;
            uint32_t memtable_id;
            MemTable *table;
            uint32_t partition_id;
            uint32_t next_imm_slot;
        };
        std::vector<ImmutableTable> imms;
        for (int partition_id = 0; partition_id < partitioned_active_memtables_.size(); partition_id++) {
            MemTablePartition *partition = partitioned_active_memtables_[partition_id];
            uint32_t next_imm_slot = -1;
            partition->mutex.Lock();
//            NOVA_LOG(rdmaio::INFO) << partition->DebugString();
            MemTable *table = partition->active_memtable;
            if (flush_active_memtable && table) {
                if (versions_->mid_table_mapping_[table->memtableid()]->nentries_ == 0 ||
                    partition->available_slots.empty()) {
                    versions_->mid_table_mapping_[table->memtableid()]->generation_id_.store(
                            flush_order_->latest_generation_id);
                } else {
                    NOVA_ASSERT(!partition->available_slots.empty());
                    next_imm_slot = partition->available_slots.front();
                    partition->available_slots.pop();

                    // Create a new table.
                    NOVA_ASSERT(partitioned_imms_[next_imm_slot] == 0);
                    partitioned_imms_[next_imm_slot] = table->memtableid();
                    partition->slot_imm_id[next_imm_slot] = table->memtableid();
                    partition->immutable_memtable_ids.push_back(table->memtableid());

                    uint32_t new_memtable_id = memtable_id_seq_.fetch_add(1);
                    MemTable *new_table = new MemTable(internal_comparator_, new_memtable_id, db_profiler_, true);
                    NOVA_ASSERT(new_memtable_id < MAX_LIVE_MEMTABLES);
                    uint64_t gen_id = flush_order_->latest_generation_id;
                    versions_->mid_table_mapping_[new_memtable_id]->SetMemTable(gen_id, new_table);
                    partition->active_memtable = new_table;
                    partition->AddMemTable(gen_id, new_memtable_id);
                    number_of_immutable_memtables_.fetch_add(1);
                    bool merge_memtables_without_flushing = false;
                    int thread_id =
                            EnvBGThread::bg_flush_memtable_thread_id_seq.fetch_add(1, std::memory_order_relaxed) %
                            bg_flush_memtable_threads_.size();
                    ImmutableTable imm = {};
                    imm.thread_id = thread_id;
                    imm.memtable_id = table->memtableid();
                    imm.table = table;
                    imm.partition_id = partition_id;
                    imm.next_imm_slot = next_imm_slot;
                    imms.push_back(imm);
                }
            }
            for (const auto &slot_immid : partition->slot_imm_id) {
                NOVA_ASSERT(versions_->mid_table_mapping_[slot_immid.second]);
                auto atomic_memtable = versions_->mid_table_mapping_[slot_immid.second];
                NOVA_ASSERT(partitioned_imms_[slot_immid.first] == atomic_memtable->memtable_id_);
                int thread_id =
                        EnvBGThread::bg_flush_memtable_thread_id_seq.fetch_add(1, std::memory_order_relaxed) %
                        bg_flush_memtable_threads_.size();
                ImmutableTable imm = {};
                imm.thread_id = thread_id;
                imm.memtable_id = slot_immid.second;
                imm.table = atomic_memtable->memtable_;
                imm.partition_id = partition_id;
                imm.next_imm_slot = slot_immid.first;
                imms.push_back(imm);
            }
            partition->mutex.Unlock();
        }
        for (auto &imm : imms) {
            ScheduleFlushMemTableTask(imm.thread_id, imm.memtable_id, imm.table, imm.partition_id, imm.next_imm_slot, &rand_seed_,
                                      false);
        }
        return number_of_immutable_memtables_;
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
            int tinyrange_id;
            NOVA_ASSERT(BinarySearch(subrange->tiny_ranges, key, &tinyrange_id, user_comparator_))
                << fmt::format("key:{} range:{}", key.ToString(), subrange->DebugString());
            subrange->tiny_ranges[tinyrange_id].ninserts++;
        }

        MemTable *table = nullptr;
        bool wait = false;
        int imm_slot;
        uint64_t start_wait = 0;
        while (true) {
            table = partition->active_memtable;
            imm_slot = -1;
            if (table) {
                auto atomic_mem = versions_->mid_table_mapping_[table->memtableid()];
                if (atomic_mem->memtable_size_ <= options_.write_buffer_size) {
                    break;
                }
                bool wait_for_l0 = false;
                while (options_.l0bytes_stop_writes_trigger > 0) {
                    // Get the current version.
                    Version *current = nullptr;
                    while (current == nullptr) {
                        uint32_t vid = versions_->current_version_id();
                        NOVA_ASSERT(vid < MAX_LIVE_MEMTABLES) << vid;
                        current = versions_->versions_[vid]->Ref();
                    }
                    if (current->l0_bytes_ >= options_.l0bytes_stop_writes_trigger) {
                        versions_->versions_[current->version_id_]->Unref(dbname_);
                        // Wait if the L0 bytes exceed max.
                        // The mutex is only needed to protect the conditional varilable.
                        if (start_wait == 0) {
                            start_wait = env_->NowMicros();
                        }
                        partition->background_work_finished_signal_.Wait();
                        wait = true;
                        wait_for_l0 = true;
                        continue;
                    }
                    versions_->versions_[current->version_id_]->Unref(dbname_);
                    break;
                }

                if (wait_for_l0) {
                    // Check if the table is still valid.
                    continue;
                }

                if (atomic_mem->number_of_pending_writes_ > 0) {
                    // Wait until the number of pending writes is 0.
                    partition->background_work_finished_signal_.Wait();
                    continue;
                }

                // The table is full.
                NOVA_ASSERT(!partition->available_slots.empty());
                // Mark the active memtable as immutable and schedule compaction.
                imm_slot = partition->available_slots.front();
                partition->available_slots.pop();
                NOVA_ASSERT(partitioned_imms_[imm_slot] == 0);
                partitioned_imms_[imm_slot] = table->memtableid();
                partition->slot_imm_id[imm_slot] = table->memtableid();
                partition->immutable_memtable_ids.push_back(table->memtableid());
                number_of_immutable_memtables_.fetch_add(1);
                partition->active_memtable = nullptr;
            }
            if (partition->available_slots.empty()) {
                if (imm_slot != -1) {
                    int thread_id = -1;
                    bool merge_memtables_without_flushing = false;
                    if (subrange) {
                        thread_id = subrange->GetCompactionThreadId(
                                &EnvBGThread::bg_flush_memtable_thread_id_seq,
                                &merge_memtables_without_flushing);
                    } else {
                        thread_id =
                                EnvBGThread::bg_flush_memtable_thread_id_seq.fetch_add(
                                        1, std::memory_order_relaxed) %
                                bg_flush_memtable_threads_.size();
                    }
                    ScheduleFlushMemTableTask(thread_id,
                                              partitioned_imms_[imm_slot],
                                              versions_->mid_table_mapping_[partitioned_imms_[imm_slot]]->memtable_,
                                              partition_id, imm_slot,
                                              options.rand_seed,
                                              merge_memtables_without_flushing);
                }
                // We have filled up all memtables, but the previous
                // one is still being compacted, so we wait.
                if (!should_wait) {
                    partition->mutex.Unlock();
                    return false;
                }
                if (start_wait == 0) {
                    start_wait = env_->NowMicros();
                }
                partition->background_work_finished_signal_.Wait();
                wait = true;
            } else {
                // Create a new table.
                uint32_t memtable_id = memtable_id_seq_.fetch_add(1);
                table = new MemTable(internal_comparator_, memtable_id, db_profiler_, true);
                NOVA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                uint64_t generation_id = flush_order_->latest_generation_id;
                versions_->mid_table_mapping_[memtable_id]->SetMemTable(generation_id, table);
                partition->active_memtable = table;
                partition->AddMemTable(generation_id, table->memtableid());

                if (range_index_manager_) {
                    RangeIndexVersionEdit edit;
                    edit.sr = subrange;
                    edit.add_new_memtable = true;
                    edit.new_memtable_id = memtable_id;
                    range_index_manager_->AppendNewVersion(&scan_stats, edit);
                }
                break;
            }
        }
        if (wait) {
            number_of_puts_wait_ += 1;
            uint64_t duration = env_->NowMicros() - start_wait;
            Log(options_.info_log, "%u,%lu\n", partition_id, duration);
        } else {
            number_of_puts_no_wait_ += 1;
        }
        uint32_t memtable_id = table->memtableid();
        auto atomic_mem = versions_->mid_table_mapping_[memtable_id];
        atomic_mem->number_of_pending_writes_ += 1;
        atomic_mem->memtable_size_ += (key.size() + value.size());
        if (nova::NovaConfig::config->log_record_mode ==
            nova::NovaLogRecordMode::LOG_RDMA && !options.local_write) {
            partition->mutex.Unlock();
            GenerateLogRecord(options, last_sequence, key, value, memtable_id);
            partition->mutex.Lock();
        }
        table->Add(last_sequence, ValueType::kTypeValue, key, value);
        atomic_mem->number_of_pending_writes_ -= 1;
        versions_->mid_table_mapping_[memtable_id]->nentries_ += 1;
        if (lookup_index_) {
            lookup_index_->Insert(key, options.hash, table->memtableid());
        }
        if (nova::NovaConfig::config->log_record_mode ==
            nova::NovaLogRecordMode::LOG_RDMA && !options.local_write) {
            if (atomic_mem->number_of_pending_writes_ == 0 &&
                atomic_mem->memtable_size_ > options_.write_buffer_size) {
                // Wake up other threads that are waiting on pending.
                partition->background_work_finished_signal_.SignalAll();
            }
        }
        partition->mutex.Unlock();
        // Schedule.
        if (imm_slot != -1) {
            int thread_id = -1;
            bool merge_memtables_without_flushing = false;
            if (subrange) {
                thread_id = subrange->GetCompactionThreadId(&EnvBGThread::bg_flush_memtable_thread_id_seq,
                                                            &merge_memtables_without_flushing);
            } else {
                thread_id =
                        EnvBGThread::bg_flush_memtable_thread_id_seq.fetch_add(1, std::memory_order_relaxed) %
                        bg_flush_memtable_threads_.size();
            }
            ScheduleFlushMemTableTask(thread_id,
                                      partitioned_imms_[imm_slot],
                                      versions_->mid_table_mapping_[partitioned_imms_[imm_slot]]->memtable_,
                                      partition_id,
                                      imm_slot, options.rand_seed,
                                      merge_memtables_without_flushing);
        }
        return true;
    }

    Status DBImpl::WriteStaticPartition(const WriteOptions &options,
                                        const Slice &key, const Slice &val) {
        uint64_t last_sequence = versions_->last_sequence_.fetch_add(1);
        if (options.is_loading_db) {
            NOVA_ASSERT(WriteStaticPartition(options, key, val, 0, true, last_sequence, nullptr));
            return Status::OK();
        }

        uint32_t partition_id = rand_r(options.rand_seed) % partitioned_active_memtables_.size();
        if (options_.num_memtable_partitions > 1) {
            int tries = 2;
            int i = 0;
            while (i < tries) {
                if (WriteStaticPartition(options, key, val, partition_id, false, last_sequence, nullptr)) {
                    return Status::OK();
                }
                i++;
                partition_id = (partition_id + 1) % partitioned_active_memtables_.size();
            }
        }
        partition_id = (partition_id + 1) % partitioned_active_memtables_.size();
        NOVA_ASSERT(WriteStaticPartition(options, key, val, partition_id, true, last_sequence, nullptr));
        return Status::OK();
    }

    void DBImpl::PerformSubRangeReorganization() {
        if (options_.enable_subrange_reorg) {
            subrange_manager_->ReorganizeSubranges();
        }
    }

    Status DBImpl::WriteSubrange(const leveldb::WriteOptions &options,
                                 const leveldb::Slice &key,
                                 const leveldb::Slice &val) {
        uint64_t last_sequence = versions_->last_sequence_.fetch_add(1);
        if (processed_writes_ > SUBRANGE_WARMUP_NPUTS &&
            processed_writes_ % SUBRANGE_REORG_INTERVAL == 0 &&
            options_.enable_subrange_reorg) {
            // wake up reorg thread.
            EnvBGTask task = {};
            task.db = this;
            reorg_thread_->Schedule(task);
        }
        SubRange *subrange = nullptr;
        int subrange_id = subrange_manager_->SearchSubranges(options, key, val,
                                                             &subrange);
        NOVA_ASSERT(subrange_id >= 0);
        NOVA_ASSERT(WriteStaticPartition(options, key, val, subrange_id,
                                         true,
                                         last_sequence,
                                         subrange));
        return Status::OK();
    }

    Status DBImpl::WriteMemTablePool(const WriteOptions &options,
                                     const Slice &key,
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
            NOVA_ASSERT(full_memtables.empty());
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

                NOVA_ASSERT(atomic_memtable_index < active_memtables_.size())
                    << fmt::format("{} {} {} {}", atomic_memtable_index,
                                   active_memtables_.size(),
                                   i, number_of_retries);

                atomic_memtable = active_memtables_[atomic_memtable_index];
                NOVA_ASSERT(atomic_memtable);

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
                NOVA_ASSERT(atomic_memtable->memtable_);
                NOVA_ASSERT(!atomic_memtable->is_flushed_);
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
                NOVA_ASSERT(atomic_memtable->memtable_->memtableid() ==
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
                NOVA_ASSERT(atomic_memtable);

                atomic_memtable->mutex_.lock();
                NOVA_ASSERT(atomic_memtable->memtable_);
                NOVA_ASSERT(!atomic_memtable->is_flushed_);

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
                NOVA_ASSERT(atomic_memtable->memtable_->memtableid() ==
                            active_memtables_[atomic_memtable_index]->memtable_->memtableid());
                range_lock_.Unlock();
                break;
            }

            // is full.
            bool has_available_memtable = false;
            bool pin = false;

            NOVA_ASSERT(number_of_available_pinned_memtables_ >= 0);
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
                MemTable *new_table = new MemTable(internal_comparator_, memtable_id, db_profiler_, true);
                if (pin) {
                    new_table->is_pinned_ = true;
                }
                NOVA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                versions_->mid_table_mapping_[memtable_id]->SetMemTable(flush_order_->latest_generation_id, new_table);
                atomic_memtable = versions_->mid_table_mapping_[memtable_id];
                active_memtables_.push_back(atomic_memtable);

                atomic_memtable->mutex_.lock();
                range_lock_.Unlock();
                break;
            } else {
                if (nova::NovaConfig::config->num_memtables >
                    2 * nova::NovaConfig::config->cfgs[0]->fragments.size()) {
                    StealMemTable(options);
                }
                if (emptiest_memtable) {
                    NOVA_ASSERT(emptiest_index >= 0);
                    atomic_memtable = emptiest_memtable;

                    atomic_memtable->mutex_.lock();
                    NOVA_ASSERT(atomic_memtable->memtable_);
                    NOVA_ASSERT(!atomic_memtable->is_flushed_);
                    NOVA_ASSERT(!active_memtables_.empty());
                    NOVA_ASSERT(emptiest_index < active_memtables_.size())
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
                    int thread_id =
                            EnvBGThread::bg_flush_memtable_thread_id_seq.fetch_add(
                                    1, std::memory_order_relaxed) %
                            bg_flush_memtable_threads_.size();
                    ScheduleFlushMemTableTask(thread_id, imm->memtableid(), imm, 0, 0,
                                              options.rand_seed, false);
                }
                full_memtables.clear();

                if (atomic_memtable) {
                    range_lock_.Unlock();
                    break;
                }
                number_of_puts_wait_++;
                NOVA_LOG(rdmaio::DEBUG)
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
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("db[{}]: Insert {} resume",
                               dbid_, key.ToString());
            Log(options_.info_log,
                "Make room; resuming... tid-%lu\n", options.thread_id);
        }


        NOVA_ASSERT(atomic_memtable);
        NOVA_ASSERT(!atomic_memtable->is_immutable_);
        NOVA_ASSERT(!atomic_memtable->is_flushed_);
        NOVA_ASSERT(atomic_memtable->memtable_);
        atomic_memtable->memtable_size_ += (key.size() + val.size());

        if (nova::NovaConfig::config->log_record_mode ==
            nova::NovaLogRecordMode::LOG_RDMA && !options.local_write) {
            // this memtable is selected. Replicate the log records first.
            // Increment the pending writes counter.
            atomic_memtable->number_of_pending_writes_ += 1;
            atomic_memtable->mutex_.unlock();
            GenerateLogRecord(options, last_sequence, key, val,
                              atomic_memtable->memtable_->memtableid());
            atomic_memtable->mutex_.lock();
            atomic_memtable->number_of_pending_writes_ -= 1;
        }

        atomic_memtable->memtable_->Add(last_sequence, ValueType::kTypeValue,
                                        key, val);
        atomic_memtable->nentries_ += 1;
        if (lookup_index_) {
            lookup_index_->Insert(key, options.hash,
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
            int thread_id =
                    EnvBGThread::bg_flush_memtable_thread_id_seq.fetch_add(
                            1, std::memory_order_relaxed) %
                    bg_flush_memtable_threads_.size();
            ScheduleFlushMemTableTask(thread_id, imm->memtableid(), imm, 0, 0, options.rand_seed,
                                      false);
        }
        return Status::OK();
    }

    void DBImpl::GenerateLogRecord(const leveldb::WriteOptions &options,
                                   const std::vector<leveldb::LevelDBLogRecord> &log_records,
                                   uint32_t memtable_id) {
        if (nova::NovaConfig::config->log_record_mode ==
            nova::NovaLogRecordMode::LOG_RDMA && !options.local_write) {
            auto stoc = reinterpret_cast<leveldb::StoCBlockClient *>(options.stoc_client);
            NOVA_ASSERT(stoc);
            options.stoc_client->InitiateReplicateLogRecords(
                    nova::LogFileName(dbid_, memtable_id),
                    options.thread_id, dbid_, memtable_id,
                    options.rdma_backing_mem, log_records,
                    options.replicate_log_record_states);
            stoc->Wait();
        }
    }

    void DBImpl::GenerateLogRecord(const WriteOptions &options,
                                   SequenceNumber last_sequence,
                                   const Slice &key, const Slice &val,
                                   uint32_t memtable_id) {
        if (nova::NovaConfig::config->log_record_mode ==
            nova::NovaLogRecordMode::LOG_RDMA && !options.local_write) {
            auto stoc = reinterpret_cast<leveldb::StoCBlockClient *>(options.stoc_client);
            NOVA_ASSERT(stoc);
            LevelDBLogRecord log_record = {};
            log_record.sequence_number = last_sequence;
            log_record.key = key;
            log_record.value = val;
            NOVA_ASSERT(8 + key.size() + val.size() + 4 + 4 + 1 <=
                        options.rdma_backing_mem_size);
            options.stoc_client->InitiateReplicateLogRecords(
                    nova::LogFileName(dbid_, memtable_id),
                    options.thread_id, dbid_, memtable_id,
                    options.rdma_backing_mem, {log_record},
                    options.replicate_log_record_states);
            stoc->Wait();
        }
    }

    const std::string &DBImpl::dbname() {
        return dbname_;
    }

    void DBImpl::QueryFailedReplicas(uint32_t failed_stoc_id,
                                     bool is_stoc_failed,
                                     std::unordered_map<uint32_t, std::vector<ReplicationPair> > *stoc_repl_pairs,
                                     int level, ReconstructReplicasStats *stats) {
        Version *current = nullptr;
        uint32_t vid = 0;
        while (current == nullptr) {
            vid = versions_->current_version_id();
            NOVA_ASSERT(vid < MAX_LIVE_MEMTABLES) << vid;
            current = versions_->versions_[vid]->Ref();
        }
        NOVA_ASSERT(current->version_id() == vid);
        StorageSelector selector(&rand_seed_);
        mutex_compacting_tables.Lock();
        for (const auto &it : current->files_[level]) {
            // find an available replica to replicate the sstable.
            for (int replica_id = 0; replica_id < it->block_replica_handles.size(); replica_id++) {
                const auto &replica = it->block_replica_handles[replica_id];
                if (replica.meta_block_handle.server_id == failed_stoc_id) {
                    stats->total_num_failed_datafiles += 1;
                    stats->total_num_failed_metafiles += 1;
                    stats->total_failed_metafiles_bytes += replica.meta_block_handle.size;
                    stats->total_failed_datafiles_bytes += replica.data_block_group_handles[0].size;

                    if (compacting_tables_.find(it->number) != compacting_tables_.end()) {
                        // Skip replicating this table since it going to be deleted soon.
                        continue;
                    }

                    stats->recover_num_datafiles += 1;
                    stats->recover_num_metafiles += 1;
                    stats->recover_metafiles_bytes += replica.meta_block_handle.size;
                    stats->recover_datafiles_bytes += replica.data_block_group_handles[0].size;

                    NOVA_ASSERT(replica.data_block_group_handles.size() == 1);
                    // meta replica.
                    uint32_t available_replica_id = 0;
                    uint32_t dest_stoc_id = selector.SelectAvailableStoCForFailedMetaBlock(
                            it->block_replica_handles, replica_id, is_stoc_failed, &available_replica_id);
                    ReplicationPair pair = {};
                    pair.dest_stoc_id = dest_stoc_id;
                    const auto &available_replica = it->block_replica_handles[available_replica_id];
                    pair.source_stoc_file_id = available_replica.meta_block_handle.stoc_file_id;
                    pair.internal_type = FileInternalType::kFileMetadata;
                    pair.sstable_file_number = it->number;
                    if (is_stoc_failed) {
                        pair.replica_id = it->block_replica_handles.size();
                    } else {
                        pair.replica_id = replica_id;
                    }
                    pair.source_file_size = available_replica.meta_block_handle.size;
                    (*stoc_repl_pairs)[available_replica.meta_block_handle.server_id].push_back(pair);
                    // data fragment replica.
                    {
                        ReplicationPair pair = {};
                        pair.dest_stoc_id = dest_stoc_id;
                        pair.source_stoc_file_id = available_replica.data_block_group_handles[0].stoc_file_id;
                        pair.internal_type = FileInternalType::kFileData;
                        pair.sstable_file_number = it->number;
                        if (is_stoc_failed) {
                            pair.replica_id = it->block_replica_handles.size();
                        } else {
                            pair.replica_id = replica_id;
                        }
                        pair.source_file_size = available_replica.data_block_group_handles[0].size;
                        (*stoc_repl_pairs)[available_replica.data_block_group_handles[0].server_id].push_back(pair);
                    }
                    break;
                }
            }
        }
        mutex_compacting_tables.Unlock();
        versions_->versions_[vid]->Unref(dbname_);
    }

    void DBImpl::QueryDBStats(leveldb::DBStats *db_stats) {
        Version *current = nullptr;
        uint32_t vid = 0;
        while (current == nullptr) {
            vid = versions_->current_version_id();
            NOVA_ASSERT(vid < MAX_LIVE_MEMTABLES) << vid;
            current = versions_->versions_[vid]->Ref();
        }
        NOVA_ASSERT(current->version_id() == vid);
        if (options_.enable_subranges && options_.enable_detailed_stats) {
            subrange_manager_->QueryDBStats(db_stats);
        }
        current->QueryStats(db_stats, options_.enable_detailed_stats);
        versions_->versions_[vid]->Unref(dbname_);
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
            if (!ok || level >= options_.level) {
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
            InternalKey k1(range[i].lower, kMaxSequenceNumber,
                           kValueTypeForSeek);
            InternalKey k2(range[i].upper, kMaxSequenceNumber,
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
        return WriteMemTablePool(opt, key, value);
    }

    Status DB::Delete(const WriteOptions &opt, const Slice &key) {
        return WriteMemTablePool(opt, key, Slice());
    }

    DB::~DB() = default;

    Status
    DB::Open(const Options &options, const std::string &dbname, DB **dbptr) {
        *dbptr = nullptr;
        DBImpl *impl = new DBImpl(options, dbname);
        impl->mutex_.Lock();
        impl->server_id_ = nova::NovaConfig::config->my_server_id;

        if (!options.debug) {
            std::string manifest_file_name = DescriptorFileName(dbname, 0, 0);
            impl->manifest_file_ = new StoCWritableFileClient(options.env,
                                                              impl->options_, 0,
                                                              options.mem_manager,
                                                              options.stoc_client,
                                                              impl->dbname_, 0,
                                                              nova::NovaConfig::config->manifest_file_size,
                                                              &impl->rand_seed_,
                                                              manifest_file_name);
        }
        for (int i = 0; i < MAX_LIVE_MEMTABLES; i++) {
            impl->versions_->mid_table_mapping_[i]->nentries_ = 0;
        }
        if (options.memtable_type == MemTableType::kMemTablePool) {
            for (int i = 0; i < impl->min_memtables_; i++) {
                uint32_t memtable_id = impl->memtable_id_seq_.fetch_add(1);
                MemTable *new_table = new MemTable(impl->internal_comparator_, memtable_id, impl->db_profiler_, true);
                new_table->is_pinned_ = true;
                NOVA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                impl->versions_->mid_table_mapping_[memtable_id]->SetMemTable(INIT_GEN_ID, new_table);
                impl->active_memtables_.push_back(
                        impl->versions_->mid_table_mapping_[memtable_id]);
                NOVA_ASSERT(
                        options.memtable_pool->num_available_memtables_ >= 1);
                options.memtable_pool->num_available_memtables_ -= 1;

            }
            options.memtable_pool->range_cond_vars_[impl->dbid_] = &impl->memtable_available_signal_;
            impl->number_of_active_memtables_ = impl->min_memtables_;
        } else {
            impl->partitioned_active_memtables_.resize(options.num_memtable_partitions);
            impl->partitioned_imms_.resize(options.num_memtables);

            for (int i = 0; i < impl->partitioned_imms_.size(); i++) {
                impl->partitioned_imms_[i] = 0;
            }
            uint32_t nslots = options.num_memtables / options.num_memtable_partitions;
            uint32_t remainder = options.num_memtables % options.num_memtable_partitions;
            uint32_t slot_id = 0;
            for (int i = 0; i < options.num_memtable_partitions; i++) {
                uint64_t memtable_id = impl->memtable_id_seq_.fetch_add(1);
                MemTable *table = new MemTable(impl->internal_comparator_, memtable_id, impl->db_profiler_, true);
                NOVA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                impl->versions_->mid_table_mapping_[memtable_id]->SetMemTable(INIT_GEN_ID, table);
                impl->partitioned_active_memtables_[i] = new MemTablePartition;
                impl->partitioned_active_memtables_[i]->active_memtable = table;
                impl->partitioned_active_memtables_[i]->partition_id = i;
                impl->partitioned_active_memtables_[i]->AddMemTable(INIT_GEN_ID, table->memtableid());
                uint32_t slots = nslots;
                if (remainder > 0) {
                    slots += 1;
                    remainder--;
                }
                impl->partitioned_active_memtables_[i]->imm_slots.resize(slots);
                for (int j = 0; j < slots; j++) {
                    impl->partitioned_active_memtables_[i]->imm_slots[j] = slot_id + j;
                    impl->partitioned_active_memtables_[i]->available_slots.push(slot_id + j);
                }
                slot_id += slots;
            }
            NOVA_ASSERT(slot_id == options.num_memtables);
            slot_id = 0;
            for (int i = 0; i < options.num_memtable_partitions; i++) {
                for (int j = 0; j < impl->partitioned_active_memtables_[i]->imm_slots.size(); j++) {
                    NOVA_ASSERT(slot_id == impl->partitioned_active_memtables_[i]->imm_slots[j]);
                    slot_id++;
                }
                NOVA_LOG(rdmaio::INFO) << impl->partitioned_active_memtables_[i]->DebugString();
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
        impl->flush_order_ = new FlushOrder(&impl->partitioned_active_memtables_);

        if (options.enable_subranges) {
            FlushOrder *flush_order = nullptr;
            if (nova::NovaConfig::config->use_ordered_flush) {
                flush_order = impl->flush_order_;
            }
            impl->subrange_manager_ = new SubRangeManager(impl->manifest_file_,
                                                          flush_order,
                                                          dbname,
                                                          impl->dbid_,
                                                          impl->versions_,
                                                          impl->options_,
                                                          &impl->internal_comparator_,
                                                          impl->user_comparator_,
                                                          &impl->memtable_id_seq_,
                                                          &impl->partitioned_active_memtables_,
                                                          &impl->partitioned_imms_);
        }
        if (options.enable_range_index) {
            impl->range_index_manager_ = new RangeIndexManager(&impl->scan_stats, impl->versions_,
                                                               impl->user_comparator_);
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
//            env->UnlockFile(lockname);  // Ignore error since state is already gone
            env->DeleteFile(lockname);
            env->DeleteDir(
                    dbname);  // Ignore error in case dir contains other files
        }
        return result;
    }

}  // namespace leveldb