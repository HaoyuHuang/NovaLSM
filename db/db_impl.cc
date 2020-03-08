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

namespace leveldb {

    namespace {
        struct TableLocation {
            uint32_t memtable_id;
        };

        static void DeleteEntry(const Slice &key, void *value) {
            TableLocation *tf = reinterpret_cast<TableLocation *>(value);
            delete tf;
        }
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

    struct DBImpl::CompactionState {
        // Files produced by compaction
        struct Output {
            uint64_t number;
            uint64_t file_size;
            uint64_t converted_file_size;
            InternalKey smallest, largest;
            std::vector<RTableHandle> data_block_group_handles;
        };

        Output *current_output() { return &outputs[outputs.size() - 1]; }

        explicit CompactionState(Compaction *c)
                : compaction(c),
                  smallest_snapshot(0),
                  outfile(nullptr),
                  builder(nullptr),
                  total_bytes(0) {}

        Compaction *const compaction;

        // Sequence numbers < smallest_snapshot are not significant since we
        // will never have to service a snapshot below smallest_snapshot.
        // Therefore if we have seen a sequence number S <= smallest_snapshot,
        // we can drop all entries for the same key with sequence numbers < S.
        SequenceNumber smallest_snapshot;

        std::vector<Output> outputs;

        // State kept for output being generated
        MemWritableFile *outfile;
        TableBuilder *builder;
        std::vector<MemWritableFile *> output_files;

        uint64_t total_bytes;
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
        if (result.block_cache == nullptr) {
            result.block_cache = NewLRUCache(8 << 20);
        }
        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format("block cache {}",
                           result.block_cache->TotalCapacity());
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
              background_work_finished_signal_(&mutex_),
              seed_(0),
              manual_compaction_(nullptr),
              versions_(new VersionSet(dbname_, &options_, table_cache_,
                                       &internal_comparator_)),
              bg_threads_(raw_options.bg_threads) {
        if (options_.enable_table_locator) {
            table_locator_ = NewLRUCache(10000000);
        }
        RDMA_LOG(rdmaio::INFO) << fmt::format("DB Cache capacity {}. {}",
                                              options_.block_cache->TotalCapacity(),
                                              raw_options.block_cache->TotalCapacity());
    }

    DBImpl::~DBImpl() {
        // Wait for background work to finish.
        mutex_.Lock();
        shutting_down_.store(true, std::memory_order_release);
//        while (background_compaction_scheduled_) {
//            background_work_finished_signal_.Wait();
//        }
        mutex_.Unlock();

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

    Status DBImpl::NewDB() {
        VersionEdit new_db;
        new_db.SetComparatorName(user_comparator()->Name());
        new_db.SetLogNumber(0);
        new_db.SetNextFile(2);
        new_db.SetLastSequence(0);

        const std::string manifest = DescriptorFileName(dbname_, 1);
        WritableFile *file;
        Status s = env_->NewWritableFile(manifest, {
                .level = -1
        }, &file);
        if (!s.ok()) {
            return s;
        }
        {
            log::Writer log(file);
            std::string record;
            new_db.EncodeTo(&record);
            s = log.AddRecord(record);
            if (s.ok()) {
                s = file->Close();
            }
        }
        delete file;
        if (s.ok()) {
            // Make "CURRENT" file that points to the new manifest file.
            s = SetCurrentFile(env_, dbname_, 1);
        } else {
            env_->DeleteFile(manifest);
        }
        return s;
    }

    void DBImpl::EvictFileFromCache(uint64_t file_number) {
        table_cache_->Evict(file_number);
    }

    void DBImpl::MaybeIgnoreError(Status *s) const {
        if (s->ok() || options_.paranoid_checks) {
            // No change needed
        } else {
            Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
            *s = Status::OK();
        }
    }

    void DBImpl::DeleteObsoleteFiles(EnvBGThread *bg_thread) {
        mutex_.AssertHeld();

        if (!bg_error_.ok()) {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return;
        }

        // Make a set of all of the live files
        std::set<uint64_t> live = pending_outputs_;
        versions_->AddLiveFiles(&live);

        std::vector<std::string> filenames;
        env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
        uint64_t number;
        FileType type;
        std::vector<std::string> files_to_delete;

        for (std::string &filename : filenames) {
            if (ParseFileName(filename, &number, &type)) {
                bool keep = true;
                switch (type) {
                    case kLogFile:
                        keep = ((number >= versions_->LogNumber()) ||
                                (number == versions_->PrevLogNumber()));
                        break;
                    case kDescriptorFile:
                        // Keep my manifest file, and any newer incarnations'
                        // (in case there is a race that allows other incarnations)
                        keep = (number >= versions_->ManifestFileNumber());
                        break;
                    case kTableFile:
                        keep = (live.find(number) != live.end());
                        break;
                    case kTempFile:
                        // Any temp files that are currently being written to must
                        // be recorded in pending_outputs_, which is inserted into "live"
                        keep = (live.find(number) != live.end());
                        break;
                    case kCurrentFile:
                    case kDBLockFile:
                    case kInfoLogFile:
                        keep = true;
                        break;
                }

                if (!keep) {
                    files_to_delete.push_back(std::move(filename));
                    if (type == kTableFile) {
//                        table_cache_->Evict(number);
                    }
                    Log(options_.info_log, "Delete type=%d #%lld\n",
                        static_cast<int>(type),
                        static_cast<unsigned long long>(number));
                }
            }
        }

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
            auto handles = meta.data_block_group_handles;
            for (int i = 0; i < handles.size(); i++) {
                SSTableRTablePair pair = {};
                pair.sstable_id = TableFileName(dbname_, meta.number);
                pair.rtable_id = handles[i].rtable_id;
                server_pairs[handles[i].server_id].push_back(pair);
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
        for (auto it : server_pairs) {
            bg_thread->dc_client()->InitiateDeleteTables(it.first,
                                                         it.second);
        }
        mutex_.Lock();
    }

    Status DBImpl::Recover(VersionEdit *edit, bool *save_manifest) {
        mutex_.AssertHeld();

        // Ignore error from CreateDir since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        env_->CreateDir(dbname_);
        assert(db_lock_ == nullptr);
        Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
        if (!s.ok()) {
            return s;
        }

        if (!env_->FileExists(CurrentFileName(dbname_))) {
            if (options_.create_if_missing) {
                s = NewDB();
                if (!s.ok()) {
                    return s;
                }
            } else {
                return Status::InvalidArgument(
                        dbname_, "does not exist (create_if_missing is false)");
            }
        } else {
            if (options_.error_if_exists) {
                return Status::InvalidArgument(dbname_,
                                               "exists (error_if_exists is true)");
            }
        }

        s = versions_->Recover(save_manifest);
        if (!s.ok()) {
            return s;
        }
        SequenceNumber max_sequence(0);

        // Recover from all newer log files than the ones named in the
        // descriptor (new log files may have been added by the previous
        // incarnation without registering them in the descriptor).
        //
        // Note that PrevLogNumber() is no longer used, but we pay
        // attention to it in case we are recovering a database
        // produced by an older version of leveldb.
        const uint64_t min_log = versions_->LogNumber();
        const uint64_t prev_log = versions_->PrevLogNumber();
        std::vector<std::string> filenames;
        s = env_->GetChildren(dbname_, &filenames);
        if (!s.ok()) {
            return s;
        }
        std::set<uint64_t> expected;
        versions_->AddLiveFiles(&expected);
        uint64_t number;
        FileType type;
        std::vector<uint64_t> logs;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type)) {
                expected.erase(number);
                if (type == kLogFile &&
                    ((number >= min_log) || (number == prev_log)))
                    logs.push_back(number);
            }
        }
        if (!expected.empty()) {
            char buf[50];
            snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                     static_cast<int>(expected.size()));
            return Status::Corruption(buf, TableFileName(dbname_,
                                                         *(expected.begin())));
        }

        // Recover in the order in which the logs were generated
        std::sort(logs.begin(), logs.end());
        for (size_t i = 0; i < logs.size(); i++) {
            s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest,
                               edit,
                               &max_sequence);
            if (!s.ok()) {
                return s;
            }

            // The previous incarnation may not have written any MANIFEST
            // records after allocating this log number.  So we manually
            // update the file number allocation counter in VersionSet.
            versions_->MarkFileNumberUsed(logs[i]);
        }

        if (versions_->LastSequence() < max_sequence) {
            versions_->SetLastSequence(max_sequence);
        }

        return Status::OK();
    }

    Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                                  bool *save_manifest, VersionEdit *edit,
                                  SequenceNumber *max_sequence) {
        struct LogReporter : public log::Reader::Reporter {
            Env *env;
            Logger *info_log;
            const char *fname;
            Status *status;  // null if options_.paranoid_checks==false
            void Corruption(size_t bytes, const Status &s) override {
                Log(info_log, "%s%s: dropping %d bytes; %s",
                    (this->status == nullptr ? "(ignoring error) " : ""), fname,
                    static_cast<int>(bytes), s.ToString().c_str());
                if (this->status != nullptr && this->status->ok())
                    *this->status = s;
            }
        };

        mutex_.AssertHeld();

        // Open the log file
        std::string fname = LogFileName(dbname_, log_number);
        SequentialFile *file;
        Status status = env_->NewSequentialFile(fname, &file);
        if (!status.ok()) {
            MaybeIgnoreError(&status);
            return status;
        }

        // Create the log reader.
        LogReporter reporter;
        reporter.env = env_;
        reporter.info_log = options_.info_log;
        reporter.fname = fname.c_str();
        reporter.status = (options_.paranoid_checks ? &status : nullptr);
        // We intentionally make log::Reader do checksumming even if
        // paranoid_checks==false so that corruptions cause entire commits
        // to be skipped instead of propagating bad information (like overly
        // large sequence numbers).
        log::Reader reader(file, &reporter, true /*checksum*/,
                           0 /*initial_offset*/);
        Log(options_.info_log, "Recovering log #%llu",
            (unsigned long long) log_number);

        // Read all the records and add to a memtable
        std::string scratch;
        Slice record;
        WriteBatch batch;
        int compactions = 0;
        MemTable *mem = nullptr;
        while (reader.ReadRecord(&record, &scratch) && status.ok()) {
            if (record.size() < 12) {
                reporter.Corruption(record.size(),
                                    Status::Corruption("log record too small"));
                continue;
            }
            WriteBatchInternal::SetContents(&batch, record);

            if (mem == nullptr) {
                mem = new MemTable(internal_comparator_, memtable_id_,
                                   db_profiler_);
                mem->Ref();
                RDMA_ASSERT(memtable_id_ < MAX_LIVE_MEMTABLES);
                versions_->mid_table_mapping_[memtable_id_].SetMemTable(mem);
                memtable_id_++;
            }
            status = WriteBatchInternal::InsertInto(&batch, mem);
            MaybeIgnoreError(&status);
            if (!status.ok()) {
                break;
            }
            const SequenceNumber last_seq =
                    WriteBatchInternal::Sequence(&batch) +
                    WriteBatchInternal::Count(&batch) - 1;
            if (last_seq > *max_sequence) {
                *max_sequence = last_seq;
            }

            if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
                compactions++;
                *save_manifest = true;
//                status = WriteLevel0Table(mem, edit, nullptr);
                versions_->mid_table_mapping_[mem->memtableid()].Unref();
                mem = nullptr;
                if (!status.ok()) {
                    // Reflect errors immediately so that conditions like full
                    // file-systems cause the DB::Open() to fail.
                    break;
                }
            }
        }

        delete file;

        // See if we should keep reusing the last log file.
        if (status.ok() && options_.reuse_logs && last_log &&
            compactions == 0) {
        }

        if (mem != nullptr) {
            // mem did not get reused; compact it.
            if (status.ok()) {
                *save_manifest = true;
//                status = WriteLevel0Table(mem, edit, nullptr);
            }
            versions_->mid_table_mapping_[mem->memtableid()].Unref();
        }

        return status;
    }

    bool DBImpl::CompactMemTable(EnvBGThread *bg_thread) {
        mutex_.AssertHeld();
        if (imms_.empty() || versions_->current()->NumFiles(0) + imms_.size() -
                             nimms_wait_for_compaction_ >=
                             options_.l0_stop_writes_trigger) {
            return false;
        }
        MemTable *imm = nullptr;
        for (int i = 0; i < imms_.size(); i++) {
            if (imms_[i]->state() == MemTableState::MEMTABLE_FULL) {
                imm = imms_[i];
                break;
            }
        }
        if (!imm) {
            return false;
        }
        nimms_wait_for_compaction_ -= 1;
        imm->set_state(MemTableState::MEMTABLE_FLUSHING);
        FileMetaData &meta = imm->meta();
        meta.number = versions_->NewFileNumber();
        pending_outputs_.insert(meta.number);
        Log(options_.info_log, "bg[%lu]: Level-0 table #%llu: started",
            bg_thread->thread_id(),
            (unsigned long long) meta.number);

        Status s;
        {
            mutex_.Unlock();
            Iterator *iter = imm->NewIterator(TraceType::IMMUTABLE_MEMTABLE,
                                              AccessCaller::kCompaction);
            s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta,
                           bg_thread);
            RDMA_ASSERT(s.ok()) << s.ToString();
            delete iter;
            mutex_.Lock();
            imm->set_state(MemTableState::MEMTABLE_FLUSHED);
        }

        {
            VersionEdit edit;
            Version *base = versions_->current();
            auto it = imms_.begin();
            while (it != imms_.end()) {
                MemTable *imm = *it;
                if (imm->state() != MemTableState::MEMTABLE_FLUSHED) {
                    it++;
                    continue;
                }
                FileMetaData &meta = imm->meta();
                Log(options_.info_log,
                    "bg[%lu]: Level-0 table #%llu: %lld bytes",
                    bg_thread->thread_id(),
                    (unsigned long long) meta.number,
                    (unsigned long long) meta.file_size);
                pending_outputs_.erase(meta.number);

                // Note that if file_size is zero, the file has been deleted and
                // should not be added to the manifest.
                int level = 0;
                if (meta.file_size > 0) {
                    const Slice min_user_key = meta.smallest.user_key();
                    const Slice max_user_key = meta.largest.user_key();
                    level = base->PickLevelForMemTableOutput(min_user_key,
                                                             max_user_key);
                    RDMA_ASSERT(imm->memtableid() != 0);
                    edit.AddFile(level, imm->memtableid(),
                                 meta.number,
                                 meta.file_size,
                                 meta.converted_file_size, meta.smallest,
                                 meta.largest, FileCompactionStatus::NONE,
                                 meta.data_block_group_handles);
                }

                CompactionStats stats;
                stats.micros = env_->NowMicros() - 0;
                stats.bytes_written = meta.file_size;
                stats_[level].Add(stats);
                it = imms_.erase(it);
            }
            // Replace immutable memtable with the generated Table
            edit.SetPrevLogNumber(0);
            Status s = versions_->LogAndApply(&edit, &mutex_);
            RDMA_ASSERT(s.ok());
        }
        return true;
    }

    void DBImpl::CompactRange(const Slice *begin, const Slice *end) {
        int max_level_with_files = 1;
        {
            MutexLock l(&mutex_);
            Version *base = versions_->current();
            for (int level = 1; level < config::kNumLevels; level++) {
                if (base->OverlapInLevel(level, begin, end)) {
                    max_level_with_files = level;
                }
            }
        }
        TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
        for (int level = 0; level < max_level_with_files; level++) {
            TEST_CompactRange(level, begin, end);
        }
    }

    void DBImpl::TEST_CompactRange(int level, const Slice *begin,
                                   const Slice *end) {
        assert(level >= 0);
        assert(level + 1 < config::kNumLevels);

        InternalKey begin_storage, end_storage;

        ManualCompaction manual;
        manual.level = level;
        manual.done = false;
        if (begin == nullptr) {
            manual.begin = nullptr;
        } else {
            begin_storage = InternalKey(*begin, kMaxSequenceNumber,
                                        kValueTypeForSeek);
            manual.begin = &begin_storage;
        }
        if (end == nullptr) {
            manual.end = nullptr;
        } else {
            end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
            manual.end = &end_storage;
        }

        MutexLock l(&mutex_);
        while (!manual.done &&
               !shutting_down_.load(std::memory_order_acquire) &&
               bg_error_.ok()) {
            if (manual_compaction_ == nullptr) {  // Idle
                manual_compaction_ = &manual;
//                MaybeScheduleCompaction();
            } else {  // Running either my compaction or another compaction.
                background_work_finished_signal_.Wait();
            }
        }
        if (manual_compaction_ == &manual) {
            // Cancel my manual compaction since we aborted early for some reason.
            manual_compaction_ = nullptr;
        }
    }

    Status DBImpl::TEST_CompactMemTable() {
        // nullptr batch means just wait for earlier writes to be done
//        Status s = Write(WriteOptions(), nullptr);
//        if (s.ok()) {
        // Wait until the compaction completes
//            MutexLock l(&mutex_);
//            while (imm_ != nullptr && bg_error_.ok()) {
//                background_work_finished_signal_.Wait();
//            }
//            if (imm_ != nullptr) {
//                s = bg_error_;
//            }
//        }
        return Status::OK();
//        return s;
    }

    void DBImpl::RecordBackgroundError(const Status &s) {
        mutex_.AssertHeld();
        if (bg_error_.ok()) {
            bg_error_ = s;
            background_work_finished_signal_.SignalAll();
        }
    }

    void DBImpl::MaybeScheduleCompaction(uint32_t thread_id) {
        EnvBGThread *bg = nullptr;
        if (nimms_wait_for_compaction_ > 0 ||
            versions_->NeedsCompaction()) {
            uint32_t min_tasks = 99999;
            uint32_t min_bg_id = 0;
            for (int i = 0; i < bg_threads_.size(); i++) {
                uint32_t tasks = bg_threads_[i]->num_running_tasks();
                if (tasks < min_tasks) {
                    min_tasks = tasks;
                    min_bg_id = i;
                }
            }
            bg = bg_threads_[min_bg_id];
        }

        CompactionTask task = {};
        task.db = this;
        if (bg) {
            if (bg->Schedule(task)) {
                Log(options_.info_log,
                    "t[%u]: Schedule compaction on thread %lu. %u",
                    thread_id, bg->thread_id(), nimms_wait_for_compaction_);
            }
            return;
        }
    }

    void DBImpl::PerformCompaction(leveldb::EnvBGThread *bg_thread,
                                   const CompactionTask &task) {
        MutexLock l(&mutex_);
        bool schedule = false;
        if (shutting_down_.load(std::memory_order_acquire)) {
            // No more background work when shutting down.
        } else if (!bg_error_.ok()) {
            // No more background work after a background error.
        } else {
            bool compacted = BackgroundCompaction(bg_thread);
            if (compacted) {
                versions_->LevelSummary(bg_thread->thread_id());
                schedule = true;
            }

            if (nimms_wait_for_compaction_ > 0 &&
                versions_->current()->NumFiles(0) + imms_.size() -
                nimms_wait_for_compaction_ <
                options_.l0_stop_writes_trigger) {
                schedule = true;
            }
        }

        if (schedule) {
            MaybeScheduleCompaction(bg_thread->thread_id());
        }

        if (imms_.size() < options_.num_memtables - 1 ||
            (versions_->NumLevelFiles(0) <
             options_.l0_stop_writes_trigger &&
             options_.l0_stop_writes_trigger != UINT32_MAX)) {
            background_work_finished_signal_.SignalAll();
        }
    }

    bool DBImpl::BackgroundCompaction(EnvBGThread *bg_thread) {
        mutex_.AssertHeld();
        return CompactMemTable(bg_thread);
//        if (CompactMemTable(bg_thread)) {
//            return true;
//        }
//
//        if (is_major_compaciton_running_) {
//            return false;
//        }
//
////        if (!versions_->NeedsCompaction()) {
////            if (CompactMemTable(bg_thread)) {
////                return true;
////            }
////            return false;
////        }
//        Compaction *c;
//        bool is_manual = (manual_compaction_ != nullptr);
//        InternalKey manual_end;
//        if (is_manual) {
//            ManualCompaction *m = manual_compaction_;
//            c = versions_->CompactRange(m->level, m->begin, m->end);
//            m->done = (c == nullptr);
//            if (c != nullptr) {
//                manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
//            }
//            Log(options_.info_log,
//                "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
//                m->level,
//                (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
//                (m->end ? m->end->DebugString().c_str() : "(end)"),
//                (m->done ? "(end)" : manual_end.DebugString().c_str()));
//        } else {
//            c = versions_->PickCompaction(bg_thread->thread_id());
//        }
//
//        Status status;
//        if (c == nullptr) {
//            // Nothing to do
//            if (CompactMemTable(bg_thread)) {
//                return true;
//            }
//            return false;
//        }
//        if (!is_manual && c->IsTrivialMove()) {
//            // Move file to next level
//            assert(c->level() >= 0);
//            assert(c->num_input_files(0) == 1);
//            assert(c->num_input_files(1) == 0);
//            FileMetaData *f = c->input(0, 0);
//            int level = c->level() == -1 ? 0 : c->level();
//            c->edit()->DeleteFile(level, f->memtable_id, f->number);
//            c->edit()->AddFile(level + 1,
//                               0,
//                               f->number, f->file_size,
//                               f->converted_file_size,
//                               f->smallest,
//                               f->largest, FileCompactionStatus::NONE,
//                               f->data_block_group_handles);
//            status = versions_->LogAndApply(c->edit(), &mutex_);
//            if (!status.ok()) {
//                RecordBackgroundError(status);
//            }
//            Log(options_.info_log,
//                "Moved #%lld@%d to level-%d %lld bytes %s\n",
//                static_cast<unsigned long long>(f->number),
//                c->level(), c->level() + 1,
//                static_cast<unsigned long long>(f->file_size),
//                status.ToString().c_str());
//        } else {
//            is_major_compaciton_running_ = true;
//            CompactionState *compact = new CompactionState(c);
//            status = DoCompactionWork(compact, bg_thread);
//            if (!status.ok()) {
//                RecordBackgroundError(status);
//            }
//            CleanupCompaction(compact);
//            c->ReleaseInputs();
//            DeleteObsoleteFiles(bg_thread);
//            is_major_compaciton_running_ = false;
//            RDMA_LOG(rdmaio::DEBUG)
//                << fmt::format("!!!!!!!!!!!!!Compaction complete");
//
//        }
//        delete c;
//
//        if (status.ok()) {
//            // Done
//        } else if (shutting_down_.load(std::memory_order_acquire)) {
//            // Ignore compaction errors found during shutting down
//        } else {
//            Log(options_.info_log, "Compaction error: %s",
//                status.ToString().c_str());
//        }
//
//        if (is_manual) {
//            ManualCompaction *m = manual_compaction_;
//            if (!status.ok()) {
//                m->done = true;
//            }
//            if (!m->done) {
//                // We only compacted part of the requested range.  Update *m
//                // to the range that is left to be compacted.
//                m->tmp_storage = manual_end;
//                m->begin = &m->tmp_storage;
//            }
//            manual_compaction_ = nullptr;
//        }
//        return true;;
    }

    void DBImpl::CleanupCompaction(CompactionState *compact) {
        mutex_.AssertHeld();
        if (compact->builder != nullptr) {
            // May happen if we get a shutdown call in the middle of compaction
            compact->builder->Abandon();
            delete compact->builder;
        } else {
//            assert(compact->outfile == nullptr);
        }

        // Also delete its contained mem file.
        // Delete everything now.
        // TODO:

        // Delete the files.
        for (int i = 0; i < compact->output_files.size(); i++) {
            MemWritableFile *out = compact->output_files[i];
            if (out) {
                auto *mem_file = dynamic_cast<NovaCCMemFile *>(out->mem_file());
                delete mem_file;
                delete out;
                mem_file = nullptr;
                out = nullptr;
            }
        }


        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionState::Output &out = compact->outputs[i];
            pending_outputs_.erase(out.number);
        }
        delete compact;
    }

    Status DBImpl::OpenCompactionOutputFile(CompactionState *compact,
                                            EnvBGThread *bg_thread) {
        assert(compact != nullptr);
        assert(compact->builder == nullptr);
        uint64_t file_number;
        {
            mutex_.Lock();
            file_number = versions_->NewFileNumber();
            pending_outputs_.insert(file_number);
            CompactionState::Output out;
            out.number = file_number;
            out.smallest.Clear();
            out.largest.Clear();
            compact->outputs.push_back(out);
            mutex_.Unlock();
        }
        // Make the output file
        MemManager *mem_manager = bg_thread->mem_manager();
        NovaCCMemFile *cc_file = new NovaCCMemFile(options_.env,
                                                   options_,
                                                   file_number,
                                                   mem_manager,
                                                   bg_thread->dc_client(),
                                                   dbname_,
                                                   bg_thread->thread_id(),
                                                   options_.max_dc_file_size);
        compact->outfile = new MemWritableFile(cc_file);
        compact->builder = new TableBuilder(options_, compact->outfile);
        compact->output_files.push_back(compact->outfile);
        return Status::OK();
    }

    Status DBImpl::FinishCompactionOutputFile(CompactionState *compact,
                                              Iterator *input) {
        assert(compact != nullptr);
        assert(compact->outfile != nullptr);
        assert(compact->builder != nullptr);
        assert(!compact->output_files.empty());

        const uint64_t output_number = compact->current_output()->number;
        assert(output_number != 0);

        // Check for iterator errors
        Status s = input->status();
        if (s.ok()) {
            s = compact->builder->Finish();
        } else {
            compact->builder->Abandon();
        }
        const uint64_t current_entries = compact->builder->NumEntries();
        const uint64_t current_data_blocks = compact->builder->NumDataBlocks();
        const uint64_t current_bytes = compact->builder->FileSize();
        compact->current_output()->file_size = current_bytes;
        compact->total_bytes += current_bytes;
        delete compact->builder;
        compact->builder = nullptr;

        FileMetaData meta;
        meta.number = output_number;
        meta.file_size = current_bytes;
        meta.smallest = compact->current_output()->smallest;
        meta.largest = compact->current_output()->largest;
        // Set meta in order to flush to the corresponding DC node.
        NovaCCMemFile *mem_file = static_cast<NovaCCMemFile *>(compact->outfile->mem_file());
        mem_file->set_meta(meta);
        mem_file->set_num_data_blocks(current_data_blocks);

        // Finish and check for file errors
        RDMA_ASSERT(s.ok());
        s = compact->outfile->Sync();
        s = compact->outfile->Close();

        mem_file->WaitForPersistingDataBlocks();

        if (s.ok() && current_entries > 0) {
            // Verify that the table is usable
//            Iterator *iter =
//                    table_cache_->NewIterator(AccessCaller::kUncategorized,
//                                              ReadOptions(), meta,
//                                              output_number,
//                                              compact->compaction->level() + 1,
//                                              current_bytes);
//            s = iter->status();
//            delete iter;
//            if (s.ok()) {
//                Log(options_.info_log,
//                    "Generated table #%llu@%d: %lld keys, %lld bytes",
//                    (unsigned long long) output_number,
//                    compact->compaction->level(),
//                    (unsigned long long) current_entries,
//                    (unsigned long long) current_bytes);
//            }
        }
        return s;
    }

    Status DBImpl::InstallCompactionResults(CompactionState *compact,
                                            uint32_t thread_id) {
        // Wait for all writes to complete.
//        for (int i = 0; i < compact->output_files.size(); i++) {
//            CompactionState::Output &output = compact->outputs[i];
//            MemWritableFile *out = compact->output_files[i];
//            auto *mem_file = dynamic_cast<NovaCCMemFile *>(out->mem_file());
//            mem_file->WaitForPersistingDataBlocks();
//        }

        // Now finalize all SSTables.
        for (int i = 0; i < compact->output_files.size(); i++) {
            CompactionState::Output &output = compact->outputs[i];
            MemWritableFile *out = compact->output_files[i];
            auto *mem_file = dynamic_cast<NovaCCMemFile *>(out->mem_file());
            output.converted_file_size = mem_file->Finalize();
            output.data_block_group_handles = mem_file->rhs();

            delete mem_file;
            delete out;
            compact->output_files[i] = nullptr;
            mem_file = nullptr;
            out = nullptr;
        }

        // Add compaction outputs
        compact->compaction->AddInputDeletions(compact->compaction->edit());
        const int src_level = compact->compaction->level() == -1 ? 0
                                                                 : compact->compaction->level();
        const int dest_level = compact->compaction->level() == -1 ? 0 :
                               compact->compaction->level() + 1;
        FileCompactionStatus status = FileCompactionStatus::NONE;
        if (compact->compaction->level() == -1) {
            status = FileCompactionStatus::COMPACTED;
        }
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionState::Output &out = compact->outputs[i];
            compact->compaction->edit()->AddFile(dest_level,
                                                 0,
                                                 out.number,
                                                 out.file_size,
                                                 out.converted_file_size,
                                                 out.smallest, out.largest,
                                                 status,
                                                 out.data_block_group_handles);
        }
        mutex_.Lock();
        Log(options_.info_log,
            "bg[%u]: Compacted %d@%d + %d@%d files => %lld bytes",
            thread_id,
            compact->compaction->num_input_files(0),
            src_level,
            compact->compaction->num_input_files(1),
            dest_level,
            static_cast<long long>(compact->total_bytes));
        return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
    }

    Status
    DBImpl::DoCompactionWork(CompactionState *compact, EnvBGThread *bg_thread) {
        const uint64_t start_micros = env_->NowMicros();
        int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

        Log(options_.info_log, "bg[%lu] Compacting %d@%d + %d@%d files",
            bg_thread->thread_id(),
            compact->compaction->num_input_files(0),
            compact->compaction->level(),
            compact->compaction->num_input_files(1),
            compact->compaction->level() + 1);

        //        RDMA_LOG(rdmaio::DEBUG)
//            << fmt::format("!!!!!!!!!!!!!!!!!Compacting {}@{} + {}@{} files",
//                           compact->compaction->num_input_files(
//                                   0),
//                           compact->compaction->level(),
//                           compact->compaction->num_input_files(
//                                   1),
//                           compact->compaction->level() +
//                           1);

        int src_level = compact->compaction->level() == -1 ? 0
                                                           : compact->compaction->level();
        assert(versions_->NumLevelFiles(src_level) > 0);
        assert(compact->builder == nullptr);
        assert(compact->outfile == nullptr);
        assert(compact->outputs.empty());

        if (snapshots_.empty()) {
            compact->smallest_snapshot = versions_->LastSequence();
        } else {
            compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
        }
        // Release mutex while we're actually doing the compaction work
        mutex_.Unlock();

        Iterator *input = versions_->MakeInputIterator(compact->compaction,
                                                       bg_thread);
        input->SeekToFirst();
        Status status;
        ParsedInternalKey ikey;
        std::string current_user_key;
        bool has_current_user_key = false;
        SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
        while (input->Valid() &&
               !shutting_down_.load(std::memory_order_acquire)) {
            // Prioritize immutable compaction work
//            if (nimms_ > 0) {
//                const uint64_t imm_start = env_->NowMicros();
//                mutex_.Lock();
//                if (CompactMemTable(bg_thread)) {
//                    // Wake up MakeRoomForWrite() if necessary.
//                    background_work_finished_signal_.SignalAll();
//                }
//                mutex_.Unlock();
//                imm_micros += (env_->NowMicros() - imm_start);
//            }

            Slice key = input->key();
            if (compact->compaction->ShouldStopBefore(key) &&
                compact->builder != nullptr) {
                status = FinishCompactionOutputFile(compact, input);
                if (!status.ok()) {
                    break;
                }
            }

            // Handle key/value, add to state, etc.
            bool drop = false;
            if (!ParseInternalKey(key, &ikey)) {
                // Do not hide error keys
                current_user_key.clear();
                has_current_user_key = false;
                last_sequence_for_key = kMaxSequenceNumber;
            } else {
                if (!has_current_user_key ||
                    user_comparator()->Compare(ikey.user_key,
                                               Slice(current_user_key)) !=
                    0) {
                    // First occurrence of this user key
                    current_user_key.assign(ikey.user_key.data(),
                                            ikey.user_key.size());
                    has_current_user_key = true;
                    last_sequence_for_key = kMaxSequenceNumber;
                }

                if (last_sequence_for_key <= compact->smallest_snapshot) {
                    // Hidden by an newer entry for same user key
                    drop = true;  // (A)
                } else if (ikey.type == kTypeDeletion &&
                           ikey.sequence <= compact->smallest_snapshot &&
                           compact->compaction->IsBaseLevelForKey(
                                   ikey.user_key)) {
                    // For this user key:
                    // (1) there is no data in higher levels
                    // (2) data in lower levels will have larger sequence numbers
                    // (3) data in layers that are being compacted here and have
                    //     smaller sequence numbers will be dropped in the next
                    //     few iterations of this loop (by rule (A) above).
                    // Therefore this deletion marker is obsolete and can be dropped.
                    drop = true;
                }
                last_sequence_for_key = ikey.sequence;
            }
#if 0
            Log(options_.info_log,
                "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
                "%d smallest_snapshot: %d",
                ikey.user_key.ToString().c_str(),
                (int)ikey.sequence, ikey.type, kTypeValue, drop,
                compact->compaction->IsBaseLevelForKey(ikey.user_key),
                (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

            if (!drop) {
                // Open output file if necessary
                if (compact->builder == nullptr) {
                    status = OpenCompactionOutputFile(compact, bg_thread);
                    if (!status.ok()) {
                        break;
                    }
                }
                if (compact->builder->NumEntries() == 0) {
                    compact->current_output()->smallest.DecodeFrom(key);
                }
                compact->current_output()->largest.DecodeFrom(key);
                compact->builder->Add(key, input->value());

                // Close output file if it is big enough
                if (compact->builder->FileSize() >=
                    compact->compaction->MaxOutputFileSize()) {
                    status = FinishCompactionOutputFile(compact, input);
                    if (!status.ok()) {
                        break;
                    }
                }
            }
            input->Next();
        }

        if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
            status = Status::IOError("Deleting DB during compaction");
        }
        if (status.ok() && compact->builder != nullptr) {
            status = FinishCompactionOutputFile(compact, input);
        }
        if (status.ok()) {
            status = input->status();
        }
        delete input;
        input = nullptr;

        CompactionStats stats;
        stats.micros = env_->NowMicros() - start_micros - imm_micros;
        for (int which = 0; which < 2; which++) {
            for (int i = 0;
                 i < compact->compaction->num_input_files(which); i++) {
                stats.bytes_read += compact->compaction->input(which,
                                                               i)->file_size;
            }
        }
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            stats.bytes_written += compact->outputs[i].file_size;
        }

        if (status.ok()) {
            status = InstallCompactionResults(compact, bg_thread->thread_id());
        }
        if (!status.ok()) {
            RecordBackgroundError(status);
        }
        stats_[compact->compaction->level() + 1].Add(stats);
        versions_->AddCompactedInputs(compact->compaction, &compacted_tables_);
        return status;
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
//        mutex_.Lock();
//        *latest_snapshot = versions_->LastSequence();
//
//        // Collect together all needed child iterators
//        std::vector<Iterator *> list;
//        list.push_back(mem_->NewIterator(TraceType::MEMTABLE,
//                                         AccessCaller::kUserIterator));
//        mem_->Ref();

//
//        if (imm_ != nullptr) {
//            list.push_back(imm_->NewIterator(TraceType::IMMUTABLE_MEMTABLE,
//                                             AccessCaller::kUserIterator));
//            imm_->Ref();
//        }
//        versions_->current()->AddIterators(options, &list);
//        Iterator *internal_iter =
//                NewMergingIterator(&internal_comparator_, &list[0],
//                                   list.size());
//        versions_->current()->Ref();
//
//        IterState *cleanup = new IterState(&mutex_, mem_, imm_,
//                                           versions_->current());
//        internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

//        *seed = ++seed_;
//        mutex_.Unlock();
//        return internal_iter;
    }

    Iterator *DBImpl::TEST_NewInternalIterator() {
        SequenceNumber ignored;
        uint32_t ignored_seed;
        return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
    }

    int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
        MutexLock l(&mutex_);
        return versions_->MaxNextLevelOverlappingBytes();
    }

    Status DBImpl::Get(const ReadOptions &options, const Slice &key,
                       std::string *value) {
        Status s;
        std::string tmp;
        SequenceNumber snapshot = kMaxSequenceNumber;

        mutex_.Lock();
        Version *current = versions_->current();
        current->Ref();
        mutex_.Unlock();

        MemTable *memtable = nullptr;
        uint64_t l0_file_number = 0;
        if (table_locator_ != nullptr) {
            Cache::Handle *handle = table_locator_->Lookup(key);
            if (handle != nullptr) {
                TableLocation *loc = reinterpret_cast<TableLocation *>(table_locator_->Value(
                        handle));
                if (loc->memtable_id != 0) {
                    memtable = versions_->mid_table_mapping_[loc->memtable_id].Ref(
                            &l0_file_number);
                }
            }
        }

        if (memtable != nullptr || l0_file_number != 0) {
            LookupKey lkey(key, snapshot);
            if (memtable != nullptr) {
                RDMA_ASSERT(memtable->Get(lkey, value, &s))
                    << fmt::format("key:{} memtable:{} s:{}", key.ToString(),
                                   memtable->memtableid(), s.ToString());
            } else if (l0_file_number != 0) {
                s = current->Get(options, l0_file_number, lkey, value);
            }
            mutex_.Lock();
            if (memtable != nullptr) {
                versions_->mid_table_mapping_[memtable->memtableid()].Unref();
            }
            current->Unref();
            mutex_.Unlock();
            return s;
        }

        mutex_.Lock();
        // Make a copy of memtables and immutable memtables.
        std::vector<MemTable *> mems = active_memtables_;
        std::vector<MemTable *> imms = imms_;
        for (auto mem : mems) {
            versions_->mid_table_mapping_[mem->memtableid()].Ref(nullptr);
        }
        for (auto imm : imms) {
            if (imm) {
                versions_->mid_table_mapping_[imm->memtableid()].Ref(nullptr);
            }
        }

        bool have_stat_update = false;
        Version::GetStats stats;

        // Unlock while reading from files and memtables
        {
            mutex_.Unlock();
            // First look in the memtable, then in the immutable memtable (if any).
            bool found = false;
            bool search_all = options_.num_memtable_partitions > 1;
            LookupKey lkey(key, snapshot);
            for (auto it = mems.begin(); it != mems.end(); it++) {
                if ((*it)->Get(lkey, &tmp, &s)) {
                    found = true;
                    *value = tmp;
                    if (!search_all) {
                        break;
                    }
                }
            }
            if (!found && !imms.empty()) {
                // search in reverse order.
                for (auto it = imms.rbegin(); it != imms.rend(); it++) {
                    if (*it != nullptr && (*it)->Get(lkey, &tmp, &s)) {
                        *value = tmp;
                        found = true;
                        if (!search_all) {
                            break;
                        }
                    }
                }
            }
            if (!found) {
                s = current->Get(options, lkey, &tmp, &stats, search_all);
                if (s.ok()) {
                    *value = tmp;
                }
                have_stat_update = true;
            }
            mutex_.Lock();
        }

        for (auto mem : mems) {
            versions_->mid_table_mapping_[mem->memtableid()].Unref();
        }
        for (auto imm : imms) {
            if (imm) {
                versions_->mid_table_mapping_[imm->memtableid()].Unref();
            }
        }
        current->Unref();
        mutex_.Unlock();
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

    void DBImpl::RecordReadSample(Slice key) {
//        MutexLock l(&mutex_);
//        if (versions_->current()->RecordReadSample(key)) {
//            MaybeScheduleCompaction();
//        }
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
        return Write(o, key, val);
    }

    Status DBImpl::Delete(const WriteOptions &options, const Slice &key) {
        return DB::Delete(options, key);
    }

    Status DBImpl::GenerateLogRecords(const leveldb::WriteOptions &options,
                                      leveldb::WriteBatch *updates) {
        mutex_.Lock();
        std::string logfile = current_log_file_name_;
        std::list<std::string> closed_files(closed_log_files_.begin(),
                                            closed_log_files_.end());
        closed_log_files_.clear();
        mutex_.Unlock();
        // Synchronous replication.
        uint32_t server_id = 0;
        uint32_t dbid = 0;
        nova::ParseDBIndexFromFile(current_log_file_name_, &server_id, &dbid);

        auto dc = reinterpret_cast<leveldb::NovaBlockCCClient *>(options.dc_client);
        RDMA_ASSERT(dc);
        dc->set_dbid(dbid);
        options.dc_client->InitiateReplicateLogRecords(
                logfile, options.thread_id,
                WriteBatchInternal::Contents(updates));

        for (const auto &file : closed_files) {
            options.dc_client->InitiateCloseLogFile(file);
        }
        return Status::OK();
    }

    Status DBImpl::Write(const WriteOptions &options, const Slice &key,
                         const Slice &val) {
        uint64_t last_sequence = versions_->last_sequence_.fetch_add(1,
                                                                     std::memory_order_relaxed);
        uint32_t partition_id = last_sequence % active_memtables_.size();
        active_memtable_mutexs_[partition_id]->lock();
        MemTable *table = active_memtables_[partition_id];

        if (table->ApproximateMemoryUsage() <= options_.write_buffer_size) {
            table->Add(last_sequence, ValueType::kTypeValue, key, val);
            if (table_locator_ != nullptr) {
                TableLocation *loc = new TableLocation;
                loc->memtable_id = table->memtableid();
                table_locator_->Insert(key, loc, 1, &DeleteEntry);
            }
            active_memtable_mutexs_[partition_id]->unlock();
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("#### Put key {} in table {}", key.ToString(),
                               table->memtableid());
            return Status::OK();
        }

        // Table is full.
        mutex_.Lock();
        if (imms_.size() < options_.num_memtables - 1 &&
            versions_->NumLevelFiles(0) <
            options_.l0_stop_writes_trigger) {
            if (table->state() == MemTableState::MEMTABLE_INIT) {
                table->set_state(MemTableState::MEMTABLE_FULL);
                imms_.push_back(table);
                nimms_wait_for_compaction_ += 1;
                table = new MemTable(internal_comparator_, memtable_id_,
                                     db_profiler_);
                table->Ref();
                RDMA_ASSERT(memtable_id_ < MAX_LIVE_MEMTABLES);
                versions_->mid_table_mapping_[memtable_id_].SetMemTable(table);
                memtable_id_++;
                active_memtables_[partition_id] = table;
            }
            table = active_memtables_[partition_id];
            mutex_.Unlock();
            table->Add(last_sequence, ValueType::kTypeValue, key, val);

            if (table_locator_ != nullptr) {
                TableLocation *loc = new TableLocation;
                loc->memtable_id = table->memtableid();
                table_locator_->Insert(key, loc, 1, &DeleteEntry);
            }

            active_memtable_mutexs_[partition_id]->unlock();
            MaybeScheduleCompaction(options.thread_id + 100);
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("#### Put key {} in table {}", key.ToString(),
                               table->memtableid());
            return Status::OK();
        }

        // Must wait. Unlock the memtable mutex first.
        active_memtable_mutexs_[partition_id]->unlock();
        bool wait = false;
        while (true) {
            mutex_.AssertHeld();
            if (imms_.size() >= options_.num_memtables - 1) {
                // We have filled up all memtables, but the previous
                // one is still being compacted, so we wait.
                Log(options_.info_log,
                    "Current memtable full; Make room waiting...\n");
                background_work_finished_signal_.Wait();
                wait = true;
            } else if (versions_->NumLevelFiles(0) >=
                       options_.l0_stop_writes_trigger) {
                // There are too many level-0 files.
                Log(options_.info_log,
                    "Too many L0 files; Make room waiting...\n");
                background_work_finished_signal_.Wait();
                wait = true;
            } else {
                if (wait) {
                    Log(options_.info_log, "Make room; resuming...\n");
                }
                break;
            }
        }

        assert(imms_.size() < options_.num_memtables - 1 &&
               versions_->NumLevelFiles(0) <
               options_.l0_stop_writes_trigger);
        mutex_.Unlock();

        active_memtable_mutexs_[partition_id]->lock();
        mutex_.Lock();
        table = active_memtables_[partition_id];

        if (table->state() == MemTableState::MEMTABLE_INIT) {
            table->set_state(MemTableState::MEMTABLE_FULL);
            imms_.push_back(table);
            nimms_wait_for_compaction_ += 1;
            table = new MemTable(internal_comparator_, memtable_id_,
                                 db_profiler_);
            table->Ref();
            RDMA_ASSERT(memtable_id_ < MAX_LIVE_MEMTABLES);
            versions_->mid_table_mapping_[memtable_id_].SetMemTable(table);

            memtable_id_++;
            active_memtables_[partition_id] = table;
        }
        mutex_.Unlock();
        table->Add(last_sequence, ValueType::kTypeValue, key, val);

        if (table_locator_ != nullptr) {
            TableLocation *loc = new TableLocation;
            loc->memtable_id = table->memtableid();
            table_locator_->Insert(key, loc, 1, &DeleteEntry);
        }
        active_memtable_mutexs_[partition_id]->unlock();
        MaybeScheduleCompaction(options.thread_id + 100);
        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format("#### Put key {} in table {}", key.ToString(),
                           table->memtableid());
        return Status::OK();
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
            char buf[200];
            snprintf(buf, sizeof(buf),
                     "                               Compactions\n"
                     "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                     "--------------------------------------------------\n");
            value->append(buf);
            for (int level = 0; level < config::kNumLevels; level++) {
                int files = versions_->NumLevelFiles(level);
                if (stats_[level].micros > 0 || files > 0) {
                    snprintf(buf, sizeof(buf),
                             "%3d %8d %8.0f %9.0f %8.0f %9.0f\n", level,
                             files, versions_->NumLevelBytes(level) / 1048576.0,
                             stats_[level].micros / 1e6,
                             stats_[level].bytes_read / 1048576.0,
                             stats_[level].bytes_written / 1048576.0);
                    value->append(buf);
                }
            }
            return true;
        } else if (in == "sstables") {
            *value = versions_->current()->DebugString();
            return true;
        } else if (in == "approximate-memory-usage") {
            size_t total_usage = options_.block_cache->TotalCharge();
            for (auto mem : active_memtables_) {
                total_usage += mem->ApproximateMemoryUsage();
            }
            for (auto imm : imms_) {
                if (!imm) {
                    continue;
                }
                total_usage += imm->ApproximateMemoryUsage();
            }
            char buf[50];
            snprintf(buf, sizeof(buf), "%llu",
                     static_cast<unsigned long long>(total_usage));
            value->append(buf);
            return true;
        }

        return false;
    }

    void
    DBImpl::GetApproximateSizes(const Range *range, int n, uint64_t *sizes) {
        // TODO(opt): better implementation
        MutexLock l(&mutex_);
        Version *v = versions_->current();
        v->Ref();

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

        v->Unref();
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
        VersionEdit edit;
        // Recover handles create_if_missing, error_if_exists
        bool save_manifest = false;
        Status s = impl->Recover(&edit, &save_manifest);
        if (s.ok()) {
            // Create new log and a corresponding memtable.
            uint64_t new_log_number = impl->versions_->NewFileNumber();
            WritableFile *lfile;
            s = options.env->NewWritableFile(
                    LogFileName(dbname, new_log_number),
                    {.level = -1},
                    &lfile);
            impl->current_log_file_name_ = LogFileName(dbname, new_log_number);
            if (s.ok()) {
                edit.SetLogNumber(new_log_number);

                for (int i = 0; i < options.num_memtable_partitions; i++) {
                    MemTable *table = new MemTable(impl->internal_comparator_,
                                                   impl->memtable_id_,
                                                   impl->db_profiler_);
                    table->Ref();
                    RDMA_ASSERT(impl->memtable_id_ < MAX_LIVE_MEMTABLES);
                    impl->versions_->mid_table_mapping_[impl->memtable_id_].SetMemTable(
                            table);
                    impl->memtable_id_++;
                    impl->active_memtables_.push_back(table);
                    impl->active_memtable_mutexs_.push_back(new std::mutex);
                }
            }
        }
        if (s.ok() && save_manifest) {
            edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
            edit.SetLogNumber(0);
            s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
        }
        if (s.ok()) {
//            impl->DeleteObsoleteFiles();
//            impl->MaybeScheduleCompaction();
        }
        impl->mutex_.Unlock();
        if (s.ok()) {
            *dbptr = impl;
        } else {
            delete impl;
        }
        return s;
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