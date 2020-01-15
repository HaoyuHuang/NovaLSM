
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//
#include "port/port.h"
#include "leveldb/env.h"
#include "db/version_set.h"
#include "db/compaction.h"
#include <thread>
#include "mc.h"

namespace nova {

    leveldb::Status
    MemoryComponent::MCTableCache::FindTable(
            const nova::GlobalSSTableHandle &th,
            leveldb::Cache::Handle **handle) {
//
//        SSTableContext *ctx = mc_->sstables_l0_[th];
//        char buf[th.size()];
//        th.Encode(buf);
//        leveldb::Slice key(buf, sizeof(buf));
//        leveldb::TableAndFile *tf = new leveldb::TableAndFile;
//        tf->file = ctx->table->backing_file();
//        tf->table = ctx->table;
//        *handle = cache_->Insert(key, tf, 1, &leveldb::DeleteEntry);
    }

    MemoryComponent::MemoryComponent(uint32_t mc_id, leveldb::Env *env,
                                     NovaMemManager *mem_manager,
                                     const leveldb::Options &options,
                                     const leveldb::InternalKeyComparator internal_comparator)
            :
            mc_id_(mc_id), env_(env), mem_manager_(mem_manager),
            options_(options), internal_comparator_(internal_comparator),
            table_cache_("", options, 1000, nullptr) {
        current_sstable_group_.store(
                new std::map<GlobalSSTableHandle, leveldb::Table *>());
        current_log_bufs_.store(new std::vector<leveldb::Slice>());
        current_log_records_.store(
                new std::map<GlobalSSTableHandle, NovaList<LogRecord>>());

        seal_log_file_worker_ = new SealLogFileWorker(this);
        trim_log_file_worker_ = new TrimLogFilesWorker(this);
        compact_sstables_worker_ = new CompactSSTablesWorker(this);

        threads_.push_back(
                std::thread(&SealLogFileWorker::Run, seal_log_file_worker_));
        threads_.push_back(
                std::thread(&TrimLogFilesWorker::Run, trim_log_file_worker_));
        threads_.push_back(
                std::thread(&CompactSSTablesWorker::Run,
                            compact_sstables_worker_));
    }

    void MemoryComponent::SealLogFileWorker::DoWork(
            Request *request) {
        SealLogFileRequest *req = static_cast<SealLogFileRequest *>(request);
        mc_->SealLogFile(req->log_records, req->log_bufs);
        delete req->log_records;
        delete req->log_bufs;
    }

    void MemoryComponent::TrimLogFilesWorker::DoWork(
            Request *request) {
        mc_->TrimLogFiles();
    }

    void MemoryComponent::CompactSSTablesWorker::DoWork(
            Request *request) {
        CompactSSTablesRequest *req = static_cast<CompactSSTablesRequest *>(request);
        mc_->CompactSSTables(req->sstables, req->latest_seq);
    }

    char *MemoryComponent::AllocateBuf(uint32_t size) {
//        uint32_t scid = mem_manager_->slabclassid(size);
//        if (scid == MAX_NUMBER_OF_SLAB_CLASSES) {
//            return {};
//        }
//        char *buf = mem_manager_->ItemAlloc(scid);
//        if (buf == nullptr) {
//            RDMA_ASSERT(false) << "Evict not supported";
//        }
//        return buf;
    }

    void MemoryComponent::FreeBuf(char *buf, uint32_t size) {
//        uint32_t scid = mem_manager_->slabclassid(size);
//        if (scid == MAX_NUMBER_OF_SLAB_CLASSES) {
//            return;
//        }
//        mem_manager_->FreeItem(buf, scid);
    }

    leveldb::Status
    MemoryComponent::Get(GlobalBlockHandle *handles, GetResult *results,
                         int size) {
//        int nkey = GlobalBlockHandle::CacheKeySize();
//        char cache_key[nkey];
//        for (int i = 0; i < size; i++) {
//            handles[i].CacheKey(cache_key);
//            nova::GetResult result = mem_manager_->LocalGet(cache_key, nkey,
//                                                            true);
//            if (!result.index_entry.empty()) {
//                results[i].hit = true;
//                results[i].index = result.index_entry;
//                results[i].data = result.data_entry;
//            } else {
//                // Cache miss.
//                sstable_mutex_.Lock();
//                auto it = sstables_l0_.find(handles[i].table_handle);
//                sstable_mutex_.Unlock();
//                if (it != sstables_l0_.end()) {
//                    // Found.
//                    results[i].hit = true;
//                    it->second->mutex.Lock();
////                    leveldb::ReadBlock(it->second->table->backing_file(), {},
////                                       handles[i].block_handle,
////                                       &(results[i].block));
//                    // Insert the block into the cache.
//                    mem_manager_->LocalPut(cache_key, nkey,
//                                           (char *) results[i].block.data.data(),
//                                           results[i].block.data.size(), true,
//                                           true);
//                    it->second->mutex.Unlock();
//                }
//            }
//        }
    }

    leveldb::Status
    MemoryComponent::Insert(GlobalBlockHandle *handles, char **blocks,
                            int size) {
//        int nkey = GlobalBlockHandle::CacheKeySize();
//        char cache_key[nkey];
//        for (int i = 0; i < size; i++) {
//            handles[i].CacheKey(cache_key);
//            mem_manager_->LocalPut(cache_key, nkey,
//                                   blocks[i],
//                                   handles[i].block_handle.size(), true, true);
//        }
    }

    leveldb::Status
    MemoryComponent::Flush(const GlobalSSTableHandle &handle,
                           leveldb::SequenceNumber last_seq, char *sstable,
                           uint32_t size) {
        leveldb::Table *table = nullptr;
        leveldb::MemReadableFile readable_file("tmp", sstable, size);
        leveldb::Status status = leveldb::Table::Open(options_, &readable_file,
                                                      size, 0, handle.table_id,
                                                      &table,
                                                      nullptr);
        RDMA_ASSERT(status.ok());

        sstable_mutex_.Lock();
        SSTableContext *ctx = new SSTableContext;
        ctx->table = table;

        sstables_l0_.insert(std::make_pair(handle, ctx));
        std::map<GlobalSSTableHandle, leveldb::Table *> *tables = current_sstable_group_.load();
        tables->insert(std::make_pair(handle, table));

        if (current_sstable_group_.load()->size() ==
            options_.l0_sstables_per_group) {
            current_sstable_group_.store(
                    new std::map<GlobalSSTableHandle, leveldb::Table *>());
            sstable_mutex_.Unlock();

            // The group is full. Merge them.
            CompactSSTablesRequest *req = new CompactSSTablesRequest;
            req->sstables = tables;
            req->latest_seq = last_seq;
            compact_sstables_worker_->AddRequest(req);
            compact_sstables_worker_->WakeupForWork();
            return leveldb::Status::OK();
        }
        sstable_mutex_.Unlock();
        return leveldb::Status::OK();
    }

    void MemoryComponent::CompactSSTables(
            std::map<GlobalSSTableHandle, leveldb::Table *> *tables,
            leveldb::SequenceNumber last_seq) {
        // TODO.
//        leveldb::Compaction *compaction = new leveldb::Compaction(&options_, 0);
//        leveldb::CompactionState *state = new leveldb::CompactionState(
//                compaction, env_, "", options_,
//                table_cache_, internal_comparator_,
//                last_seq, nullptr);
//        state->DoCompactionWork();
    }

    leveldb::Status
    MemoryComponent::FlushedToDC(const GlobalSSTableHandle &handle) {
//        sstable_mutex_.Lock();
//        tables_flushed_to_dc_.insert(handle);
//        auto table = sstables_l0_.find(handle);
//        sstables_l0_.erase(table);
//        table_cache_.Evict(handle);
//        sstable_mutex_.Unlock();
//
//        // Free its backing memory.
//        table->second->mutex.Lock();
//        leveldb::MemReadableFile *backing_file = static_cast<leveldb::MemReadableFile *>(table->second->table->backing_file());
//        leveldb::Slice backing_mem = backing_file->backing_mem();
//        FreeBuf((char *) backing_mem.data(), backing_mem.size());
//        table->second->mutex.Unlock();
//
//        // Remove its log records.
//        trim_log_file_worker_->AddRequest(new Request());
//        trim_log_file_worker_->WakeupForWork();
    }

    leveldb::Status MemoryComponent::AppendLogRecord(char *log_buf, int size) {
        log_mutex_.Lock();
        auto log_records = current_log_records_.load();
        auto log_bufs = current_log_bufs_.load();
        log_bufs->push_back(leveldb::Slice(log_buf, size));

        char *ptr = log_buf;
        int read_bytes = 0;
        while (read_bytes + 4 < size) {
            LogRecord record;
            if (!record.Decode(ptr).ok()) {
                break;
            }
            ptr += record.backing_mem.size();
            read_bytes += record.backing_mem.size();
            (*log_records)[record.table_handle].append(record);
            log_file_size_ += record.backing_mem.size();
        }

        if (log_file_size_ > options_.max_log_file_size) {
            log_id_ += 1;
            current_log_records_.store(
                    new std::map<GlobalSSTableHandle, NovaList<LogRecord>>());
            current_log_bufs_.store(new std::vector<leveldb::Slice>());
            log_mutex_.Unlock();

            SealLogFileRequest *req = new SealLogFileRequest;
            req->log_records = log_records;
            req->log_bufs = log_bufs;
            seal_log_file_worker_->AddRequest(req);
            seal_log_file_worker_->WakeupForWork();
            return leveldb::Status::OK();
        }
        log_mutex_.Unlock();
        return leveldb::Status::OK();
    }


    void MemoryComponent::SealLogFile(
            std::map<GlobalSSTableHandle, NovaList<LogRecord>> *log_records,
            std::vector<leveldb::Slice> *log_bufs) {
        // Trim log records that has been flushed.
//        sstable_mutex_.Lock();
//        std::set<GlobalSSTableHandle> tables_flushed_to_dc = tables_flushed_to_dc_;
//        sstable_mutex_.Unlock();
//
//        char *base = AllocateBuf(options_.max_log_file_size);
//        leveldb::MemWritableFile writable_file("tmp", base,
//                                               options_.max_log_file_size);
//        leveldb::log::Writer writer(&writable_file);
//        struct GlobalLogFileHandle log_handle = {
//                // TODO: Put Configuration id here.
//                .configuration_id = 1,
//                .mc_id = mc_id_,
//                .log_id = log_id_
//        };
//        LogFile *log_file = new LogFile(log_handle, base,
//                                        options_.max_log_file_size);
//        int writte_log_records = 0;
//        for (auto it = log_records->begin();
//             it != log_records->end(); it++) {
////            uint32_t file_offset = writer.file_offset();
////            uint32_t nrecords = it->second.size();
////            const GlobalSSTableHandle &table_handle = it->first;
////
////            if (tables_flushed_to_dc.find(table_handle) !=
////                tables_flushed_to_dc.end()) {
////                // Already flushed.
////                continue;
////            }
////            writte_log_records += nrecords;
////            writer.AddIndex(file_offset, nrecords, table_handle);
//            LogFile::TableIndex table_index = {
//                    .handle = table_handle,
//                    .file_offset = file_offset,
//                    .nrecords = nrecords
//            };
//
//            log_file->AddIndex(table_index);
//            for (int i = 0; i < it->second.size(); i++) {
//                writer.AddRecord(it->second.value(i).backing_mem);
//            }
//        }
//
//        if (writte_log_records > 0) {
//            writer.WriteFooter();
//
//            // Insert into log file.
//            log_mutex_.Lock();
//            log_files_.insert(std::make_pair(log_handle, log_file));
//            log_mutex_.Unlock();
//        } else {
//            // Discard if the log file is empty.
//        }
//
//        // Release memory for log buffers.
//        uint32_t scid = mem_manager_->slabclassid(log_bufs->begin()->size());
//        mem_manager_->FreeItems(*log_bufs, scid);
//
//        // Clean up
//        sstable_mutex_.Lock();
//        tables_flushed_to_dc_.insert(gced_tables_log_records_.begin(),
//                                     gced_tables_log_records_.end());
//        gced_tables_log_records_.clear();
//        sstable_mutex_.Unlock();
    }

    void MemoryComponent::DeleteLogFile(const GlobalLogFileHandle &handle) {
        log_mutex_.Lock();
        log_files_.erase(handle);
        log_mutex_.Unlock();
    }

    void
    MemoryComponent::TrimLogFiles() {
        // Make a copy of the data.
        sstable_mutex_.Lock();
        std::set<GlobalSSTableHandle> tables_flushed_to_dc = tables_flushed_to_dc_;
        sstable_mutex_.Unlock();

        log_mutex_.Lock();
        std::map<GlobalLogFileHandle, LogFile *> log_files = log_files_;
        log_mutex_.Unlock();

        // These tables are safe to remove from 'tables_flushed_to_dc_'.
        // All its log files are removed or the log file contains future generations of its SSTable.
        std::set<GlobalSSTableHandle> gc_tables_flushed_to_dc;
        std::vector<GlobalLogFileHandle> gc_log_files;

        // Lazy: Delete a log file if all its SSTables are flushed.
        // Eager: Delete log records if its SSTables are flushed.
        for (auto it = log_files.begin(); it != log_files.end(); it++) {
            LogFile *log_file = it->second;
            const std::vector<LogFile::TableIndex> &index = log_file->table_index();
            bool all_flushed = true;
            std::set<GlobalSSTableHandle> gc_tables;
            for (auto iit = index.begin(); iit != index.end(); iit++) {
                if (tables_flushed_to_dc.find(iit->handle) ==
                    tables_flushed_to_dc.end()) {
                    all_flushed = false;
                    break;
                }
                gc_tables.insert(iit->handle);
            }

            if (all_flushed) {
                gc_log_files.push_back(it->first);
                // The current unsealed log records may contain this table. Defer this cleanup to SealLogFile
                gc_tables_flushed_to_dc.insert(gc_tables.begin(),
                                               gc_tables.end());
            }

            if (options_.mc_enable_eager_trim_log_records) {
                // TODO: Remove log records eagerly.
            }
        }

        // Cleanup.
        sstable_mutex_.Lock();
        gced_tables_log_records_.insert(gc_tables_flushed_to_dc.begin(),
                                        gc_tables_flushed_to_dc.end());
        sstable_mutex_.Unlock();

        log_mutex_.Lock();
        for (auto it = gc_log_files.begin(); it != gc_log_files.end(); it++) {
            log_files_.erase(*it);
        }
        log_mutex_.Unlock();
    }

    void MemoryComponent::Delete(GlobalSSTableHandle *handles, int size) {

    }

}