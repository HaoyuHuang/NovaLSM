
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "compaction.h"
#include "filename.h"

namespace leveldb {
    void
    FetchMetadataFilesInParallel(const std::vector<const FileMetaData *> &files,
                                 const std::string &dbname,
                                 const Options &options,
                                 StoCBlockClient *client,
                                 Env *env) {
        uint32_t fetched_files = 0;
        std::vector<const FileMetaData *> batch;
        for (int i = 0; i < files.size(); i++) {
            if (batch.size() == FETCH_METADATA_BATCH_SIZE &&
                files.size() - i > FETCH_METADATA_BATCH_SIZE) {
                fetched_files += batch.size();
                FetchMetadataFiles(batch, dbname, options, client, env);
                batch.clear();
            }
            batch.push_back(files[i]);
        }
        if (!batch.empty()) {
            fetched_files += batch.size();
            FetchMetadataFiles(batch, dbname, options, client, env);
        }
        NOVA_ASSERT(fetched_files == files.size());
    }

    void
    FetchMetadataFiles(const std::vector<const FileMetaData *> &files,
                       const std::string &dbname, const Options &options,
                       StoCBlockClient *client, Env *env) {
        // Fetch all metadata files in parallel.
        char *backing_mems[files.size() * nova::NovaConfig::config->number_of_sstable_metadata_replicas];
        int index = 0;
        for (int i = 0; i < files.size(); i++) {
            auto meta = files[i];
            for (int replica_id = 0; replica_id < meta->block_replica_handles.size(); replica_id++) {
                std::string filename = TableFileName(dbname, meta->number, FileInternalType::kFileMetadata, replica_id);
                const StoCBlockHandle &meta_handle = meta->block_replica_handles[replica_id].meta_block_handle;
                uint32_t backing_scid = options.mem_manager->slabclassid(0, meta_handle.size);
                char *backing_buf = options.mem_manager->ItemAlloc(0, backing_scid);
                memset(backing_buf, 0, meta_handle.size);
                NOVA_LOG(rdmaio::DEBUG)
                    << fmt::format("Fetch metadata blocks {} handle:{}", filename, meta->DebugString());
                uint32_t req_id = client->InitiateReadDataBlock(meta_handle, 0, meta_handle.size, backing_buf,
                                                                meta_handle.size, "", false);
                backing_mems[index] = backing_buf;
                index++;
            }
        }

        for (int i = 0; i < files.size(); i++) {
            auto meta = files[i];
            for (int replica_id = 0; replica_id < meta->block_replica_handles.size(); replica_id++) {
                client->Wait();
            }
        }
        index = 0;
        for (int i = 0; i < files.size(); i++) {
            auto meta = files[i];
            for (int replica_id = 0; replica_id < meta->block_replica_handles.size(); replica_id++) {
                char *backing_buf = backing_mems[index];
                const StoCBlockHandle &meta_handle = meta->block_replica_handles[replica_id].meta_block_handle;
                uint32_t backing_scid = options.mem_manager->slabclassid(0, meta_handle.size);
                WritableFile *writable_file;
                EnvFileMetadata env_meta = {};
                auto sstablename = TableFileName(dbname, meta->number, FileInternalType::kFileData, replica_id);
                Status s = env->NewWritableFile(sstablename, env_meta, &writable_file);
                NOVA_ASSERT(s.ok());
                Slice sstable_meta(backing_buf, meta_handle.size);
                s = writable_file->Append(sstable_meta);
                NOVA_ASSERT(s.ok());
                s = writable_file->Flush();
                NOVA_ASSERT(s.ok());
                s = writable_file->Sync();
                NOVA_ASSERT(s.ok());
                s = writable_file->Close();
                NOVA_ASSERT(s.ok());
                delete writable_file;
                writable_file = nullptr;
                options.mem_manager->FreeItem(0, backing_buf, backing_scid);
                index++;
            }
        }
    }

    Compaction::Compaction(VersionFileMap *input_version,
                           const InternalKeyComparator *icmp,
                           const Options *options, int level, int target_level)
            : level_(level),
              target_level_(target_level),
              icmp_(icmp),
              options_(options),
              max_output_file_size_(options->max_file_size),
              input_version_(input_version),
              grandparent_index_(0),
              seen_key_(false),
              overlapped_bytes_(0) {
        is_completed_ = false;
        level_ptrs_.resize(options->level);
        for (int i = 0; i < options->level; i++) {
            level_ptrs_[i] = 0;
        }
    }

    std::string Compaction::DebugString(const Comparator *user_comparator) {
        Slice smallest = {};
        Slice largest = {};

        std::string files;
        for (int which = 0; which < 2; which++) {
            for (auto file : inputs_[which]) {
                if (smallest.empty() ||
                    user_comparator->Compare(file->smallest.user_key(),
                                             smallest) < 0) {
                    smallest = file->smallest.user_key();
                }
                if (largest.empty() ||
                    user_comparator->Compare(file->largest.user_key(),
                                             largest) > 0) {
                    largest = file->largest.user_key();
                }
                files += file->ShortDebugString();
                files += ",";
            }
        }
        std::string debug = fmt::format("{}@{} + {}@{} s:{} l:{} {} {}",
                                        inputs_[0].size(), level_,
                                        inputs_[1].size(), target_level_,
                                        smallest.ToString(), largest.ToString(),
                                        files, grandparents_.size());
        return debug;
    }

    bool Compaction::IsTrivialMove() const {
//         Avoid a move if there is lots of overlapping grandparent data.
//         Otherwise, the move could create a parent file that will require
//         a very expensive merge later on.
        return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
                level_ != target_level_);
//        &&
//                TotalFileSize(grandparents_) <=
//                MaxGrandParentOverlapBytes(options_));
    }

    void Compaction::AddInputDeletions(VersionEdit *edit) {
        for (int which = 0; which < 2; which++) {
            for (size_t i = 0; i < inputs_[which].size(); i++) {
                int delete_level = level_ + which;
                auto *f = inputs_[which][i];
                edit->DeleteFile(delete_level, f->number);
            }
        }
    }

    bool Compaction::ShouldStopBefore(const Slice &internal_key) {
        // Scan to find earliest grandparent file that contains key.
        while (grandparent_index_ < grandparents_.size() &&
               icmp_->Compare(internal_key,
                              grandparents_[grandparent_index_]->largest.Encode()) >
               0) {
            if (seen_key_) {
                overlapped_bytes_ += 1; //grandparents_[grandparent_index_]->file_size;
            }
            grandparent_index_++;
        }
        seen_key_ = true;
        int max_overlap = 5;
        max_overlap = std::min(max_overlap, (int) grandparents_.size() / 4);
        max_overlap = std::max(max_overlap, 1);
        if (overlapped_bytes_ >= max_overlap) {
            // Too much overlap for current output; start new output
            overlapped_bytes_ = 0;
            return true;
        } else {
            return false;
        }
    }

    CompactionStats CompactionState::BuildStats() {
        CompactionStats stats;
        stats.input_source.num_files = compaction->num_input_files(0);
        stats.input_source.level = compaction->level();
        stats.input_source.file_size = compaction->num_input_file_sizes(0);

        stats.input_target.num_files = compaction->num_input_files(1);
        stats.input_target.level = compaction->target_level();
        stats.input_target.file_size = compaction->num_input_file_sizes(1);
        return stats;
    }


    CompactionJob::CompactionJob(std::function<uint64_t(void)> &fn_generator,
                                 leveldb::Env *env, const std::string &dbname,
                                 const leveldb::Comparator *user_comparator,
                                 const leveldb::Options &options,
                                 EnvBGThread *bg_thread,
                                 TableCache *table_cache)
            : fn_generator_(fn_generator), env_(env), dbname_(dbname),
              user_comparator_(
                      user_comparator), options_(options),
              bg_thread_(bg_thread), table_cache_(table_cache) {
    }

    Status CompactionJob::OpenCompactionOutputFile(CompactionState *compact) {
        assert(compact != nullptr);
        assert(compact->builder == nullptr);
        uint64_t file_number;
        {
            file_number = fn_generator_();
            FileMetaData out;
            if (file_number == 0) {
                std::string str;
                if (compact->compaction) {
                    str = compact->compaction->DebugString(user_comparator_);
                }
                NOVA_ASSERT(false) << str;
            }
            out.number = file_number;
            out.smallest.Clear();
            out.largest.Clear();
            compact->outputs.push_back(out);
        }
        // Make the output file
        MemManager *mem_manager = bg_thread_->mem_manager();
        std::string filename = TableFileName(dbname_, file_number, FileInternalType::kFileData, 0);
        StoCWritableFileClient *stoc_writable_file = new StoCWritableFileClient(
                options_.env,
                options_,
                file_number,
                mem_manager,
                bg_thread_->stoc_client(),
                dbname_,
                bg_thread_->thread_id(),
                options_.max_stoc_file_size,
                bg_thread_->rand_seed(),
                filename);
        compact->outfile = new MemWritableFile(stoc_writable_file);
        compact->builder = new TableBuilder(options_, compact->outfile);
        return Status::OK();
    }

    Status
    CompactionJob::FinishCompactionOutputFile(const ParsedInternalKey &ik,
                                              CompactionState *compact,
                                              Iterator *input) {
        assert(compact != nullptr);
        assert(compact->outfile != nullptr);
        assert(compact->builder != nullptr);

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

        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("Close table-{} at {} bytes", output_number,
                           current_bytes);
        FileMetaData meta;
        meta.number = output_number;
        meta.file_size = current_bytes;
        meta.smallest = compact->current_output()->smallest;
        meta.largest = compact->current_output()->largest;
        // Set meta in order to flush to the corresponding DC node.
        StoCWritableFileClient *mem_file = static_cast<StoCWritableFileClient *>(compact->outfile->mem_file());
        mem_file->set_meta(meta);
        mem_file->set_num_data_blocks(current_data_blocks);

        // Finish and check for file errors
        NOVA_ASSERT(s.ok()) << s.ToString();
        s = compact->outfile->Sync();
        s = compact->outfile->Close();

        mem_file->WaitForPersistingDataBlocks();
        {
            FileMetaData *output = compact->current_output();
            output->converted_file_size = mem_file->Finalize();
            output->block_replica_handles = mem_file->replicas();
            output->parity_block_handle = mem_file->parity_block_handle();
            mem_file->Validate(output->block_replica_handles, output->parity_block_handle);
            delete mem_file;
            mem_file = nullptr;
            delete compact->outfile;
            compact->outfile = nullptr;
        }
        return s;
    }

    Status
    CompactionJob::CompactTables(CompactionState *compact,
                                 Iterator *input,
                                 CompactionStats *stats, bool drop_duplicates,
                                 CompactInputType input_type,
                                 CompactOutputType output_type,
                                 const std::function<void(
                                         const ParsedInternalKey &ikey,
                                         const Slice &value)> &add_to_memtable) {
        const uint64_t start_micros = env_->NowMicros();
        std::string output;
        if (input_type == CompactInputType::kCompactInputMemTables) {
            output = fmt::format(
                    "bg[{}] Flushing {} memtables",
                    bg_thread_->thread_id(),
                    stats->input_source.num_files);
        } else {
            output = fmt::format(
                    "bg[{}] Major Compacting {}@{} + {}@{} files",
                    bg_thread_->thread_id(),
                    stats->input_source.num_files,
                    stats->input_source.level,
                    stats->input_target.num_files,
                    stats->input_target.level);
            Log(options_.info_log, "%s", output.c_str());
            NOVA_LOG(rdmaio::INFO) << output;
        }

        assert(compact->builder == nullptr);
        assert(compact->outfile == nullptr);
        assert(compact->outputs.empty());

        input->SeekToFirst();
        Status status;
        ParsedInternalKey ikey;
        std::string current_user_key;
        bool has_current_user_key = false;
        SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
        std::vector<std::string> keys;
        uint64_t memtable_size = 0;
        while (input->Valid()) {
            Slice key = input->key();
            NOVA_ASSERT(ParseInternalKey(key, &ikey));

            if (output_type == kCompactOutputSSTables &&
                compact->ShouldStopBefore(key, user_comparator_) &&
                compact->builder != nullptr &&
                compact->builder->NumEntries() > 0) {
                status = FinishCompactionOutputFile(ikey, compact, input);
                if (!status.ok()) {
                    break;
                }
            }

            // Handle key/value, add to state, etc.
            bool drop = false;
            if (!has_current_user_key ||
                user_comparator_->Compare(ikey.user_key,
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
            }
            last_sequence_for_key = ikey.sequence;
#if 0
            Log(options_.info_log,
                "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
                "%d smallest_snapshot: %d",
                ikey.user_key.ToString().c_str(),
                (int)ikey.sequence, ikey.type, kTypeValue, drop,
                compact->compaction->IsBaseLevelForKey(ikey.user_key),
                (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif
            if (drop && drop_duplicates) {
//                RDMA_LOG(rdmaio::DEBUG)
//                    << fmt::format("drop key-{}", ikey.FullDebugString());
                input->Next();
                continue;
            } else {
                // Open output file if necessary
                if (output_type == kCompactOutputSSTables &&
                    compact->builder == nullptr) {
                    status = OpenCompactionOutputFile(compact);
                    if (!status.ok()) {
                        break;
                    }
                }
                if (output_type == kCompactOutputSSTables &&
                    compact->builder->NumEntries() == 0) {
                    compact->current_output()->smallest.DecodeFrom(key);
                }
//                RDMA_LOG(rdmaio::DEBUG)
//                    << fmt::format("add key-{}", ikey.FullDebugString());
//                keys.push_back(ikey.DebugString());
                if (output_type == kCompactOutputSSTables) {
                    compact->current_output()->largest.DecodeFrom(key);
                    if (!compact->builder->Add(key, input->value())) {
                        std::string added_keys;
                        for (auto &k : keys) {
                            added_keys += k;
                            added_keys += "\n";
                        }
                        NOVA_ASSERT(false) << fmt::format("{}\n {}",
                                                          compact->compaction->DebugString(
                                                                  user_comparator_),
                                                          added_keys);
                    }
                } else {
                    NOVA_ASSERT(output_type == kCompactOutputMemTables);
                    add_to_memtable(ikey, input->value());
                    memtable_size +=
                            input->key().size() + input->value().size();
                }

                // Close output file if it is big enough
                if (output_type == kCompactOutputSSTables &&
                    compact->builder->FileSize() >= options_.max_file_size) {
                    status = FinishCompactionOutputFile(ikey, compact, input);
                    if (!status.ok()) {
                        break;
                    }
                }
            }
            input->Next();
        }

        if (output_type == kCompactOutputSSTables && status.ok() &&
            compact->builder != nullptr) {
            status = FinishCompactionOutputFile(ikey, compact, input);
        }
        if (status.ok()) {
            status = input->status();
        }
        delete input;
        input = nullptr;

        stats->micros = env_->NowMicros() - start_micros;
        if (output_type == CompactOutputType::kCompactOutputSSTables) {
            for (size_t i = 0; i < compact->outputs.size(); i++) {
                stats->output.file_size += compact->outputs[i].file_size;
                stats->output.num_files += 1;
            }
        } else {
            stats->output.num_files = 1;
            stats->output.file_size = memtable_size;
        }

        if (input_type == CompactInputType::kCompactInputMemTables) {
            output = fmt::format(
                    "bg[{}] Flushing {} memtables => {} files {} bytes {}",
                    bg_thread_->thread_id(),
                    stats->input_source.num_files,
                    stats->output.num_files,
                    stats->output.file_size,
                    output_type == kCompactOutputMemTables ? "memtable"
                                                           : "sstable");
        } else {
            const int src_level = compact->compaction->level();
            const int dest_level = compact->compaction->target_level();
            output = fmt::format(
                    "bg[{}]: Major Compacted {}@{} + {}@{} files => {} bytes",
                    bg_thread_->thread_id(),
                    compact->compaction->num_input_files(0),
                    src_level,
                    compact->compaction->num_input_files(1),
                    dest_level,
                    compact->total_bytes);
            NOVA_LOG(rdmaio::INFO) << output;
            Log(options_.info_log, "%s", output.c_str());
        }

        if (input_type == CompactInputType::kCompactInputMemTables) {
            output = fmt::format(
                    "Flushing memtables stats,{},{},{},{},{}",
                    stats->input_source.num_files +
                    stats->input_target.num_files,
                    stats->input_source.file_size +
                    stats->input_target.file_size,
                    stats->output.num_files, stats->output.file_size,
                    stats->micros);
        } else {
            output = fmt::format("Major compaction stats,{},{},{},{},{}",
                                 stats->input_source.num_files +
                                 stats->input_target.num_files,
                                 stats->input_source.file_size +
                                 stats->input_target.file_size,
                                 stats->output.num_files,
                                 stats->output.file_size,
                                 stats->micros);
            NOVA_LOG(rdmaio::INFO) << output;
            Log(options_.info_log, "%s", output.c_str());
        }

        // Remove input files from table cache.
        if (table_cache_) {
            if (compact->compaction) {
                for (int which = 0; which < 2; which++) {
                    for (int i = 0;
                         i < compact->compaction->inputs_[which].size(); i++) {
                        auto f = compact->compaction->inputs_[which][i];
                        table_cache_->Evict(f->number, true);
                    }
                }
            }
        }
        if (compact->compaction) {
            compact->compaction->is_completed_ = true;
            if (compact->compaction->complete_signal_) {
                sem_post(compact->compaction->complete_signal_);
            }
        }
        return status;
    }
}