
//
// Created by Haoyu Huang on 12/15/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "leveldb/db_profiler.h"
#include "leveldb/env.h"
#include "compaction.h"
#include "filename.h"
#include "table_cache.h"

namespace leveldb {
    void CompactionState::CleanupCompaction() {
        if (builder_ != nullptr) {
            // May happen if we get a shutdown call in the middle of compaction
            builder_->Abandon();
            delete builder_;
        } else {
            assert(outfile_ == nullptr);
        }
        delete outfile_;
        for (size_t i = 0; i < outputs_.size(); i++) {
            const CompactionState::Output &out = outputs_[i];
            pending_outputs_.erase(out.number);
        }
    }

    Status CompactionState::OpenCompactionOutputFile() {
        assert(builder_ == nullptr);
        uint64_t file_number;
        {
//            file_number = versions_->NewFileNumber();
            pending_outputs_.insert(file_number);
            CompactionState::Output out;
            out.number = file_number;
            out.smallest.Clear();
            out.largest.Clear();
            outputs_.push_back(out);
        }

        // Make the output file
        std::string fname = TableFileName(dbname_, file_number);
        Status s = env_->NewWritableFile(fname, {}, &outfile_);
        if (s.ok()) {
            builder_ = new TableBuilder(options_, outfile_);
        }
        return s;
    }

    Status CompactionState::FinishCompactionOutputFile(
            Iterator *input) {
        assert(outfile_ != nullptr);
        assert(builder_ != nullptr);

        const uint64_t output_number = current_output()->number;
        assert(output_number != 0);

        // Check for iterator errors
        Status s = input->status();
        const uint64_t current_entries = builder_->NumEntries();
        if (s.ok()) {
            s = builder_->Finish();
        } else {
            builder_->Abandon();
        }
        const uint64_t current_bytes = builder_->FileSize();
        current_output()->file_size = current_bytes;
        total_bytes_ += current_bytes;
        delete builder_;
        builder_ = nullptr;

        // Finish and check for file errors
        if (s.ok()) {
            s = outfile_->Sync();
        }
        if (s.ok()) {
            s = outfile_->Close();
        }
        delete outfile_;
        outfile_ = nullptr;

        if (s.ok() && current_entries > 0) {
            // Verify that the table is usable
            // TODO
//            Iterator *iter =
//                    table_cache_->NewIterator(AccessCaller::kCompaction,
//                                              ReadOptions(), output_number,
//                                              compaction_->level() + 1,
//                                              current_bytes);
//            s = iter->status();
//            delete iter;
//            if (s.ok()) {
//                Log(options_.info_log,
//                    "Generated table #%llu@%d: %lld keys, %lld bytes",
//                    (unsigned long long) output_number,
//                    compaction_->level(),
//                    (unsigned long long) current_entries,
//                    (unsigned long long) current_bytes);
//            }
        }
        return s;
    }

    Status CompactionState::InstallCompactionResults() {
        Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
            compaction_->num_input_files(0),
            compaction_->level(),
            compaction_->num_input_files(1),
            compaction_->level() + 1,
            static_cast<long long>(total_bytes_));

        // Add compaction outputs
        compaction_->AddInputDeletions(compaction_->edit());
        const int level = compaction_->level();
        for (size_t i = 0; i < outputs_.size(); i++) {
            const CompactionState::Output &out = outputs_[i];
            compaction_->edit()->AddFile(level + 1, out.number,
                                         out.file_size,
                                         out.smallest, out.largest);
        }
    }

    Status CompactionState::DoCompactionWork() {
        return Status::OK();
//        const uint64_t start_micros = env_->NowMicros();
//        int64_t imm_micros = 0;  // Micros spent doing imm_ compaction_s
//
//        Log(options_.info_log, "Compacting %d@%d + %d@%d files",
//            compaction_->num_input_files(0),
//            compaction_->level(),
//            compaction_->num_input_files(1),
//            compaction_->level() + 1);
//
//        assert(builder_ == nullptr);
//        assert(outfile_ == nullptr);
//        Iterator *input = compaction_->MakeInputIterator(options_,
//                                                         internal_comparator_,
//                                                         table_cache_);
//        input->SeekToFirst();
//        Status status;
//        ParsedInternalKey ikey;
//        std::string current_user_key;
//        bool has_current_user_key = false;
//        SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
//        while (input->Valid()) {
//            Slice key = input->key();
//            if (compaction_->ShouldStopBefore(key) &&
//                builder_ != nullptr) {
//                status = FinishCompactionOutputFile(input);
//                if (!status.ok()) {
//                    break;
//                }
//            }
//
//            // Handle key/value, add to state, etc.
//            bool drop = false;
//            if (!ParseInternalKey(key, &ikey)) {
//                // Do not hide error keys
//                current_user_key.clear();
//                has_current_user_key = false;
//                last_sequence_for_key = kMaxSequenceNumber;
//            } else {
//                if (!has_current_user_key ||
//                    internal_comparator_.user_comparator()->Compare(
//                            ikey.user_key,
//                            Slice(current_user_key)) !=
//                    0) {
//                    // First occurrence of this user key
//                    current_user_key.assign(ikey.user_key.data(),
//                                            ikey.user_key.size());
//                    has_current_user_key = true;
//                    last_sequence_for_key = kMaxSequenceNumber;
//                }
//
//                if (last_sequence_for_key <= smallest_snapshot_) {
//                    // Hidden by an newer entry for same user key
//                    drop = true;  // (A)
//                } else if (ikey.type == kTypeDeletion &&
//                           ikey.sequence <= smallest_snapshot_ &&
//                           compaction_->IsBaseLevelForKey(
//                                   ikey.user_key)) {
//                    // For this user key:
//                    // (1) there is no data in higher levels
//                    // (2) data in lower levels will have larger sequence numbers
//                    // (3) data in layers that are being compacted here and have
//                    //     smaller sequence numbers will be dropped in the next
//                    //     few iterations of this loop (by rule (A) above).
//                    // Therefore this deletion marker is obsolete and can be dropped.
//                    drop = true;
//                }
//
//                last_sequence_for_key = ikey.sequence;
//            }
//#if 0
//            Log(options_.info_log,
//                "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
//                "%d smallest_snapshot_: %d",
//                ikey.user_key.ToString().c_str(),
//                (int)ikey.sequence, ikey.type, kTypeValue, drop,
//                compaction_->IsBaseLevelForKey(ikey.user_key),
//                (int)last_sequence_for_key, (int)smallest_snapshot_);
//#endif
//
//            if (!drop) {
//                // Open output file if necessary
//                if (builder_ == nullptr) {
//                    status = OpenCompactionOutputFile();
//                    if (!status.ok()) {
//                        break;
//                    }
//                }
//                if (builder_->NumEntries() == 0) {
//                    current_output()->smallest.DecodeFrom(key);
//                }
//                current_output()->largest.DecodeFrom(key);
//                builder_->Add(key, input->value());
//
//                // Close output file if it is big enough
//                if (builder_->FileSize() >=
//                    compaction_->MaxOutputFileSize()) {
//                    status = FinishCompactionOutputFile(input);
//                    if (!status.ok()) {
//                        break;
//                    }
//                }
//            }
//
//            input->Next();
//        }
//
//        if (status.ok() && builder_ != nullptr) {
//            status = FinishCompactionOutputFile(input);
//        }
//        if (status.ok()) {
//            status = input->status();
//        }
//        delete input;
//
//        CompactionStats stats;
//        stats.micros = env_->NowMicros() - start_micros - imm_micros;
//        for (int which = 0; which < 2; which++) {
//            for (int i = 0;
//                 i < compaction_->num_input_files(which); i++) {
//                stats.bytes_read += compaction_->input(which,
//                                                       i)->file_size;
//            }
//        }
//        for (size_t i = 0; i < outputs_.size(); i++) {
//            stats.bytes_written += outputs_[i].file_size;
//        }
//
//        stats_[compaction_->level() + 1].Add(stats);
//
//        if (db_profiler_) {
//            CompactionProfiler compaction;
//            compaction.level = compaction_->level();
//            compaction.output_level = compaction_->level() + 1;
//            compaction.level_stats.num_files = compaction_->num_input_files(
//                    0);
//            for (int i = 0;
//                 i < compaction_->num_input_files(0); i++) {
//                compaction.level_stats.num_bytes_read += compaction_->input(
//                        0, i)->file_size;
//            }
//
//            compaction.next_level_stats.num_files = compaction_->num_input_files(
//                    1);
//            for (int i = 0;
//                 i < compaction_->num_input_files(1); i++) {
//                compaction.next_level_stats.num_bytes_read += compaction_->input(
//                        1, i)->file_size;
//            }
//
//            compaction.next_level_output_stats.num_files = outputs_.size();
//            compaction.next_level_output_stats.num_bytes_written = stats.bytes_written;
//            db_profiler_->Trace(compaction);
//        }
//
//        if (status.ok()) {
//            status = InstallCompactionResults();
//        }
//        VersionSet::LevelSummaryStorage tmp;
//        Log(options_.info_log, "compacted to: %s",
//            versions_->LevelSummary(&tmp));
//        return status;
    }
}