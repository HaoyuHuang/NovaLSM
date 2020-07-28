// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"
#include "util/coding.h"

namespace leveldb {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
    enum Tag : char {
        kComparator = 1,
        kLogNumber = 2,
        kNextFileNumber = 3,
        kLastSequence = 4,
        kCompactPointer = 5,
        kDeletedFile = 6,
        kNewFile = 7,
        kUpdateSubRange = 8,
        kEndEdit = 10,
        // 8 was used for large value refs
                kPrevLogNumber = 9
    };

    void VersionEdit::Clear() {
        comparator_.clear();
        last_sequence_ = 0;
        next_file_number_ = 0;
        has_comparator_ = false;
        has_prev_log_number_ = false;
        has_next_file_number_ = false;
        has_last_sequence_ = false;
        compact_pointers_.clear();
        deleted_files_.clear();
        new_files_.clear();
        new_subranges_.clear();
    }

    uint32_t VersionEdit::EncodeTo(char *dst) const {
        uint32_t msg_size = 0;
        if (has_comparator_) {
            dst[msg_size] = kComparator;
            msg_size += 1;
            msg_size += EncodeStr(dst + msg_size, comparator_);
        }
        if (has_next_file_number_) {
            dst[msg_size] = kNextFileNumber;
            msg_size += 1;
            msg_size += EncodeFixed64(dst + msg_size, next_file_number_);
        }
        if (has_last_sequence_) {
            dst[msg_size] = kLastSequence;
            msg_size += 1;
            msg_size += EncodeFixed64(dst + msg_size, last_sequence_);
        }

        for (const auto &deleted_file_kvp : deleted_files_) {
            dst[msg_size] = kDeletedFile;
            msg_size += 1;
            msg_size += EncodeFixed32(dst + msg_size,
                                      deleted_file_kvp.first);// level
            msg_size += EncodeFixed64(dst + msg_size,
                                      deleted_file_kvp.second.fnumber); // file number
        }

        for (size_t i = 0; i < new_files_.size(); i++) {
            const auto &f = new_files_[i].second;
            dst[msg_size] = kNewFile;
            msg_size += 1;
            msg_size += EncodeFixed32(dst + msg_size,
                                      new_files_[i].first); // level
            msg_size += f.Encode(dst + msg_size);
        }

        for (size_t i = 0; i < new_subranges_.size(); i++) {
            const auto &subrange = new_subranges_[i];
            dst[msg_size] = kUpdateSubRange;
            msg_size += 1;
            msg_size += subrange.Encode(dst + msg_size, i);
        }
        dst[msg_size] = kEndEdit;
        msg_size += 1;
        return msg_size;
    }

    static bool GetLevel(Slice *input, int *level) {
        uint32_t v;
        if (DecodeFixed32(input, &v) && v < nova::NovaConfig::config->level) {
            *level = v;
            return true;
        } else {
            return false;
        }
    }

    Status VersionEdit::DecodeFrom(const Slice &src, Slice *result) {
        Clear();
        Slice input = src;
        std::string msg;
        uint32_t tag;

        // Temporary storage for parsing
        int level;
        uint64_t number;
        uint32_t number_2;
        FileMetaData f;
        Slice str;
        InternalKey key;
        SubRange sr;

        while (msg.empty() && input.size() > 0) {
            tag = input[0];
            input.remove_prefix(1);

            if (tag == kEndEdit) {
                if (result != nullptr) {
                    *result = input;
                }
                return Status::OK();
            }

            switch (tag) {
                case kComparator:
                    break;
                case kNextFileNumber:
                    if (DecodeFixed64(&input, &next_file_number_)) {
                        has_next_file_number_ = true;
                    } else {
                        msg = "next file number";
                    }
                    break;

                case kLastSequence:
                    if (DecodeFixed64(&input, &last_sequence_)) {
                        has_last_sequence_ = true;
                    } else {
                        msg = "last sequence number";
                    }
                    break;
                case kDeletedFile:
                    if (GetLevel(&input, &level) &&
                        DecodeFixed64(&input, &number)) {
                        DeletedFileIdentifier df = {};
                        df.fnumber = number;
                        deleted_files_.emplace_back(std::make_pair(level, df));
                    } else {
                        msg = "deleted file";
                    }
                    break;

                case kNewFile:
                    if (GetLevel(&input, &level) && f.Decode(&input, false)) {
                        new_files_.emplace_back(std::make_pair(level, f));
                        f.block_replica_handles.clear();
                    } else {
                        msg = "new-file entry";
                    }
                    break;
                case kUpdateSubRange:
                    sr = {};
                    if (sr.Decode(&input)) {
                        new_subranges_.push_back(std::move(sr));
                    } else {
                        msg = "update-subrange entry";
                    }
                    break;

                default:
                    msg = "unknown tag " + std::to_string(tag);
                    break;
            }
        }

        if (msg.empty() && !input.empty()) {
            msg = "invalid tag";
        }

        Status s;
        if (!msg.empty()) {
            s = Status::Corruption("VersionEdit", msg);
        }
        if (result != nullptr) {
            *result = input;
        }
        return s;
    }

    std::string VersionEdit::DebugString() const {
        std::string r;
        r.append("VersionEdit {");
        if (has_comparator_) {
            r.append("\n  Comparator: ");
            r.append(comparator_);
        }
        if (has_next_file_number_) {
            r.append("\n  NextFile: ");
            AppendNumberTo(&r, next_file_number_);
        }
        if (has_last_sequence_) {
            r.append("\n  LastSeq: ");
            AppendNumberTo(&r, last_sequence_);
        }
        for (size_t i = 0; i < compact_pointers_.size(); i++) {
            r.append("\n  CompactPointer: ");
            AppendNumberTo(&r, compact_pointers_[i].first);
            r.append(" ");
            r.append(compact_pointers_[i].second.DebugString());
        }
        for (const auto &deleted_files_kvp : deleted_files_) {
            r.append("\n  DeleteFile: ");
            AppendNumberTo(&r, deleted_files_kvp.first);
            r.append(" ");
            AppendNumberTo(&r, deleted_files_kvp.second.fnumber);
        }
        for (size_t i = 0; i < new_files_.size(); i++) {
            const FileMetaData &f = new_files_[i].second;
            r.append("\n  AddFile: ");
            AppendNumberTo(&r, new_files_[i].first);
            r.append(" ");
            r.append(f.DebugString());
        }
        for (size_t i = 0; i < new_subranges_.size(); i++) {
            const SubRange &sr = new_subranges_[i];
            r.append("\n  UpdateSubrange: ");
            r.append(sr.DebugString());
        }
        r.append("\n}\n");
        return r;
    }

}  // namespace leveldb
