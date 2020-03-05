// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

    class VersionSet;

    class VersionEdit {
    public:
        VersionEdit() { Clear(); }

        ~VersionEdit() = default;

        void Clear();

        void SetComparatorName(const Slice &name) {
            has_comparator_ = true;
            comparator_ = name.ToString();
        }

        void SetLogNumber(uint64_t num) {
            has_log_number_ = true;
            log_number_ = num;
        }

        void SetPrevLogNumber(uint64_t num) {
            has_prev_log_number_ = true;
            prev_log_number_ = num;
        }

        void SetNextFile(uint64_t num) {
            has_next_file_number_ = true;
            next_file_number_ = num;
        }

        void SetLastSequence(SequenceNumber seq) {
            has_last_sequence_ = true;
            last_sequence_ = seq;
        }

        void SetCompactPointer(int level, const InternalKey &key) {
            compact_pointers_.emplace_back(std::make_pair(level, key));
        }

        // Add the specified file at the specified number.
        // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
        // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
        void AddFile(int level, uint64_t file, uint64_t file_size,
                     uint64_t converted_file_size,
                     const InternalKey &smallest, const InternalKey &largest,
                     FileCompactionStatus status,
                     const std::vector<RTableHandle> &data_block_group_handles) {
            FileMetaData f;
            f.number = file;
            f.file_size = file_size;
            f.converted_file_size = converted_file_size;
            f.smallest = smallest;
            f.largest = largest;
            f.compaction_status = status;
            f.data_block_group_handles = data_block_group_handles;
            new_files_.emplace_back(std::make_pair(level, f));
        }

        // Delete the specified "file" from the specified "level".
        void DeleteFile(int level, uint64_t file) {
            deleted_files_.insert(std::make_pair(level, file));
        }

        void EncodeTo(std::string *dst) const;

        Status DecodeFrom(const Slice &src);

        std::string DebugString() const;

    private:
        friend class VersionSet;

        typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

        std::string comparator_;
        uint64_t log_number_;
        uint64_t prev_log_number_;
        uint64_t next_file_number_;
        SequenceNumber last_sequence_;
        bool has_comparator_;
        bool has_log_number_;
        bool has_prev_log_number_;
        bool has_next_file_number_;
        bool has_last_sequence_;

        std::vector<std::pair<int, InternalKey>> compact_pointers_;
        DeletedFileSet deleted_files_;
        std::vector<std::pair<int, FileMetaData>> new_files_;
    };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
