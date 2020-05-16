// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <stddef.h>
#include <stdint.h>

#include "leveldb/iterator.h"

namespace leveldb {

    struct BlockContents;

    class Comparator;

    class Block {
    public:
        // Initialize the block with the specified contents.
        explicit Block(const BlockContents &contents, uint64_t file_number,
                       uint64_t block_id, bool adhoc = false);

        Block(const Block &) = delete;

        Block &operator=(const Block &) = delete;

        ~Block();

        size_t size() const { return size_; }

        uint64_t file_number() const { return file_number_; }

        uint64_t block_id() const { return block_id_; }

        Iterator *NewIterator(const Comparator *comparator);

    private:
        class Iter;

        uint32_t NumRestarts() const;

        const uint64_t file_number_;
        const uint64_t block_id_;

        const char *data_;
        size_t size_;
        uint32_t restart_offset_;  // Offset in data_ of restart array
        bool owned_;               // Block owns data_[]
        bool adhoc_;
    };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
