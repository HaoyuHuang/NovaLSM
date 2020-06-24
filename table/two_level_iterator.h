// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
#define STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_

#include <leveldb/db_profiler.h>
#include "leveldb/iterator.h"
#include "block.h"

namespace leveldb {

    struct ReadOptions;

// Return a new two level iterator.  A two-level iterator contains an
// index iterator whose values point to a sequence of blocks where
// each block is itself a sequence of key,value pairs.  The returned
// two-level iterator yields the concatenation of all key/value pairs
// in the sequence of blocks.  Takes ownership of "index_iter" and
// will delete it when no longer needed.
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.
    Iterator *NewTwoLevelIterator(
            Iterator *index_iter,
            BlockReadContext context,
            Iterator *(*block_function)(void *arg,
                                        void *arg2,
                                        BlockReadContext context,
                                        const ReadOptions &options,
                                        const Slice &index_value,
                                        std::string* next_key),
            void *arg, void *arg2, const ReadOptions &options,
            const Comparator *comparator = nullptr, bool merging = false);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
