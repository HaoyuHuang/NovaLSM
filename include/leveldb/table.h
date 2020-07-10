// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <stdint.h>
#include "table/format.h"

#include "leveldb/export.h"
#include "leveldb/iterator.h"
#include "db_profiler.h"

namespace leveldb {

    class Block;

    class BlockHandle;

    class Footer;

    struct Options;

    class RandomAccessFile;

    struct ReadOptions;

    class TableCache;

    class BlockContents;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
// TODO: Create a new Table reader for CC to fetch the Table from DC.
//
    class LEVELDB_EXPORT Table {
    public:
        struct Rep;

        // Attempt to open the table that is stored in bytes [0..file_size)
        // of "file", and read the metadata entries necessary to allow
        // retrieving data from the table.
        //
        // If successful, returns ok and sets "*table" to the newly opened
        // table.  The client should delete "*table" when no longer needed.
        // If there was an error while initializing the table, sets "*table"
        // to nullptr and returns a non-ok status.  Does not take ownership of
        // "*source", but the client must ensure that "source" remains live
        // for the duration of the returned table's lifetime.
        //
        // *file must remain live while this Table is in use.
        static Status
        Open(const Options &options,
             const ReadOptions &read_options,
             const FileMetaData *meta,
             RandomAccessFile *file,
             uint64_t file_size, int level,
             uint64_t file_number, uint32_t replica_id, Table **table,
             DBProfiler *db_profiler);

        Table(const Table &) = delete;

        Table &operator=(const Table &) = delete;

        ~Table();

        // Returns a new iterator over the table contents.
        // The result of NewIterator() is initially invalid (caller must
        // call one of the Seek methods on the iterator before using it).
        Iterator *NewIterator(AccessCaller caller, const ReadOptions &) const;

        // Given a key, return an approximate byte offset in the file where
        // the data for that key begins (or would begin if the key were
        // present in the file).  The returned value is in terms of file
        // bytes, and so includes effects like compression of the underlying data.
        // E.g., the approximate offset of the last key in the table will
        // be close to the file length.
        uint64_t ApproximateOffsetOf(const Slice &key) const;

        static Status
        ReadBlock(RandomAccessFile *file, const ReadOptions &options,
                  const StoCBlockHandle &stoc_block_handle,
                  BlockContents *result);


        static Status
        ReadBlock(const char *buf, const Slice &content,
                  const ReadOptions &options,
                  const StoCBlockHandle &handle, BlockContents *result);

    private:

        friend class TableCache;

        static Iterator *
        DataBlockReader(void *arg, void *arg2, BlockReadContext context,
                        const ReadOptions &options,
                        const Slice &index_value, std::string *next_key);

        explicit Table() {}

        // Calls (*handle_result)(arg, ...) with the entry found after a call
        // to Seek(key).  May not make such a call if filter policy says
        // that key is not present.
        Status InternalGet(const ReadOptions &, const Slice &key, void *arg,
                           void (*handle_result)(void *arg, const Slice &k,
                                                 const Slice &v));

        uint64_t TranslateToDataBlockOffset(const StoCBlockHandle &handle);

        void ReadMeta(const Footer &footer);

        void ReadFilter(const Slice &filter_handle_value);

        Rep *rep_;
        DBProfiler *db_profiler_ = nullptr;
    };

    class StoCRandomAccessFileClient : public RandomAccessFile {
    public:
        virtual Status
        Read(const ReadOptions &read_options,
             const StoCBlockHandle &stoc_block_handle,
             uint64_t offset, size_t n, Slice *result, char *scratch) = 0;
    };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_