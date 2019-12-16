// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <stdint.h>

#include "leveldb/export.h"
#include "leveldb/iterator.h"
#include "db_profiler.h"

namespace nova {

    struct GlobalSSTableHandle {
        uint32_t configuration_id;
        uint32_t partition_id;
        uint32_t cc_id;
        uint32_t table_id;

        uint32_t size() const {return 16;}

        uint32_t Encode(char *data) const {
            leveldb::EncodeFixed32(data, configuration_id);
            leveldb::EncodeFixed32(data + 4, partition_id);
            leveldb::EncodeFixed32(data + 8, cc_id);
            leveldb::EncodeFixed32(data + 12, table_id);
            return 16;
        }

        uint32_t Decode(char *data) {
            configuration_id = leveldb::DecodeFixed32(data);
            if (configuration_id == 0) {
                return 0;
            }
            partition_id = leveldb::DecodeFixed32(data + 4);
            cc_id = leveldb::DecodeFixed32(data + 8);
            table_id = leveldb::DecodeFixed32(data + 12);
            return 16;
        }

        bool operator<(const GlobalSSTableHandle &h2) const {
            if (configuration_id < h2.configuration_id) {
                return true;
            }
            if (partition_id < h2.partition_id) {
                return true;
            }
            if (cc_id < h2.cc_id) {
                return true;
            }
            if (table_id < h2.table_id) {
                return true;
            }
            return false;
        }
    };

    struct GlobalBlockHandle {
        struct GlobalSSTableHandle table_handle;
        leveldb::BlockHandle block_handle;

        static uint32_t CacheKeySize() {
            return 12;
        }

        void CacheKey(char *key) {
            leveldb::EncodeFixed32(key, table_handle.cc_id);
            leveldb::EncodeFixed32(key + 4, table_handle.table_id);
            leveldb::EncodeFixed32(key + 8, block_handle.offset());
        }

        bool operator<(const GlobalBlockHandle &h2) const {
            if (table_handle < h2.table_handle) {
                return true;
            }
            if (block_handle < h2.block_handle) {
                return true;
            }
            return false;
        }
    };

    struct GlobalLogFileHandle {
        uint32_t configuration_id;
        uint32_t mc_id;
        uint32_t log_id;

        bool operator<(const GlobalLogFileHandle &h2) const {
            if (configuration_id < h2.configuration_id) {
                return true;
            }
            if (mc_id < h2.mc_id) {
                return true;
            }
            if (log_id < h2.log_id) {
                return true;
            }
            return false;
        }
    };
}

namespace leveldb {

    class Block;

    class BlockHandle;

    class Footer;

    struct Options;

    class RandomAccessFile;

    struct ReadOptions;

    class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
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
        static Status Open(const Options &options, RandomAccessFile *file,
                           uint64_t file_size, int level,
                           uint64_t file_number, Table **table,
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

        RandomAccessFile *backing_file() { return backing_file_; }

    private:
        friend class TableCache;

        static Iterator *
        BlockReader(void *, BlockReadContext, const ReadOptions &,
                    const Slice &);

        explicit Table(Rep *rep) : rep_(rep) {}

        // Calls (*handle_result)(arg, ...) with the entry found after a call
        // to Seek(key).  May not make such a call if filter policy says
        // that key is not present.
        Status InternalGet(const ReadOptions &, const Slice &key, void *arg,
                           void (*handle_result)(void *arg, const Slice &k,
                                                 const Slice &v));

        void ReadMeta(const Footer &footer);

        void ReadFilter(const Slice &filter_handle_value);

        Rep *const rep_;
        DBProfiler *db_profiler_ = nullptr;
        RandomAccessFile *backing_file_ = nullptr;
    };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_
