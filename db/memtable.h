// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>
#include <set>
#include <queue>

#include "leveldb/db_profiler.h"
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "leveldb/db.h"
#include "util/arena.h"

namespace leveldb {

    class InternalKeyComparator;

    class MemTableIterator;

    class MemTable {
    public:
        // MemTables are reference counted.  The initial reference count
        // is zero and the caller must call Ref() at least once.
        explicit MemTable(const InternalKeyComparator &comparator,
                          uint32_t memtable_id,
                          DBProfiler *db_profiler);

        MemTable(const MemTable &) = delete;

        MemTable &operator=(const MemTable &) = delete;

        // Increase reference count.
        void Ref() {
            ++refs_;
        }

        uint32_t memtableid() {
            return memtable_id_;
        }

        // Drop reference count.  Delete if no more references exist.
        uint32_t Unref() {
            --refs_;
            uint32_t refs = refs_;
            assert(refs_ >= 0);
            return refs;
        }

        // Returns an estimate of the number of bytes of data in use by this
        // data structure. It is safe to call when MemTable is being modified.
        size_t ApproximateMemoryUsage();

        // Return an iterator that yields the contents of the memtable.
        //
        // The caller must ensure that the underlying MemTable remains live
        // while the returned iterator is live.  The keys returned by this
        // iterator are internal keys encoded by AppendInternalKey in the
        // db/format.{h,cc} module.
        Iterator *NewIterator(TraceType trace_type, AccessCaller caller,
                              uint32_t sample_size = 0);

        // Add an entry into memtable that maps key to value at the
        // specified sequence number and with the specified type.
        // Typically value will be empty if type==kTypeDeletion.
        void Add(SequenceNumber seq, ValueType type, const Slice &key,
                 const Slice &value);

        // If memtable contains a value for key, store it in *value and return true.
        // If memtable contains a deletion for key, store a NotFound() error
        // in *status and return true.
        // Else, return false.
        bool Get(const LookupKey &key, std::string *value, Status *s);

        FileMetaData &meta() {
            return flushed_meta_;
        }

        ~MemTable();  // Private since only Unref() should be used to delete it

        bool is_pinned_ = false;
    private:
        friend class MemTableIterator;

        friend class MemTableBackwardIterator;

        struct KeyComparator {
            const InternalKeyComparator comparator;

            explicit KeyComparator(const InternalKeyComparator &c) : comparator(
                    c) {}

            int operator()(const char *a, const char *b) const;
        };

        typedef SkipList<const char *, KeyComparator> Table;

        DBProfiler *db_profiler_ = nullptr;
        KeyComparator comparator_;
        int refs_ = 0;
        uint32_t memtable_id_ = 0;
        Arena arena_;
        Table table_;
        FileMetaData flushed_meta_;
    };

    class AtomicMemTable {
    public:
        void SetMemTable(MemTable *mem);

        void SetFlushed(const std::string &dbname,
                        const std::vector<uint64_t> &l0_file_numbers);

        AtomicMemTable *Ref(std::vector<uint64_t>* l0fns = nullptr);

        void DeleteL0File(std::vector<uint64_t>& l0fns);

        void Unref(const std::string &dbname);

        bool locked = false;
        bool is_immutable_ = false;
        bool is_flushed_ = false;

        std::vector<uint64_t> l0_file_numbers_;

        std::mutex mutex_;
        MemTable *memtable_ = nullptr;
        std::atomic_int_fast32_t nentries_;
        uint32_t memtable_size_ = 0;
        uint32_t number_of_pending_writes_ = 0;
    };

    // static partition.
    struct MemTablePartition {
        MemTablePartition() : background_work_finished_signal_(&mutex) {
        };
        MemTable *memtable;
        port::Mutex mutex;
        uint32_t partition_id = 0;
        std::vector<uint32_t> imm_slots;
        std::queue<uint32_t> available_slots;
        std::vector<uint32_t> closed_log_files;
        port::CondVar background_work_finished_signal_ GUARDED_BY(mutex);
    };
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
