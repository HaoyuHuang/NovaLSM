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
                          DBProfiler *db_profiler,
                          bool is_ready);

        MemTable(const MemTable &) = delete;

        MemTable &operator=(const MemTable &) = delete;

        // Increase reference count.
        void Ref() {
            ++refs_;
        }

        uint32_t memtableid() {
            return memtable_id_;
        }

        void SetReadyToProcessRequests();

        // Drop reference count.  Delete if no more references exist.
        uint32_t Unref(uint32_t unrefcount = 1) {
            refs_ -= unrefcount;
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
        // db/format.{h,ltc} module.
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
        void WaitUntilReady();

        friend class MemTableIterator;

        friend class MemTableBackwardIterator;

        struct KeyComparator {
            const InternalKeyComparator comparator;

            explicit KeyComparator(const InternalKeyComparator &c) : comparator(
                    c) {}

            int operator()(const char *a, const char *b) const;
        };

        std::atomic_bool is_ready_;
        port::Mutex is_ready_mutex_;
        port::CondVar is_ready_signal_;
        typedef SkipList<const char *, KeyComparator> Table;
        DBProfiler *db_profiler_ = nullptr;
        KeyComparator comparator_;
        int refs_ = 0;
        uint32_t memtable_id_ = 0;
        Arena arena_;
        Table table_;
        FileMetaData flushed_meta_;
    };

    struct MemTableL0FilesEdit {
        uint32_t dbid_ = 0;
        std::set<uint64_t> add_fns;
        std::set<uint64_t> remove_fns;

        std::string DebugString() const;
    };

    class AtomicMemTable {
    public:
        void SetMemTable(uint64_t generation_id, MemTable *mem);

        void SetFlushed(const std::string &dbname,
                        const std::vector<uint64_t> &l0_file_numbers,
                        uint32_t version_id);

        AtomicMemTable *RefMemTable();

        void UpdateL0Files(uint32_t version_id, const MemTableL0FilesEdit &edit);

        void Unref(const std::string &dbname, uint32_t unrefcnt = 1);

        uint32_t Encode(char *buf);

        bool Decode(Slice *buf, const InternalKeyComparator& cmp);

        bool is_immutable_ = false;
        bool is_flushed_ = false;
        std::atomic_uint_fast64_t generation_id_;
        std::atomic_bool is_scheduled_for_flushing;
        uint32_t last_version_id_ = 0;
        uint32_t memtable_id_ = 0;

        std::set<uint64_t> l0_file_numbers_;

        std::mutex mutex_;
        MemTable *memtable_ = nullptr;
        std::atomic_int_fast32_t nentries_;
        uint32_t memtable_size_ = 0;
        uint32_t number_of_pending_writes_ = 0;
    };

    struct MemTableLogFilePair {
        MemTable *memtable = nullptr;
        SubRange *subrange = nullptr;
        bool is_immutable = false;
        uint32_t partition_id = 0;
        uint32_t imm_slot = 0;
        std::string logfile;
        std::unordered_map<uint32_t, uint64_t> server_logbuf;
    };

    // static partition.
    struct MemTablePartition {
        MemTablePartition() : background_work_finished_signal_(&mutex) {
        };

        std::string DebugString() const;

        MemTable *active_memtable = nullptr;
        port::Mutex mutex;
        std::map<uint64_t, std::set<uint32_t>> generation_num_memtables_;

        void AddMemTable(uint64_t generation_id, uint32_t memtableid);

        void RemoveMemTable(uint64_t generation_id, uint32_t memtableid);

        uint32_t partition_id = 0;
        std::vector<uint32_t> imm_slots;
        std::queue<uint32_t> available_slots;
        std::vector<uint32_t> immutable_memtable_ids;
        std::map<uint32_t, uint32_t> slot_imm_id;
        port::CondVar background_work_finished_signal_ GUARDED_BY(mutex);
    };
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
