
//
// Created by Haoyu Huang on 1/11/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_DB_TYPES_H
#define LEVELDB_DB_TYPES_H

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <set>
#include <fmt/core.h>
#include <atomic>

#include "port/port.h"

#include "slice.h"

namespace leveldb {
    class StoCBlockHandle {
    public:
        uint32_t server_id = 0;
        uint32_t stoc_file_id = 0;
        uint64_t offset = 0;
        uint32_t size = 0;

        static int HandleSize() {
            return 4 + 4 + 8 + 4;
        }

        std::string DebugString() const;

        void EncodeHandle(char *buf) const;

        void DecodeHandle(const char *buf);

        static bool DecodeHandle(Slice *data, StoCBlockHandle *handle);

        static bool
        DecodeHandles(Slice *data, std::vector<StoCBlockHandle> *handles);
    };

    struct SSTableStoCFilePair {
        std::string sstable_name;
        uint32_t stoc_file_id;
    };

    typedef uint64_t SequenceNumber;

    // Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
    enum ValueType {
        kTypeDeletion = 0x0, kTypeValue = 0x1
    };

    struct ScanStats {
        uint64_t number_of_scans_ = 0;
        uint64_t number_of_scan_memtables_ = 0;
        uint64_t number_of_scan_l0_sstables_ = 0;
        uint64_t number_of_scan_sstables_ = 0;

        std::string DebugString() {
            return fmt::format("{}-{}-{}-{}", number_of_scans_,
                               number_of_scan_memtables_,
                               number_of_scan_l0_sstables_,
                               number_of_scan_sstables_);
        }
    };

    struct ParsedInternalKey {
        Slice user_key;
        SequenceNumber sequence;
        ValueType type;

        ParsedInternalKey() {}  // Intentionally left uninitialized (for speed)
        ParsedInternalKey(const Slice &u, const SequenceNumber &seq,
                          ValueType t)
                : user_key(u), sequence(seq), type(t) {}

        std::string DebugString() const;

        std::string FullDebugString() const;
    };

    // Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
    class InternalKey {
    private:
        std::string rep_;

    public:
        InternalKey() {}  // Leave rep_ as empty to indicate it is invalid
        InternalKey(const Slice &user_key, SequenceNumber s, ValueType t);

        bool DecodeFrom(const Slice &s, bool copy = false);

        Slice Encode() const {
            assert(!rep_.empty());
            return rep_;
        }

        Slice user_key() const;

        void SetFrom(const ParsedInternalKey &p);

        void Clear();

        std::string DebugString() const;
    };

    enum FileCompactionStatus {
        NONE = 0,
        COMPACTING = 1,
        COMPACTED = 2
    };

    struct DeletedFileIdentifier {
        uint64_t fnumber = 0;
    };

    enum FileInternalType : char {
        kFileMetadata = 'm',
        kFileData = 'd',
        kFileParity = 'p'
    };

    bool DecodeInternalFileType(Slice *ptr, FileInternalType *internal_type);

    struct ReplicationPair {
        uint32_t source_stoc_file_id = 0;
        uint32_t source_file_size = 0;
        uint32_t dest_stoc_id = 0;
        uint64_t sstable_file_number = 0;
        uint32_t replica_id = 0;
        FileInternalType internal_type;

        uint32_t dest_stoc_file_id = 0;

        uint32_t Encode(char *buf) const;

        std::string DebugString() const;

        bool Decode(Slice *ptr);


    };

    struct FileReplicaMetaData {
        StoCBlockHandle meta_block_handle;
        std::vector<StoCBlockHandle> data_block_group_handles;
    };



    struct FileMetaData {
        FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0),
                         converted_file_size(0),
                         compaction_status(FileCompactionStatus::NONE) {}

        uint32_t Encode(char *buf) const;

        bool Decode(Slice *ptr, bool copy);

        bool DecodeReplicas(Slice *ptr);

        bool DecodeMemTableIds(Slice *ptr);

        std::string DebugString() const;

        std::string ShortDebugString() const;

        int SelectReplica() const;

        int refs = 0;
        int allowed_seeks = 0;  // Seeks allowed until compaction
        //
        std::set<uint32_t> memtable_ids;
        uint64_t number = 0;
        uint64_t file_size = 0;    // File size in bytes in original SSTable format.
        uint64_t converted_file_size = 0; // File size in bytes after converted to StoC file.
        uint64_t flush_timestamp = 0;
        uint32_t level = 0;
        InternalKey smallest;  // Smallest internal key served by table
        InternalKey largest;   // Largest internal key served by table
        FileCompactionStatus compaction_status;
        std::vector<FileReplicaMetaData> block_replica_handles = {};
        StoCBlockHandle parity_block_handle;
    };

    class LEVELDB_EXPORT MemManager {
    public:
        virtual char *ItemAlloc(uint64_t key, uint32_t scid) = 0;

        virtual void FreeItem(uint64_t key, char *buf, uint32_t scid) = 0;

        virtual void
        FreeItems(uint64_t key, const std::vector<char *> &items,
                  uint32_t scid) = 0;

        virtual uint32_t slabclassid(uint64_t key, uint64_t size) = 0;
    };

    class RDMAServer {
    public:
        virtual int ProcessCompletionQueue() = 0;
    };

    class WBTable {
    public:
        uint64_t thread_id;
        char *backing_mem;
        uint64_t used_size;
        uint64_t allocated_size;

        void Ref();

        void Unref();

        bool is_deleted();

        void Delete();

    private:
        MemManager *mem_manager_;
        int refcount = 0;
        bool deleted = false;

        std::mutex mutex_;
    };

    class SSTableManager {
    public:
        virtual void AddSSTable(const std::string &dbname, uint64_t file_number,
                                uint64_t thread_id,
                                char *backing_mem, uint64_t used_size,
                                uint64_t allocated_size, bool async_flush) = 0;

        virtual void GetSSTable(const std::string &dbname, uint64_t file_number,
                                WBTable **table) = 0;

        virtual void
        RemoveSSTable(const std::string &dbname, uint64_t file_number) = 0;

        virtual void
        RemoveSSTables(const std::string &dbname,
                       const std::vector<uint64_t> &file_number) = 0;
    };

    class MemTablePool {
    public:
        port::CondVar **range_cond_vars_;
        uint32_t num_available_memtables_ = 0;
        std::mutex mutex_;
    };
}

#endif //LEVELDB_DB_TYPES_H
