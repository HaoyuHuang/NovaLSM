
//
// Created by Haoyu Huang on 1/29/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_PERSISTENT_STOC_FILE_H
#define LEVELDB_PERSISTENT_STOC_FILE_H

#include <string>
#include <list>
#include <unordered_map>

#include "leveldb/env.h"
#include "table/format.h"

namespace leveldb {

    // Persistent StoC file.
    class StoCPersistentFile {
    public:
        StoCPersistentFile(uint32_t file_id, Env *env, std::string filename,
                           MemManager *mem_manager,
                           uint32_t thread_id, uint32_t file_size);

        Status
        Read(uint64_t offset, uint32_t size, char *scratch, Slice *result);

        Status
        ReadForReplication(uint64_t offset, uint32_t size, char *scratch, Slice *result);

        uint64_t Persist(uint32_t given_file_id_for_assertion);

        uint64_t AllocateBuf(const std::string &filename,
                             uint32_t size, FileInternalType internal_type);

        bool MarkOffsetAsWritten(uint32_t given_file_id_for_assertion,
                                 uint64_t offset);

        BlockHandle Handle(const std::string &filename, FileInternalType internal_type);

        bool DeleteSSTable(uint32_t given_fileid_for_assertion,
                           const std::string &filename);

        void Close();

        uint32_t file_id() {
            return file_id_;
        }

        void ForceSeal();

        bool sealed() const {
            return sealed_;
        }

        std::string stoc_file_name_;
    private:

        void Seal();

        struct AllocatedBuf {
            std::string filename;
            uint64_t offset;
            uint32_t size;
            bool written_to_mem;
            FileInternalType internal_type;
        };

        struct StoCPersistStatus {
            BlockHandle disk_handle = {};
            bool persisted = false;
        };

        struct BatchWrite {
            BlockHandle mem_handle = {};
            std::string sstable;
            FileInternalType internal_type;
        };

        Env *env_ = nullptr;
        ReadWriteFile *file_ = nullptr;

        std::unordered_map<std::string, StoCPersistStatus> file_block_offset_;
        std::unordered_map<std::string, StoCPersistStatus> file_meta_block_offset_;
        std::unordered_map<std::string, StoCPersistStatus> file_parity_block_offset_;

        std::list<AllocatedBuf> allocated_bufs_;
        bool is_full_ = false;
        bool sealed_ = false;

        MemManager *mem_manager_ = nullptr;
        char *backing_mem_ = nullptr;
        uint64_t current_disk_offset_ = 0;
        uint64_t current_mem_offset_ = 0;
        uint32_t file_size_ = 0;
        uint32_t allocated_mem_size_ = 0;
        uint32_t thread_id_ = 0;
        uint32_t file_id_ = 0;
        uint32_t persisting_cnt = 0;
        uint32_t reading_cnt = 0;

        bool waiting_to_be_deleted = false;
        bool deleted_ = false;
        std::mutex mutex_;

        std::vector<BatchWrite> written_mem_blocks_;
        std::mutex persist_mutex_;

    };

    class StocPersistentFileManager {
    public:
        StocPersistentFileManager(Env *env,
                                  MemManager *mem_manager,
                                  const std::string &stoc_file_path,
                                  uint32_t stoc_file_size);

        StoCPersistentFile *FindStoCFile(uint32_t stoc_file_id);

        void
        ReadDataBlock(const StoCBlockHandle &stoc_block_handle, uint64_t offset,
                      uint32_t size, char *scratch, Slice *result);

        bool
        ReadDataBlockForReplication(const StoCBlockHandle &stoc_block_handle,
                                    uint64_t offset,
                                    uint32_t size, char *scratch,
                                    Slice *result);

        StoCPersistentFile *
        OpenStoCFile(uint32_t thread_id, std::string &filename);

        void OpenStoCFiles(const std::unordered_map<std::string, uint32_t> &fn_files);

        void DeleteSSTable(const std::string &filename);

        std::unordered_map<std::string, leveldb::StoCPersistentFile *> fn_stoc_file_map_;
    private:
        Env *env_ = nullptr;
        MemManager *mem_manager_ = nullptr;
        uint32_t stoc_file_size_ = 0;
        std::string stoc_file_path_;
        // 0 is reserved so that read knows to fetch the block from a local file.
        // 1-1000 is reserved for manifest file.
        uint32_t current_manifest_file_stoc_file_id_ = 1;
        const uint32_t MAX_MANIFEST_FILE_ID = 10000;
        uint32_t current_stoc_file_id_ = MAX_MANIFEST_FILE_ID + 1;
        std::unordered_map<uint32_t, StoCPersistentFile *> stoc_files_;
        leveldb::Cache *block_cache_ = nullptr;
        std::mutex mutex_;
    };
}


#endif //LEVELDB_PERSISTENT_STOC_FILE_H
