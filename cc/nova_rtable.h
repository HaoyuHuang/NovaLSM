
//
// Created by Haoyu Huang on 1/29/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_RTABLE_H
#define LEVELDB_NOVA_RTABLE_H

#include <string>
#include <list>
#include "leveldb/env.h"
#include "table/format.h"

#define MAX_NUM_RTABLES 100000

namespace leveldb {


    class NovaRTable {
    public:
        NovaRTable(uint32_t rtable_id, Env *env, std::string rtable_name,
                   MemManager *mem_manager,
                   uint32_t thread_id, uint32_t rtable_size);

        Status Read(uint64_t offset, uint32_t size, char *scratch);

        void Persist();

        uint64_t AllocateBuf(const std::string &sstable_id,
                             uint32_t size);

        void MarkOffsetAsWritten(uint64_t offset);

        BlockHandle Handle(const std::string &sstable_id);

        void DeleteSSTable(const std::string &sstable_id);

        uint32_t rtable_id() {
            return rtable_id_;
        }

    private:

        void Seal();

        struct AllocatedBuf {
            std::string sstable_id;
            uint64_t offset;
            uint32_t size;
            bool written_to_mem;
        };

        struct SSTablePersistStatus {
            BlockHandle disk_handle = {};
            bool persisted = false;
        };

        struct BatchWrite {
            BlockHandle mem_handle = {};
            std::string sstable;
        };

        Env *env_ = nullptr;
        ReadWriteFile *file_ = nullptr;

        std::map<std::string, SSTablePersistStatus> sstable_offset_;
        std::list<AllocatedBuf> allocated_bufs_;
        bool is_full_ = false;
        bool sealed_ = false;

        MemManager *mem_manager_ = nullptr;
        std::string rtable_name_;
        char *backing_mem_ = nullptr;
        uint64_t current_disk_offset_ = 0;
        uint64_t current_mem_offset_ = 0;
        uint32_t file_size_ = 0;
        uint32_t allocated_mem_size_ = 0;
        uint32_t thread_id_ = 0;
        uint32_t rtable_id_ = 0;
        uint32_t persisting_cnt = 0;
        bool deleted_ = false;
        std::mutex mutex_;

        std::vector<BatchWrite> written_mem_blocks_;
        std::mutex persist_mutex_;

    };

    class NovaRTableManager {
    public:
        NovaRTableManager(Env *env,
                          MemManager *mem_manager,
                          const std::string &rtable_path, uint32_t rtable_size,
                          uint32_t nservers, uint32_t nranges);

        NovaRTable *rtable(int rtable_id);

        NovaRTable *active_rtable(uint32_t thread_id);

        NovaRTable *CreateNewRTable(uint32_t thread_id);

        void ReadDataBlock(const RTableHandle &rtable_handle, uint64_t offset,
                           uint32_t size, char *scratch);

    private:

        Env *env_ = nullptr;
        MemManager *mem_manager_ = nullptr;
        uint32_t rtable_size_ = 0;
        std::string rtable_path_;
        // 0 is reserved so that read knows to fetch the block from a local file.
        uint32_t current_rtable_id_ = 1;
        NovaRTable *active_rtables_[64];
        NovaRTable *rtables_[MAX_NUM_RTABLES];
        std::mutex mutex_;
    };

}


#endif //LEVELDB_NOVA_RTABLE_H
