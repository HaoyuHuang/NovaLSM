
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

        uint64_t AllocateBuf(const std::string &sstable, uint32_t size);

        void MarkOffsetAsWritten(uint64_t offset);

        BlockHandle &Handle(const std::string &sstable_id);

        void DeleteSSTable(const std::string &dbname,
                           uint64_t file_number);

        uint32_t rtable_id() {
            return rtable_id_;
        }

    private:

        void Seal();

        struct AllocatedBuf {
            std::string sstable_id;
            uint64_t offset;
            uint32_t size;
            bool persisted;
        };

        Env *env_;
        WritableFile *file_;
        RandomAccessFile *readable_file_;

        std::map<std::string, BlockHandle> sstable_offset_;
        std::list<AllocatedBuf> allocated_bufs_;
        bool sealed_ = false;

        MemManager *mem_manager_;
        std::string rtable_name_;
        char *backing_mem_;
        uint64_t current_disk_offset_;
        uint64_t current_mem_offset_;
        uint32_t file_size_;
        uint32_t allocated_mem_size_;
        uint32_t thread_id_;
        uint32_t rtable_id_;
        bool deleted_;
        std::mutex mutex_;
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

        // db -> sstable file number -> rtables.
        void
        ReadDataBlocksOfSSTable(const std::string &dbname,
                                uint64_t file_number, char *scratch);

        void ReadDataBlock(const RTableHandle &rtable_handle, char *scratch);

        void DeleteSSTable(const std::string &dbname,
                           uint64_t file_number);

        void DeleteSSTable(const std::string &dbname,
                           const std::vector<uint64_t> &file_number);

    private:
        struct DBSSTableRTableMapping {
            std::map<uint64_t, NovaRTable *> fn_rtable;
            std::mutex mutex_;
        };

        DBSSTableRTableMapping ***server_db_sstable_rtable_mapping_;

        Env *env_;
        MemManager *mem_manager_;
        uint32_t rtable_size_;
        std::string rtable_path_;
        uint32_t current_rtable_id_ = 0;
        NovaRTable *active_rtables_[64];
        NovaRTable *rtables_[MAX_NUM_RTABLES];
        std::mutex mutex_;
    };

}


#endif //LEVELDB_NOVA_RTABLE_H
