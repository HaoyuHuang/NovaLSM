
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_LOG_H
#define LEVELDB_NOVA_LOG_H

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "nova/linked_list.h"

namespace nova {

    class LogRecord {
    public:
        leveldb::Slice backing_mem;
        GlobalSSTableHandle table_handle;
        leveldb::ValueType type;
        leveldb::Slice key;
        leveldb::Slice value;

        leveldb::Status Encode(char *data);

        leveldb::Status Decode(char *data);
    };

    class LogFile {
    public:
        struct TableIndex {
            GlobalSSTableHandle handle;
            uint32_t file_offset;
            uint32_t nrecords;
        };

        LogFile(GlobalLogFileHandle handle, char *backing_mem,
                uint64_t file_size);

        void AddIndex(const TableIndex &index) {
            table_index_.push_back(index);
        }

        const std::vector<TableIndex> &table_index() {
            return table_index_;
        }

    private:
        GlobalLogFileHandle handle_;
        char *backing_mem_;
        uint64_t file_size_;
        std::vector<TableIndex> table_index_;
    };
}

#endif //LEVELDB_NOVA_LOG_H
