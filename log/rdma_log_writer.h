
//
// Created by Haoyu Huang on 12/23/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_RDMA_LOG_WRITER_H
#define LEVELDB_RDMA_LOG_WRITER_H

#include <mc/nova_mem_manager.h>
#include "leveldb/status.h"
#include "leveldb/slice.h"
#include "leveldb/log_writer.h"
#include "nova/nova_rdma_store.h"

namespace leveldb {

    class WritableFile;

    namespace log {

        class RDMALogWriter : public Writer {
        public:
            // Create a writer that will append data to "*dest".
            // "*dest" must be initially empty.
            // "*dest" must remain live while this Writer is in use.
            RDMALogWriter(nova::NovaRDMAStore *store,
                          nova::NovaMemManager *mem_manager);

            // Create a writer that will append data to "*dest".
            // "*dest" must have initial length "dest_length".
            // "*dest" must remain live while this Writer is in use.
            RDMALogWriter(WritableFile *dest, uint64_t dest_length);

            Status
            AddRecord(const std::string &log_file_name,
                      const Slice &slice) override;

            Status CloseLogFile(const std::string &log_file_name) override;

            char *AllocateLogBuf(const std::string &log_file);

            void DeleteLogBuf(const std::string &log_file);

            void AckAllocLogBuf(int remote_sid, uint64_t offset, uint64_t size);

            void AckWriteSuccess(int remote_sid);

        private:
            char *AddLocalRecord(const std::string &log_file_name,
                                 const Slice &slice);

            nova::NovaRDMAStore *store_;
            nova::NovaMemManager *mem_manager_;
            std::map<std::string, std::vector<Slice>> logfile_bufs;

            enum WriteResult {
                NONE,
                WAIT_FOR_ALLOC,
                ALLOC_SUCCESS,
                WAIT_FOR_WRITE,
                WRITE_SUCESS,
            };

            WriteResult *write_result_;
            uint64_t *server_remote_cur_offset_;
            uint64_t *server_remote_base_offset_;
            uint64_t *server_remote_buf_size_;
        };

    }  // namespace log
}  // namespace leveldb

#endif //LEVELDB_RDMA_LOG_WRITER_H
