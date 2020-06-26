
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
#include "nova/nova_rdma_broker.h"
#include "log/nova_log.h"

namespace leveldb {

    class WritableFile;

    namespace log {

        class RDMALogWriter : public Writer {
        public:
            // Create a writer that will append data to "*dest".
            // "*dest" must be initially empty.
            // "*dest" must remain live while this Writer is in use.
            RDMALogWriter(nova::NovaRDMABroker *store,
                          nova::NovaMemManager *mem_manager,
                          nova::LogFileManager *log_manager
            );

            // Create a writer that will append data to "*dest".
            // "*dest" must have initial length "dest_length".
            // "*dest" must remain live while this Writer is in use.
            RDMALogWriter(WritableFile *dest, uint64_t dest_length);

            Status
            AddRecord(const std::string &log_file_name,
                      const Slice &slice) override;

            char *AddLocalRecord(const std::string &log_file_name,
                                 const Slice &slice) override;

            Status CloseLogFile(const std::string &log_file_name) override;

            char *AllocateLogBuf(const std::string &log_file);

            void AckAllocLogBuf(int remote_sid, uint64_t offset, uint64_t size);

            void AckWriteSuccess(int remote_sid);

        private:
            struct LogFileBuf {
                uint64_t base;
                uint64_t offset;
                uint64_t size;
            };


            void Init(const std::string &log_file_name);

            nova::NovaRDMABroker *store_;
            nova::NovaMemManager *mem_manager_;
            nova::LogFileManager *log_manager_;
            std::map<std::string, LogFileBuf *> logfile_last_buf_;

            enum WriteResult {
                NONE = 0,
                WAIT_FOR_ALLOC = 1,
                ALLOC_SUCCESS = 2,
                WAIT_FOR_WRITE = 3,
                WRITE_SUCESS =4,
            };

            std::string write_result_str(WriteResult wr);

            std::string current_log_file_;
            WriteResult *write_result_;
        };

    }  // namespace log
}  // namespace leveldb

#endif //LEVELDB_RDMA_LOG_WRITER_H
