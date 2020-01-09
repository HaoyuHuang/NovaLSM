
//
// Created by Haoyu Huang on 12/24/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NIC_LOG_WRITER_H
#define LEVELDB_NIC_LOG_WRITER_H

#include <vector>
#include "mc/nova_mem_manager.h"
#include "nova/nova_client_sock.h"
#include "nova_log.h"

#include <mc/nova_mem_manager.h>
#include "leveldb/status.h"
#include "leveldb/slice.h"
#include "leveldb/log_writer.h"
#include "nova/nova_rdma_store.h"
#include "log/nova_log.h"

namespace leveldb {

    class WritableFile;

    namespace log {

        class NICLogWriter : public Writer {
        public:
            // Create a writer that will append data to "*dest".
            // "*dest" must be initially empty.
            // "*dest" must remain live while this Writer is in use.
            NICLogWriter(std::vector<nova::NovaClientSock *> *sockets,
                         nova::NovaMemManager *mem_manager,
                         nova::LogFileManager *log_manager
            );

            // Create a writer that will append data to "*dest".
            // "*dest" must have initial length "dest_length".
            // "*dest" must remain live while this Writer is in use.
            NICLogWriter(WritableFile *dest, uint64_t dest_length);

            Status
            AddRecord(const std::string &log_file_name,
                      const Slice &slice) override;

            char *AddLocalRecord(const std::string &log_file_name,
                                 const Slice &slice) override;

            Status CloseLogFile(const std::string &log_file_name) override;

            char *AllocateLogBuf(const std::string &log_file);

        private:

            struct LogFileBuf {
                char *base;
                uint32_t size;
                uint32_t offset;
            };

            std::vector<nova::NovaClientSock *> *sockets_;
            nova::NovaMemManager *mem_manager_;
            nova::LogFileManager *log_manager_;
            std::map<std::string, LogFileBuf> logfile_last_buf_;
            std::mutex mutex_;
        };

    }  // namespace log
}  // namespace leveldb

#endif //LEVELDB_NIC_LOG_WRITER_H
