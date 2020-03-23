
//
// Created by Haoyu Huang on 12/23/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_CC_LOG_WRITER_H
#define LEVELDB_NOVA_CC_LOG_WRITER_H

#include "nova/nova_common.h"
#include "mc/nova_mem_manager.h"
#include "leveldb/status.h"
#include "leveldb/slice.h"
#include "leveldb/log_writer.h"
#include "nova/nova_rdma_store.h"
#include "log/nova_in_memory_log_manager.h"

namespace leveldb {

    namespace log {

        class RDMALogWriter {
        public:
            RDMALogWriter(nova::NovaRDMAStore *store, char *rnic_buf,
                          MemManager *mem_manager,
                          nova::InMemoryLogFileManager *log_manager);

            Status
            AddRecord(MemTableIdentifier memtable_id,
                      uint64_t thread_id,
                      const std::vector<LevelDBLogRecord> &log_records);

            void AckAllocLogBuf(int remote_sid, uint64_t offset, uint64_t size);

            bool AckWriteSuccess(int remote_sid, uint64_t rdma_wr_id);

            Status CloseLogFile(MemTableIdentifier memtable_id);

        private:
            struct LogFileBuf {
                uint64_t base;
                uint64_t offset;
                uint64_t size;
            };

            char *Init(MemTableIdentifier memtable_id, uint64_t thread_id,
                       const std::vector<LevelDBLogRecord> &log_records, uint32_t size);

            nova::NovaRDMAStore *store_;
            std::map<MemTableIdentifier, LogFileBuf *> logfile_last_buf_;

            enum WriteResult {
                NONE = 0,
                WAIT_FOR_ALLOC = 1,
                ALLOC_SUCCESS = 2,
                WAIT_FOR_WRITE = 3,
                WRITE_SUCESS = 4,
            };

            struct WriteState {
                WriteResult result;
                uint64_t rdma_wr_id;
            };

            std::string write_result_str(WriteResult wr);

            MemManager *mem_manager_;
            nova::InMemoryLogFileManager *log_manager_;

            char *rnic_buf_;
            uint32_t rnic_buf_size_;
            MemTableIdentifier current_memtable_id_;
            WriteState *write_result_;
        };

    }  // namespace log
}  // namespace leveldb

#endif //LEVELDB_NOVA_CC_LOG_WRITER_H
