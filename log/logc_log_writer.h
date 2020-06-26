
//
// Created by Haoyu Huang on 12/23/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_LOGC_LOG_WRITER_H
#define LEVELDB_LOGC_LOG_WRITER_H

#include "common/nova_mem_manager.h"
#include "leveldb/status.h"
#include "leveldb/slice.h"
#include "leveldb/log_writer.h"
#include "rdma/nova_rdma_broker.h"
#include "log/stoc_log_manager.h"
#include "novalsm/rdma_admission_ctrl.h"

namespace leveldb {
    // Replicate log records across StoCs.
    class LogCLogWriter {
    public:
        LogCLogWriter(nova::NovaRDMABroker *rdma_broker,
                      MemManager *mem_manager,
                      nova::StoCInMemoryLogFileManager *log_manager);

        bool
        AddRecord(const std::string &log_file_name,
                  uint64_t thread_id,
                  uint32_t dbid,
                  uint32_t memtableid,
                  char *rdma_backing_buf,
                  const std::vector<LevelDBLogRecord> &log_records,
                  uint32_t client_req_id,
                  StoCReplicateLogRecordState *replicate_log_record_states);

        void AckAllocLogBuf(const std::string &log_file_name, int remote_sid,
                            uint64_t offset, uint64_t size,
                            char *backing_mem, uint32_t log_record_size,
                            uint32_t client_req_id,
                            StoCReplicateLogRecordState *replicate_log_record_states);

        bool AckWriteSuccess(const std::string &log_file_name, int remote_sid,
                             uint64_t rdma_wr_id,
                             StoCReplicateLogRecordState *replicate_log_record_states);

        Status
        CloseLogFiles(const std::vector<std::string> &log_file_name, uint32_t dbid,
                     uint32_t client_req_id);

        bool CheckCompletion(const std::string &log_file_name, uint32_t dbid,
                             StoCReplicateLogRecordState *replicate_log_record_states);

        nova::RDMAAdmissionCtrl *admission_control_ = nullptr;
    private:
        std::string write_result_str(StoCReplicateLogRecordResult wr) {
            switch (wr) {
                case REPLICATE_LOG_RECORD_NONE:
                    return "none";
                case WAIT_FOR_ALLOC:
                    return "wait_for_alloc";
                case ALLOC_SUCCESS:
                    return "alloc_success";
                case WAIT_FOR_WRITE:
                    return "wait_for_write";
                case WRITE_SUCCESS:
                    return "write_success";
            }
        }

        struct LogFileBuf {
            uint64_t base = 0;
            uint64_t offset = 0;
            uint64_t size = 0;
            bool is_initializing = false;
        };

        struct LogFileMetadata {
            LogFileBuf *stoc_bufs = nullptr;
        };

        void Init(const std::string &log_file_name,
                  uint64_t thread_id,
                  const std::vector<LevelDBLogRecord> &log_records,
                  char *backing_buf);

        nova::NovaRDMABroker *rdma_broker_ = nullptr;
        std::unordered_map<std::string, LogFileMetadata> logfile_last_buf_;
        MemManager *mem_manager_ = nullptr;
        nova::StoCInMemoryLogFileManager *log_manager_ = nullptr;
    };

}  // namespace leveldb

#endif //LEVELDB_LOGC_LOG_WRITER_H
