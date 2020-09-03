
//
// Created by Haoyu Huang on 1/11/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef NOVA_STOC_CLIENT_H
#define NOVA_STOC_CLIENT_H

#include <cstdint>
#include <vector>
#include <infiniband/verbs.h>
#include <semaphore.h>
#include <unordered_map>

#include "subrange.h"
#include "db_types.h"

namespace leveldb {
    enum StoCReplicateLogRecordResult {
        REPLICATE_LOG_RECORD_NONE = 0,
        WAIT_FOR_ALLOC = 1,
        ALLOC_SUCCESS = 2,
        WAIT_FOR_WRITE = 3,
        WRITE_SUCCESS = 4,
    };

    struct StoCReplicateLogRecordState {
        uint32_t cfgid = 0;
        StoCReplicateLogRecordResult result;
        int rdma_wr_id;
    };

    struct CompactionRequest {
        std::string dbname;
        uint64_t smallest_snapshot;
        std::vector<FileMetaData *> inputs[2];
        std::vector<FileMetaData *> guides;
        std::vector<SubRange> subranges;
        uint32_t source_level = 0;
        uint32_t target_level = 0;
        sem_t *completion_signal = nullptr;

        std::vector<FileMetaData *> outputs;

        uint32_t EncodeRequest(char *buf);

        void DecodeRequest(char *buf, uint32_t buf_size);

        void FreeMemoryLTC();

        void FreeMemoryStoC();
    };

    enum StoCRequestType : char {
        STOC_READ_BLOCKS = 'a',
        STOC_ALLOCATE_LOG_BUFFER = 'k',
        STOC_ALLOCATE_LOG_BUFFER_SUCC = 'l',
        STOC_DELETE_LOG_FILE = 'm',
        STOC_DELETE_TABLES = 'o',
        STOC_READ_IN_MEMORY_LOG_FILE = 'p',
        STOC_WRITE_SSTABLE = 'q',
        STOC_WRITE_SSTABLE_RESPONSE = 'r',
        STOC_PERSIST = 'T',
        STOC_PERSIST_RESPONSE = 't',
        STOC_READ_STATS = 'u',
        STOC_READ_STATS_RESPONSE = 's',
        STOC_REPLICATE_LOG_RECORDS = 'v',
        STOC_QUERY_LOG_FILES = 'w',
        STOC_QUERY_LOG_FILES_RESPONSE = 'x',
        STOC_FILENAME_STOCFILEID = 'y',
        STOC_FILENAME_STOCFILEID_RESPONSE = 'z',
        STOC_COMPACTION = 'C',
        STOC_COMPACTION_RESPONSE = 'R',
        STOC_IS_READY_FOR_REQUESTS = 'A',
        STOC_IS_READY_FOR_REQUESTS_RESPONSE = 'B',
        RDMA_WRITE_REQUEST = 'D',
        RDMA_WRITE_REMOTE_BUF_ALLOCATED = 'E',
        LTC_MIGRATION = 'F',
        STOC_REPLICATE_SSTABLES = 'G',
        STOC_REPLICATE_SSTABLES_RESPONSE = 'H',
    };

    struct StoCRequestContext {
        StoCRequestType req_type;
        uint32_t remote_server_id = 0;
        std::string dbname;
        uint64_t file_number = 0;
        char *backing_mem = nullptr;
        uint32_t size = 0;
        bool done = false;

        uint64_t wr_id = 0;
        uint32_t stoc_file_id = 0;
        std::vector<StoCBlockHandle> stoc_block_handles;
        std::vector<ReplicationPair> replication_results;

        uint64_t stoc_queue_depth = 0;
        uint64_t stoc_pending_read_bytes = 0;
        uint64_t stoc_pending_write_bytes = 0;

        // log records.
        char *log_record_mem = nullptr;
        uint64_t log_record_size = 0;
        std::string log_file_name;
        uint64_t thread_id = 0;
        uint32_t db_id = 0;
        uint32_t memtable_id = 0;
        StoCReplicateLogRecordState *replicate_log_record_states = nullptr;
        std::unordered_map<std::string, uint64_t> *logfile_offset = nullptr;
        // compaction request.
        CompactionRequest *compaction = nullptr;
        bool is_ready_for_requests = false;
    };

    class StoCResponse {
    public:
        StoCResponse() {
            is_complete = false;
        }

        std::atomic_bool is_complete;
        uint32_t stoc_file_id = 0;
        std::vector<StoCBlockHandle> stoc_block_handles;
        std::vector<ReplicationPair> replication_results;

        uint64_t stoc_queue_depth;
        uint64_t stoc_pending_read_bytes;
        uint64_t stoc_pending_write_bytes;
        bool is_ready_to_process_requests;
    };

    enum RDMAClientRequestType : char {
        RDMA_CLIENT_REQ_READ = 'a',
        RDMA_CLIENT_REQ_LOG_RECORD = 'b',
        RDMA_CLIENT_REQ_CLOSE_LOG = 'c',
        RDMA_CLIENT_REQ_WRITE_DATA_BLOCKS = 'd',
        RDMA_CLIENT_REQ_DELETE_TABLES = 'e',
        RDMA_CLIENT_READ_STOC_STATS = 'f',
        RDMA_CLIENT_REQ_QUERY_LOG_FILES = 'g',
        RDMA_CLIENT_READ_LOG_FILE = 'h',
        RDMA_CLIENT_FILENAME_STOC_FILE_MAPPING = 'i',
        RDMA_CLIENT_COMPACTION = 'j',
        RDMA_CLIENT_WRITE_SSTABLE_RESPONSE = 'k',
        RDMA_CLIENT_ALLOCATE_LOG_BUFFER_SUCC = 'l',
        RDMA_CLIENT_IS_READY_FOR_REQUESTS = 'm',
        RDMA_CLIENT_RDMA_WRITE_REQUEST = 'n',
        RDMA_CLIENT_RDMA_WRITE_REMOTE_BUF_ALLOCATED = 'o',
        RDMA_CLIENT_RECONSTRUCT_MISSING_REPLICA = 'p',
    };

    struct LevelDBLogRecord {
        Slice key;
        Slice value;
        uint64_t sequence_number = 0;
    };

    struct RDMARequestTask {
        RDMAClientRequestType type;
        sem_t *sem = nullptr;

        char *rdma_log_record_backing_mem = nullptr;
        uint64_t remote_stoc_offset = 0;

        StoCBlockHandle stoc_block_handle = {};
        uint64_t offset = 0;
        uint32_t size = 0;
        char *result = nullptr;
        std::string filename;
        bool is_foreground_reads;

        std::vector<std::string> log_files;

        uint64_t thread_id = 0;
        uint32_t dbid = 0;
        uint32_t memtable_id = 0;
        std::string log_file_name;
        std::vector<LevelDBLogRecord> log_records;

        int server_id = -1;
        std::vector<SSTableStoCFilePair> stoc_file_ids;

        char *write_buf = nullptr;
        std::string dbname;
        uint64_t file_number = 0;
        uint32_t replica_id = 0;
        uint32_t write_size = 0;
        FileInternalType internal_type;

        std::vector<leveldb::ReplicationPair> missing_replicas;

        CompactionRequest *compaction_request = nullptr;

        StoCReplicateLogRecordState *replicate_log_record_states = nullptr;
        std::unordered_map<std::string, uint64_t> *logfile_offset = nullptr;
        std::unordered_map<std::string, uint32_t> fn_stoc_file_id;
        StoCResponse *response = nullptr;
    };

    class LEVELDB_EXPORT StoCClient {
    public:
        virtual uint32_t InitiateCompaction(uint32_t remote_server_id,
                                            CompactionRequest *compaction_request) = 0;

        virtual uint32_t InitiateReplicateSSTables(uint32_t stoc_server_id,
                                                   const std::string& dbname,
                                                   const std::vector<leveldb::ReplicationPair>& pairs) = 0;

        virtual uint32_t
        InitiateRDMAWRITE(uint32_t remote_server_id, char *data,
                          uint32_t size) = 0;

        virtual uint32_t
        InitiateIsReadyForProcessingRequests(uint32_t remote_server_id) = 0;

        virtual uint32_t
        InitiateInstallFileNameStoCFileMapping(uint32_t stoc_id,
                                               const std::unordered_map<std::string, uint32_t> &fn_stocids) = 0;

        virtual uint32_t
        InitiateReadDataBlock(const StoCBlockHandle &block_handle,
                              uint64_t offset,
                              uint32_t size,
                              char *result,
                              uint32_t result_size,
                              std::string filename,
                              bool is_foreground_reads) = 0;

        virtual uint32_t InitiateQueryLogFile(
                uint32_t stoc_id, uint32_t server_id,
                uint32_t dbid,
                std::unordered_map<std::string, uint64_t> *logfile_offset) = 0;

        virtual uint32_t
        InitiateAppendBlock(uint32_t stoc_id,
                            uint32_t thread_id,
                            uint32_t *stoc_file_id,
                            char *buf,
                            const std::string &dbname,
                            uint64_t file_number,
                            uint32_t replica_id,
                            uint32_t size,
                            FileInternalType internal_type) = 0;

        virtual uint32_t
        InitiateDeleteTables(uint32_t stoc_file_id,
                             const std::vector<SSTableStoCFilePair> &stoc_file_ids) = 0;

        virtual uint32_t
        InitiateReadInMemoryLogFile(char *local_buf, uint32_t stoc_id,
                                    uint64_t remote_offset, uint64_t size) = 0;

        virtual uint32_t
        InitiateReplicateLogRecords(const std::string &log_file_name,
                                    uint64_t thread_id,
                                    uint32_t db_id,
                                    uint32_t memtable_id,
                                    char *rdma_backing_mem,
                                    const std::vector<LevelDBLogRecord> &log_records,
                                    StoCReplicateLogRecordState *replicate_log_record_states) = 0;


        virtual uint32_t
        InitiateCloseLogFiles(const std::vector<std::string> &log_files,
                              uint32_t dbid) = 0;

        virtual uint32_t InitiateReadStoCStats(uint32_t stoc_id) = 0;

        virtual bool OnRecv(ibv_wc_opcode type, uint64_t wr_id,
                            int remote_server_id, char *buf,
                            uint32_t imm_data,
                            bool *generate_a_new_request) = 0;

        virtual bool
        IsDone(uint32_t req_id, StoCResponse *response, uint64_t *timeout) = 0;
    };
}


#endif //NOVA_STOC_CLIENT_H
