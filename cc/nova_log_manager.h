
//
// Created by Haoyu Huang on 3/18/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_LOG_MANAGER_H
#define LEVELDB_NOVA_LOG_MANAGER_H

#include <vector>
#include "mc/nova_mem_manager.h"
#include "leveldb/env.h"
#include <set>
#include <atomic>

#define MAX_NUM_LOG_FILES 100000


namespace nova {
    struct MemTableIdentifier {
        uint32_t cc_id;
        uint32_t db_id;
        uint32_t memtable_id;

        bool operator<(const MemTableIdentifier &id2)  const {
            if (cc_id < id2.cc_id) {
                return true;
            } else if (cc_id > id2.cc_id) {
                return false;
            }
            if (db_id < id2.db_id) {
                return true;
            } else if (db_id > id2.db_id) {
                return false;
            }
            if (memtable_id < id2.memtable_id) {
                return true;
            }
            return false;
        }
    };

    struct LogRecord {
        MemTableIdentifier memtable_id;
        const char *log_record;
        uint32_t log_record_size;
    };

    class NovaLogFile {
    public:
        NovaLogFile(leveldb::Env *env, uint32_t log_file_id, std::string log_file_name,
                    uint64_t max_file_size);

        bool ReserveSpaceForLogRecord(const LogRecord& record);

        void PersistLogRecords();

        void DeleteMemTables(const std::vector<MemTableIdentifier> &ids);

        uint32_t log_file_id() {
            return log_file_id_;
        }

    private:
        void Seal();

        uint32_t log_file_id_;
        leveldb::Env *env_;
        std::string log_file_name_;
        uint64_t max_file_size_;
        uint64_t current_file_size_;
        std::vector<LogRecord> pending_log_records_;
        std::set<MemTableIdentifier> memtables;
        leveldb::WritableFile *writable_file_;
        bool is_full_ = false;
        bool seal_ = false;
        bool deleted_ = false;
        std::mutex mutex_;
    };

    class NovaLogManager {
    public:
        NovaLogManager(leveldb::Env *env, uint64_t max_file_size,
                       NovaMemManager *mem_manager,
                       const std::string& log_file_path,
                       uint32_t nccs,
                       uint32_t nworkers, uint32_t log_buf_size);

        NovaLogFile *CreateNewLogFile();

        NovaLogFile *log_file(MemTableIdentifier memtable_id);

        NovaLogFile *log_file(uint32_t log_file_id);

        char *rdma_log_buf(uint32_t cc_id,
                           uint32_t worker_id) {
            return init_log_bufs_[cc_id][worker_id];
        }

    private:
        leveldb::Env *env_;
        uint64_t max_file_size_;
        std::string log_file_path_;
        std::mutex mutex_;
        std::atomic_int_fast32_t log_file_id_seq_;
        std::map<MemTableIdentifier, NovaLogFile *> memtableid_log_file_id_map_;
        NovaLogFile *log_files[MAX_NUM_LOG_FILES];
        char ***init_log_bufs_;
    };
}


#endif //LEVELDB_NOVA_LOG_MANAGER_H
