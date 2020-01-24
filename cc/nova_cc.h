
//
// Created by Haoyu Huang on 1/11/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_CC_H
#define LEVELDB_NOVA_CC_H

#include <semaphore.h>

#include "util/env_mem.h"
#include "nova_cc_client.h"
#include "leveldb/env.h"

namespace leveldb {

    class NovaCCMemFile : public MemFile {
    public:
        NovaCCMemFile(Env *env, uint64_t file_number,
                      MemManager *mem_manager,
                      SSTableManager *sstable_manager,
                      CCClient *cc_client,
                      const std::string &dbname,
                      uint64_t thread_id,
                      uint64_t file_size);

        ~NovaCCMemFile();

        uint64_t Size() const override { return used_size_; }

        Status
        Read(uint64_t offset, size_t n, Slice *result, char *scratch) override;

        Status Write(uint64_t offset, const Slice &data) override;

        Status Append(const Slice &data) override;

        Status Fsync() override;

        const char *backing_mem() override { return backing_mem_; }

        void set_meta(const FileMetaData &meta) { meta_ = meta; }

        uint64_t used_size() {return used_size_;}

        uint64_t allocated_size() { return allocated_size_; }

        uint64_t thread_id() { return thread_id_; }

        uint64_t file_number() { return file_number_; }

        void WaitForWRITEs();

    private:
        Env *env_;
        uint64_t file_number_;
        const std::string fname_;
        WritableFile *local_writable_file_;
        SSTableManager *sstable_manager_;
        MemManager *mem_manager_;
        CCClient *cc_client_;
        const std::string &dbname_;
        FileMetaData meta_;
        uint64_t thread_id_;

        char *backing_mem_ = nullptr;
        uint64_t allocated_size_ = 0;
        uint64_t used_size_ = 0;
        std::map<uint32_t, uint64_t> remote_sstable_bufs_;
        std::vector<uint32_t> WRITE_requests_;
    };

    class NovaCCRandomAccessFile : public RandomAccessFile {
    public:
        NovaCCRandomAccessFile(Env *env, const std::string &dbname,
                               uint64_t file_number,
                               const FileMetaData &meta,
                               CCClient *dc_client,
                               MemManager *mem_manager,
                               SSTableManager *sstable_manager,
                               uint64_t thread_id,
                               bool prefetch_all);

        ~NovaCCRandomAccessFile() override;

        Status Read(uint64_t offset, size_t n, Slice *result,
                    char *scratch) override;

        bool IsWBTableDeleted();

        WBTable *wb_table() {return wb_table_;}

    private:
        Status ReadAll();

        const std::string &dbname_;
        uint64_t file_number_;
        FileMetaData meta_;

        bool prefetch_all_;
        char *backing_mem_table_ = nullptr;
        char *backing_mem_block_ = nullptr;
        MemManager *mem_manager_;
        SSTableManager *sstable_manager_;
        uint64_t thread_id_;
        CCClient *dc_client_;

        Env *env_;
        WBTable *wb_table_;
        RandomAccessFile *local_ra_file_;
    };

    struct DeleteTableRequest {
        std::string dbname;
        uint32_t file_number;
    };

    class NovaCCCompactionThread : public EnvBGThread {
    public:
        explicit NovaCCCompactionThread(rdmaio::RdmaCtrl *rdma_ctrl);

        void Schedule(
                void (*background_work_function)(void *background_work_arg),
                void *background_work_arg) override;

        CCClient *dc_client_;
        MemManager *mem_manager_;

        CCClient *dc_client() override { return dc_client_; }

        MemManager *mem_manager() override { return mem_manager_; }

        uint64_t thread_id() override { return thread_id_; }

        bool IsInitialized();

        void Start();

        void DeleteMCSSTables();

        void AddDeleteSSTables(const std::vector<DeleteTableRequest> &requests);

        nova::NovaRDMAStore *rdma_store_;
        uint64_t thread_id_ = 0;

    private:
        // Stores the work item data in a Schedule() call.
        //
        // Instances are constructed on the thread calling Schedule() and used on the
        // background thread.
        //
        // This structure is thread-safe beacuse it is immutable.
        struct BackgroundWorkItem {
            explicit BackgroundWorkItem(void (*function)(void *arg),
                                        void *arg)
                    : function(function), arg(arg) {}

            void (*const function)(void *);

            void *const arg;
        };

        rdmaio::RdmaCtrl *rdma_ctrl_;
        port::Mutex background_work_mutex_;
        sem_t signal;
        std::queue<BackgroundWorkItem> background_work_queue_
        GUARDED_BY(background_work_mutex_);

        bool is_running_ = false;
        std::vector<DeleteTableRequest> delete_table_requests_;
        leveldb::port::Mutex mutex_;
    };
}


#endif //LEVELDB_NOVA_CC_H
