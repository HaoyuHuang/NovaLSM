
//
// Created by Haoyu Huang on 1/11/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_CC_H
#define LEVELDB_NOVA_CC_H

#include "util/env_mem.h"
#include "dc/nova_dc_client.h"
#include "leveldb/env.h"

namespace leveldb {

    class NovaCCRemoteMemFile : public MemFile {
    public:
        NovaCCRemoteMemFile(Env *env, uint64_t file_number,
                            MemManager *mem_manager,
                            DCClient *dc_client,
                            const std::string &dbname,
                            uint64_t thread_id,
                            uint64_t file_size);

        ~NovaCCRemoteMemFile();

        uint64_t Size() const override { return used_size_; }

        Status
        Read(uint64_t offset, size_t n, Slice *result, char *scratch) override;

        Status Write(uint64_t offset, const Slice &data) override;

        Status Append(const Slice &data) override;

        Status Fsync() override;

        const char *backing_mem() override { return backing_mem_; }

        void set_meta(const FileMetaData &meta) { meta_ = meta; }

    private:
        Env *env_;
        uint64_t file_number_;
        const std::string fname_;
        WritableFile *local_writable_file_;
        MemManager *mem_manager_;
        DCClient *dc_client_;
        const std::string &dbname_;
        FileMetaData meta_;
        uint64_t thread_id_;

        char *backing_mem_ = nullptr;
        uint32_t allocated_size_ = 0;
        uint32_t used_size_ = 0;
    };

    class NovaCCRemoteRandomAccessFile : public RandomAccessFile {
    public:
        NovaCCRemoteRandomAccessFile(const std::string &dbname,
                                     uint64_t file_number,
                                     const FileMetaData &meta,
                                     DCClient *dc_client,
                                     MemManager *mem_manager,
                                     uint64_t thread_id,
                                     bool cache_all);

        ~NovaCCRemoteRandomAccessFile() override;

        Status Read(uint64_t offset, size_t n, Slice *result,
                    char *scratch) override;

    private:
        Status ReadAll();

        const std::string &dbname_;
        uint64_t file_number_;
        FileMetaData meta_;

        bool prefetch_all_;
        bool done_prefetch_all_;
        char *backing_mem_table_ = nullptr;
        char *backing_mem_block_ = nullptr;
        MemManager *mem_manager_;
        uint64_t thread_id_;
        DCClient *dc_client_;
    };

    class NovaCCCompactionThread : public EnvBGThread {
    public:
        explicit NovaCCCompactionThread(rdmaio::RdmaCtrl *rdma_ctrl);

        void Schedule(
                void (*background_work_function)(void *background_work_arg),
                void *background_work_arg) override;

        DCClient *dc_client_;
        MemManager *mem_manager_;

        DCClient *dc_client() override { return dc_client_; }

        MemManager *mem_manager() override { return mem_manager_; }

        uint64_t thread_id() override { return thread_id_; }

        bool IsInitialized();

        void Start();

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
        port::CondVar background_work_cv_ GUARDED_BY(
                background_work_mutex_);
        std::queue<BackgroundWorkItem> background_work_queue_
        GUARDED_BY(background_work_mutex_);

        bool is_running_ = false;
        leveldb::port::Mutex mutex_;
    };
}


#endif //LEVELDB_NOVA_CC_H
