
//
// Created by Haoyu Huang on 2/12/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_RDMA_WRITE_SERVER_WORKER_H
#define LEVELDB_RDMA_WRITE_SERVER_WORKER_H

#include <semaphore.h>

#include "rdma/rdma_msg_callback.h"
#include "rdma/nova_rdma_broker.h"
#include "stoc/persistent_stoc_file.h"
#include "common/nova_mem_manager.h"
#include "bench_common.h"
#include "mock_rtable.h"
#include "rdma_write_client.h"

namespace nova {
    struct ServerWorkerAsyncTask {
        BenchRequestType request_type;
        uint32_t cc_server_thread_id = 0;
        uint32_t dc_req_id = 0;
        uint32_t remote_server_id = 0;
        char *local_buf;
    };

    struct ServerWorkerCompleteTask {
        BenchRequestType request_type;
        uint32_t remote_server_id = 0;
        uint32_t dc_req_id = 0;
    };

    class RDMAWRITEDiskWorker;

    class RDMAWRITEServerWorker : public RDMAMsgCallback {
    public:
        RDMAWRITEServerWorker(uint32_t max_run_time, uint32_t write_size_kb,
                              bool is_local_disk_bench, bool eval_disk_horizontal_scalability, uint32_t server_id);

        void Start();

        bool
        ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id, int remote_server_id,
                      char *buf, uint32_t imm_data, bool *new_request);

        void AddAsyncTask(
                const nova::ServerWorkerAsyncTask &task);

        void AddCompleteTask(const ServerWorkerCompleteTask &task);

        int PullAsyncCQ();

        void
        AddCompleteTasks(const std::vector<ServerWorkerCompleteTask> &tasks);

        uint32_t thread_id_;
        RdmaCtrl *rdma_ctrl_;
        NovaMemManager *mem_manager_;
        nova::NovaRDMABroker *rdma_broker_;
        RDMAWRITEClient *client_;
        leveldb::Env *env_;

        std::string table_path_;
        uint32_t write_size_kb_;
        uint64_t rtable_size_;
        uint32_t max_num_rtables_;

        std::vector<RDMAWRITEDiskWorker *> async_workers_;
        std::list<ServerWorkerCompleteTask> async_cq_;
        MockRTable *rtable_;

        uint32_t processed_number_of_req_ = 0;
    private:
        bool is_local_disk_bench_;
        bool eval_disk_horizontal_scalability_;
        uint32_t server_id_;
        uint32_t max_run_time_;

        struct RequestContext {
            char *local_buf;
            int persisted;
        };
        std::map<uint32_t, RequestContext> req_context_;
        std::mutex mutex_;
    };


    class RDMAWRITEDiskWorker {
    public:
        RDMAWRITEDiskWorker(const std::string &table_path,
                            uint32_t write_size_kb,
                            uint32_t rtable_size,
                            uint32_t max_num_rtables);

        void AddTask(const ServerWorkerAsyncTask &task);

        void Start();

        void Init();

        uint32_t worker_id_;
        MockRTable *rtable_;
        NovaMemManager *mem_manager_;
        std::vector<RDMAWRITEServerWorker *> cc_servers_;
        leveldb::Env *env_;

    private:

        const std::string &table_path_;
        uint32_t write_size_kb_;
        uint32_t rtable_size_;
        uint32_t max_num_rtables_;

        bool is_running_ = true;
        std::mutex mutex_;
        std::list<ServerWorkerAsyncTask> queue_;
        sem_t sem_;
    };
}


#endif //LEVELDB_RDMA_WRITE_SERVER_WORKER_H
