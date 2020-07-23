
//
// Created by Haoyu Huang on 12/25/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
// RDMA request handler.

#ifndef RDMA_MSG_HANDLER_H
#define RDMA_MSG_HANDLER_H

#include <string>
#include <port/port_stdcxx.h>
#include <leveldb/db.h>
#include "leveldb/options.h"
#include "common/nova_common.h"
#include "rdma/rdma_msg_callback.h"
#include "rdma/nova_rdma_broker.h"
#include <semaphore.h>
#include <list>
#include "ltc/stoc_client_impl.h"
#include "log/stoc_log_manager.h"
#include "log/logc_log_writer.h"
#include "rdma_server.h"
#include "common/nova_config.h"
#include "rdma_admission_ctrl.h"

namespace nova {

    struct CacheValue {
        std::string value;
    };

    class RDMAServerImpl;

    class RDMAMsgHandler : public RDMAMsgCallback {
    public:
        RDMAMsgHandler(RdmaCtrl *rdma_ctrl,
                       NovaMemManager *mem_manager,
                       RDMAAdmissionCtrl *admission_control) :
                rdma_ctrl_(rdma_ctrl), mem_manager_(mem_manager),
                admission_control_(admission_control) {
            stat_tasks_ = 0;
            sem_init(&sem_, 0, 0);
            should_pause = false;
            paused = false;
        }

        bool IsInitialized();

        bool
        ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id, int remote_server_id,
                      char *buf, uint32_t imm_data,
                      bool *generate_a_new_request) override;

        void Start();

        void AddTask(const leveldb::RDMARequestTask &task);

        int size();

        NovaRDMABroker *rdma_broker_ = nullptr;
        leveldb::StoCClient *stoc_client_ = nullptr;
        leveldb::LogCLogWriter *rdma_log_writer_ = nullptr;
        RDMAServerImpl *rdma_server_ = nullptr;
        uint64_t thread_id_ = 0;
        std::atomic_int_fast64_t stat_tasks_;

        bool verify_complete() {
            mutex_.Lock();
            bool vc = verify_complete_;
            mutex_.Unlock();
            return vc;
        }

        std::atomic_bool should_pause;
        std::atomic_bool paused;
        sem_t sem_;
        std::list<leveldb::RDMARequestTask> private_queue_;
    private:
        int ProcessRequestQueue();

        struct RequestCtx {
            uint32_t req_id = 0;
            sem_t *sem = nullptr;
            leveldb::StoCResponse *response = nullptr;
        };

        bool verify_complete_ = false;
        std::list<RequestCtx> pending_reqs_;
        RdmaCtrl *rdma_ctrl_ = nullptr;
        NovaMemManager *mem_manager_ = nullptr;
        bool is_running_ = false;
        RDMAAdmissionCtrl *admission_control_ = nullptr;
        leveldb::port::Mutex mutex_;

        std::list<leveldb::RDMARequestTask> public_queue_;
    };
}


#endif //RDMA_MSG_HANDLER_H
