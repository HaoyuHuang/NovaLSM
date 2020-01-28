
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_CC_SERVER_H
#define LEVELDB_NOVA_CC_SERVER_H

#include "nova/nova_rdma_rc_store.h"
#include "mc/nova_mem_manager.h"
#include "dc/nova_dc.h"
#include "log/nova_log.h"

namespace nova {

    struct RequestContext {
        leveldb::CCRequestType request_type;
        uint32_t remote_server_id;
        std::string db_name;
        uint32_t file_number;
        char *buf;
        uint32_t sstable_size;
    };

    class NovaCCServer : public NovaMsgCallback {
    public:
        NovaCCServer(rdmaio::RdmaCtrl *rdma_ctrl,
                     NovaMemManager *mem_manager,
                     leveldb::NovaDiskComponent *dc,
                     LogFileManager *log_manager);

        void
        ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id, int remote_server_id,
                      char *buf, uint32_t imm_data) override;

        NovaRDMAStore *rdma_store_;
        uint64_t thread_id_;

    private:
        bool is_running_ = true;

        rdmaio::RdmaCtrl *rdma_ctrl_;
        NovaMemManager *mem_manager_;
        leveldb::NovaDiskComponent *dc_;
        LogFileManager *log_manager_;
        leveldb::SSTableManager *sstable_manager_;
        std::map<uint64_t, RequestContext> request_context_map_;
    };
}


#endif //LEVELDB_NOVA_CC_SERVER_H
