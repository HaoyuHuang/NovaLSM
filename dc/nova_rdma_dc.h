
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_RDMA_DC_H
#define LEVELDB_NOVA_RDMA_DC_H

#include "nova/nova_rdma_rc_store.h"
#include "mc/nova_mem_manager.h"
#include "nova_dc.h"
#include "nova_dc_client.h"

namespace nova {

    struct RequestContext {
        leveldb::DCRequestType request_type;
        std::string db_name;
        uint32_t file_number;
        char *buf;
        uint32_t sstable_size;
    };

    class NovaRDMADiskComponent : public NovaMsgCallback {
    public:
        NovaRDMADiskComponent(rdmaio::RdmaCtrl *rdma_ctrl,
                              NovaMemManager *mem_manager,
                              leveldb::NovaDiskComponent *dc,
                              LogFileManager *log_manager);

        void Start();

        void
        ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id, int remote_server_id,
                      char *buf, uint32_t imm_data) override;

        NovaRDMAStore *rdma_store_;

    private:
        bool is_running_ = true;

        rdmaio::RdmaCtrl *rdma_ctrl_;
        leveldb::NovaDiskComponent *dc_;
        NovaMemManager *mem_manager_;
        LogFileManager *log_manager_;
        std::map<uint32_t, RequestContext> request_context_map_;
    };
}


#endif //LEVELDB_NOVA_RDMA_DC_H
