
//
// Created by Haoyu Huang on 1/22/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "nova_mc_wb_worker.h"

#include "db/filename.h"
#include "nova/nova_common.h"
#include "nova/nova_config.h"

namespace nova {

    NovaMCWBWorker::NovaMCWBWorker(std::vector<leveldb::DB *> dbs,
                                   leveldb::NovaCCCompactionThread *compact_thread,
                                   leveldb::SSTableManager *sstable_manager,
                                   leveldb::Env *env) : dbs_(dbs),
                                                        compact_thread_(
                                                                compact_thread),
                                                        sstable_manager_(
                                                                sstable_manager),
                                                        env_(env) {

    }

    void NovaMCWBWorker::AddRequest(const nova::WBRequest &req) {
        mutex_.lock();
        requests_.push_back(req);
        mutex_.unlock();
        sem_post(&sem_);
    }

    void NovaMCWBWorker::Run() {
        sem_wait(&sem_);

        mutex_.lock();
        if (requests_.empty()) {
            mutex_.unlock();
            return;
        }
        std::vector<WBRequest> copy(requests_);
        requests_.clear();
        mutex_.unlock();

        std::vector<leveldb::DeleteTableRequest> requests;
        for (auto &req : copy) {
            leveldb::EnvFileMetadata meta;
            meta.level = 0;
            leveldb::WritableFile *file = nullptr;
            std::string tablename = leveldb::TableFileName(req.dbname,
                                                           req.file_number);

            leveldb::Status status = env_->NewWritableFile(
                    tablename, meta, &file);
            RDMA_ASSERT(status.ok()) << status.ToString();

            status = file->Append(
                    leveldb::Slice(req.backing_mem, req.table_size));
            RDMA_ASSERT(status.ok()) << status.ToString();

            file->Sync();
            file->Close();
            delete file;
            sstable_manager_->RemoveSSTable(req.dbname, req.file_number);

            uint32_t sid = 0;
            uint32_t index = 0;
            nova::ParseDBIndexFromDBName(req.dbname, &sid, &index);
            dbs_[index]->EvictFileFromCache(req.file_number);

            leveldb::DeleteTableRequest dtr;
            dtr.dbname = req.dbname;
            dtr.file_number = req.file_number;
            requests.push_back(dtr);
        }

        // Wake up compaction thread to delete replicas of sstables.
        compact_thread_->AddDeleteSSTables(requests);
    }
}