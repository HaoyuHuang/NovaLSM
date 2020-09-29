
//
// Created by Haoyu Huang on 6/19/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "log_recovery.h"
#include "db/db_impl.h"
#include "common/nova_config.h"

namespace leveldb {
    namespace {
        uint64_t time_diff(timeval t1, timeval t2) {
            return (t2.tv_sec - t1.tv_sec) * 1000000 +
                   (t2.tv_usec - t1.tv_usec);
        }
    }

    LogRecovery::LogRecovery(leveldb::MemManager *mem_manager, leveldb::StoCBlockClient *client) : mem_manager_(
            mem_manager), client_(client) {
    }

    void
    LogRecovery::Recover(const std::unordered_map<uint32_t, leveldb::MemTableLogFilePair> &memtables_to_recover,
                         uint32_t cfg_id, uint32_t dbid) {
        if (memtables_to_recover.empty()) {
            return;
        }
        std::vector<char *> rdma_bufs;
        std::vector<uint32_t> reqs;
        timeval start = {};
        gettimeofday(&start, nullptr);
        for (const auto &replica : memtables_to_recover) {
            uint32_t scid = mem_manager_->slabclassid(0, nova::NovaConfig::config->max_stoc_file_size);
            char *rdma_buf = mem_manager_->ItemAlloc(0, scid);
            NOVA_ASSERT(rdma_buf);
            rdma_bufs.push_back(rdma_buf);
            NOVA_ASSERT(!replica.second.server_logbuf.empty());

            uint32_t server_id = replica.second.server_logbuf.begin()->first;
            uint64_t remote_offset = replica.second.server_logbuf.begin()->second;
            uint32_t reqid = client_->InitiateReadInMemoryLogFile(rdma_buf, server_id, remote_offset,
                                                                  nova::NovaConfig::config->max_stoc_file_size);
            NOVA_LOG(rdmaio::INFO)
                << fmt::format("Restore memtable-{} from server-{} offset:{}", replica.first, server_id,
                               remote_offset);
            reqs.push_back(reqid);
        }

        // Wait for all RDMA READ to complete.
        for (const auto &replica : memtables_to_recover) {
            client_->Wait();
        }

        for (int i = 0; i < reqs.size(); i++) {
            leveldb::StoCResponse response;
            NOVA_ASSERT(client_->IsDone(reqs[i], &response, nullptr));
        }

        timeval rdma_read_complete;
        gettimeofday(&rdma_read_complete, nullptr);
        uint32_t recovered_log_records = 0;
        int index = 0;
        leveldb::DBImpl *dbimpl = reinterpret_cast<leveldb::DBImpl *>(nova::NovaConfig::config->cfgs[cfg_id]->fragments[dbid]->db);
        uint32_t rand_seed = 0;
        for (const auto &replica : memtables_to_recover) {
            char *buf = rdma_bufs[index];
            leveldb::Slice slice(buf, nova::NovaConfig::config->max_stoc_file_size);

            leveldb::MemTable *memtable = replica.second.memtable;
            leveldb::LevelDBLogRecord record = {};
            uint32_t log_records = 0;
            while (nova::DecodeLogRecord(&slice, &record)) {
                memtable->Add(record.sequence_number, leveldb::ValueType::kTypeValue, record.key, record.value);
                recovered_log_records += 1;
                log_records += 1;
            }
            memtable->SetReadyToProcessRequests();
            // Schedule for compaction.
            if (replica.second.is_immutable) {
                int thread_id = -1;
                bool merge_memtables_without_flushing = false;
                if (replica.second.subrange) {
                    thread_id = replica.second.subrange->GetCompactionThreadId(
                            &EnvBGThread::bg_flush_memtable_thread_id_seq,
                            &merge_memtables_without_flushing);
                } else {
                    thread_id =
                            EnvBGThread::bg_flush_memtable_thread_id_seq.fetch_add(
                                    1, std::memory_order_relaxed) %
                            dbimpl->bg_flush_memtable_threads_.size();
                }
                dbimpl->ScheduleFlushMemTableTask(thread_id, memtable->memtableid(), memtable, replica.second.partition_id,
                                                  replica.second.imm_slot, &rand_seed,
                                                  merge_memtables_without_flushing);
            }
            index++;
            NOVA_LOG(rdmaio::INFO)
                << fmt::format("Recovery memtable-{} with {} log records", memtable->memtableid(), log_records);
            uint32_t scid = mem_manager_->slabclassid(0, nova::NovaConfig::config->max_stoc_file_size);
            mem_manager_->FreeItem(0, buf, scid);
        }

        timeval end{};
        gettimeofday(&end, nullptr);
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("memtable recovery duration: {},{},{},{}",
                           memtables_to_recover.size(),
                           recovered_log_records,
                           time_diff(start, rdma_read_complete),
                           time_diff(start, end));
    }
}