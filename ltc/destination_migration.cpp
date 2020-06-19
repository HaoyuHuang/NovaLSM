
//
// Created by Haoyu Huang on 6/18/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "destination_migration.h"
#include "log/log_recovery.h"

namespace leveldb {
    void DestinationMigration::Start() {
        // Open this new database;
        sem_wait(&db_->init_lsmtree_meta_signal_);
        // Wait for lsm tree metadata.
        // build lsm tree.
        // now accept request.
        NOVA_ASSERT(db_->lsmtree_meta_buf_);
        Slice buf(db_->lsmtree_meta_buf_, db_->options_.max_stoc_file_size);

        uint32_t version_size;
        uint32_t srs_size;
        uint32_t memtable_size;
        uint32_t lookup_index_size;
        uint32_t tableid_mapping_size;
        uint64_t last_sequence = 0;
        uint64_t next_file_number = 0;
        NOVA_ASSERT(DecodeFixed32(&buf, &version_size));
        NOVA_ASSERT(DecodeFixed32(&buf, &srs_size));
        NOVA_ASSERT(DecodeFixed32(&buf, &memtable_size));
        NOVA_ASSERT(DecodeFixed32(&buf, &lookup_index_size));
        NOVA_ASSERT(DecodeFixed32(&buf, &tableid_mapping_size));
        NOVA_ASSERT(DecodeFixed64(&buf, &last_sequence));
        NOVA_ASSERT(DecodeFixed64(&buf, &next_file_number));
        db_->versions_->Restore(&buf, last_sequence, next_file_number);

        SubRanges *srs = new SubRanges;
        srs->Decode(&buf);
        db_->DecodeMemTablePartitions(&buf);
        db_->lookup_index_->Decode(&buf);
        db_->versions_->DecodeTableIdMapping(&buf);
        db_->subrange_manager_->latest_subranges_.store(srs);

        // Recover memtables from log files.
        // TODO: Log File Name
        std::vector<MemTableLogFilePair> memtables_to_recover;
        LogRecovery log_recovery;
        for (int i = 0; i < db_->partitioned_active_memtables_.size(); i++) {
            auto partition = db_->partitioned_active_memtables_[i];
            MemTableLogFilePair pair = {};
            if (partition->memtable) {
                pair.memtable = partition->memtable;
                pair.logfile = nova::LogFileName(
                        db_->server_id_,
                        db_->dbid_, partition->memtable->memtableid());
                memtables_to_recover.push_back(pair);
            }
            for (int j = 0; j < partition->closed_log_files.size(); j++) {
                pair.memtable = db_->versions_->mid_table_mapping_[partition->closed_log_files[j]]->memtable_;
                // TODO: Use the server id of prior configuration.
                pair.logfile = nova::LogFileName(db_->server_id_, db_->dbid_,
                                                 partition->closed_log_files[j]);
            }
        }
        log_recovery.Recover(memtables_to_recover);
        db_->is_ready_to_process_request = true;
        db_->memtable_available_signal_.SignalAll();
    }
}