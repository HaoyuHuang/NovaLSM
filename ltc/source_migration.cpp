
//
// Created by Haoyu Huang on 6/18/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "source_migration.h"

namespace leveldb {
    void SourceMigration::Start() {
        // bump up the configuration id.

        // dump the latest version, subranges, log files, lookup index, and table id mapping.
        uint32_t scid = mem_manager_->slabclassid(0,
                                                  db_->options_.max_stoc_file_size);
        char *buf = mem_manager_->ItemAlloc(0, scid);
        uint32_t msg_size = 1 + 4 + 4 + 4 + 4 + 4 + 8 + 8;

        // wait for all pending writes to complete.
        db_->mutex_.Lock();
        AtomicVersion *atomic_version = db_->versions_->versions_[db_->versions_->current_version_id()];
        Version *v = atomic_version->Ref();
        // Get all log files.
        uint32_t version_size = v->Encode(buf + msg_size);
        msg_size += version_size;
        SubRanges *srs = db_->subrange_manager_->latest_subranges_;
        uint32_t srs_size = srs->Encode(buf + msg_size);
        msg_size += srs_size;
        uint32_t memtables_size = db_->EncodeMemTablePartitions(buf + msg_size);
        db_->mutex_.Unlock();

        uint32_t lookup_index_size = db_->lookup_index_->Encode(buf + msg_size);
        msg_size += lookup_index_size;
        uint32_t tableid_mapping_size = db_->versions_->EncodeTableIdMapping(
                buf + msg_size, db_->memtable_id_seq_);
        {
            uint32_t header_size = 1;
            buf[0] = StoCRequestType::LTC_MIGRATION;
            EncodeFixed32(buf + header_size, version_size);
            header_size += 4;
            EncodeFixed32(buf + header_size, srs_size);
            header_size += 4;
            EncodeFixed32(buf + header_size, memtables_size);
            header_size += 4;
            EncodeFixed32(buf + header_size, lookup_index_size);
            header_size += 4;
            EncodeFixed32(buf + header_size, tableid_mapping_size);
            header_size += 4;
            EncodeFixed64(buf + header_size, db_->versions_->last_sequence_);
            header_size += 8;
            EncodeFixed64(buf + header_size, db_->versions_->next_file_number_);
        }

        NOVA_ASSERT(msg_size < db_->options_.max_stoc_file_size);
        // Inform the destination of the buf offset.
        client_->InitiateRDMAWRITE(0, buf, msg_size);
        client_->Wait();

        // Wait for the source to free the memory.
    }
}