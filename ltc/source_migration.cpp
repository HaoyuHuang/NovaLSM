
//
// Created by Haoyu Huang on 6/18/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "source_migration.h"

namespace leveldb {
    uint32_t SourceMigration::EncodeDBMeta(DBImpl *db, char *buf, uint32_t dbindex) {
        // dump the latest version, subranges, log files, range index, lookup index, and table id mapping.
        uint32_t msg_size = 1 + 4 + 4 + 4 + 4 + 4 + 4 + 8 + 8;

        // wait for all pending writes to complete.
        db->mutex_.Lock();
        AtomicVersion *atomic_version = db->versions_->versions_[db->versions_->current_version_id()];
        Version *v = atomic_version->Ref();
        // Get all log files.
        uint32_t version_size = v->Encode(buf + msg_size);
        msg_size += version_size;
        SubRanges *srs = db->subrange_manager_->latest_subranges_;
        uint32_t srs_size = srs->Encode(buf + msg_size);
        msg_size += srs_size;
        uint32_t memtables_size = db->EncodeMemTablePartitions(buf + msg_size);
        db->mutex_.Unlock();

        uint32_t lookup_index_size = db->lookup_index_->Encode(buf + msg_size);
        msg_size += lookup_index_size;
        uint32_t tableid_mapping_size = db->versions_->EncodeTableIdMapping(
                buf + msg_size, db->memtable_id_seq_);
        {
            uint32_t header_size = 1;
            buf[0] = StoCRequestType::LTC_MIGRATION;
            EncodeFixed32(buf + header_size, dbindex);
            header_size += 4;
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
            EncodeFixed64(buf + header_size, db->versions_->last_sequence_);
            header_size += 8;
            EncodeFixed64(buf + header_size, db->versions_->next_file_number_);
        }
        NOVA_ASSERT(msg_size < db->options_.max_stoc_file_size);
    }

    void SourceMigration::MigrateDB(
            const std::vector<nova::LTCFragment *> &migrate_frags) {
        // bump up the configuration id.
        std::vector<char *> bufs;
        std::vector<uint32_t> msg_sizes;
        DBImpl *db = reinterpret_cast<DBImpl *>(migrate_frags[0]->db);
        uint32_t scid = mem_manager_->slabclassid(0,
                                                  db->options_.max_stoc_file_size);
        for (auto frag : migrate_frags) {
            DBImpl *db = reinterpret_cast<DBImpl *>(frag->db);
            char *buf = mem_manager_->ItemAlloc(0, scid);
            msg_sizes.push_back(EncodeDBMeta(db, buf, frag->dbid));
            bufs.push_back(buf);
        }

        // Inform the destination of the buf offset.
        for (int i = 0; i < migrate_frags.size(); i++) {
            client_->InitiateRDMAWRITE(migrate_frags[i]->ltc_server_id, bufs[i],
                                       msg_sizes[i]);
        }

        for (int i = 0; i < bufs.size(); i++) {
            client_->Wait();
        }

        for (int i = 0; i < bufs.size(); i++) {
            mem_manager_->FreeItem(0, bufs[i], scid);
        }
    }
}