
//
// Created by Haoyu Huang on 6/18/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "source_migration.h"

#include "db.h"
#include "db/db_impl.h"

namespace leveldb {
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
            msg_sizes.push_back(db->EncodeDBMetadata(buf));
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