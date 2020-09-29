//
// Created by haoyuhua on 8/6/20.
//

#include <db/db_impl.h>
#include "lsm_tree_cleaner.h"

#include "common/nova_config.h"
#include "log/stoc_log_manager.h"

namespace leveldb {
    LSMTreeCleaner::LSMTreeCleaner(nova::StoCInMemoryLogFileManager *log_manager, leveldb::StoCBlockClient *client)
            : log_manager_(log_manager), client_(client) {

    }

    void LSMTreeCleaner::CleanLSM() const {
        while (true) {
            sleep(5);
            int current_cfg_id = nova::NovaConfig::config->current_cfg_id;
            for (int fragid = 0; fragid < nova::NovaConfig::config->cfgs[current_cfg_id]->fragments.size(); fragid++) {
                auto current_frag = nova::NovaConfig::config->cfgs[current_cfg_id]->fragments[fragid];
                if (current_frag->is_complete_ &&
                    current_frag->ltc_server_id == nova::NovaConfig::config->my_server_id) {
                    auto db = reinterpret_cast<DBImpl *>(current_frag->db);
                    NOVA_ASSERT(db);
                    db->ScheduleFileDeletionTask();
                }
            }
        }
    }

    void LSMTreeCleaner::FlushingMemTables() const {
        while (true) {
            sleep(1);
            int current_cfg_id = nova::NovaConfig::config->current_cfg_id;
            for (int fragid = 0; fragid < nova::NovaConfig::config->cfgs[current_cfg_id]->fragments.size(); fragid++) {
                auto current_frag = nova::NovaConfig::config->cfgs[current_cfg_id]->fragments[fragid];
                if (current_frag->ltc_server_id == nova::NovaConfig::config->my_server_id) {
                    auto db = reinterpret_cast<DBImpl *>(current_frag->db);
                    NOVA_ASSERT(db);
                    db->FlushMemTables(false);
                }
            }
        }
    }

    void LSMTreeCleaner::CleanLSMAfterCfgChange() const {
        int current_cfg_id = nova::NovaConfig::config->current_cfg_id;

        while (true) {
            sleep(1);
            int new_cfg_id = nova::NovaConfig::config->current_cfg_id;
            NOVA_ASSERT(new_cfg_id == current_cfg_id || current_cfg_id + 1 == new_cfg_id);

            // A new config
            if (new_cfg_id == current_cfg_id + 1) {
                sleep(30);
                new_cfg_id = nova::NovaConfig::config->current_cfg_id;
                NOVA_ASSERT(current_cfg_id + 1 == new_cfg_id);

                for (int fragid = 0;
                     fragid < nova::NovaConfig::config->cfgs[current_cfg_id]->fragments.size(); fragid++) {
                    auto old_frag = nova::NovaConfig::config->cfgs[current_cfg_id]->fragments[fragid];
                    auto current_frag = nova::NovaConfig::config->cfgs[new_cfg_id]->fragments[fragid];
                    if (old_frag->ltc_server_id != current_frag->ltc_server_id &&
                        old_frag->ltc_server_id == nova::NovaConfig::config->my_server_id) {
                        auto db = reinterpret_cast<DBImpl *>(old_frag->db);
                        db->StopCompaction();
                        db->StopCoordinatedCompaction();

                        std::unordered_map<std::string, uint64_t> logfile_offset;
                        log_manager_->QueryLogFiles(old_frag->dbid, &logfile_offset);
                        std::vector<std::string> logfiles;
                        for (auto log : logfile_offset) {
                            logfiles.push_back(log.first);
                        }
                        if (!logfiles.empty()) {
                            client_->InitiateCloseLogFiles(logfiles, old_frag->dbid);
                        }
                        NOVA_LOG(rdmaio::INFO)
                            << fmt::format("Clean db-{} with log files:{}", old_frag->dbid, logfiles.size());
                    }
                }
                new_cfg_id = nova::NovaConfig::config->current_cfg_id;
                NOVA_ASSERT(current_cfg_id + 1 == new_cfg_id);
                current_cfg_id = new_cfg_id;
            }
        }
    }
}

