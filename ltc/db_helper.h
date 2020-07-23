
//
// Created by Haoyu Huang on 6/21/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_DB_HELPER_H
#define LEVELDB_DB_HELPER_H

#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "common/nova_common.h"

namespace leveldb {
    class YCSBKeyComparator : public leveldb::Comparator {
    public:
        //   if a < b: negative result
        //   if a > b: positive result
        //   else: zero result
        int
        Compare(const leveldb::Slice &a, const leveldb::Slice &b) const {
            uint64_t ai = 0;
            nova::str_to_int(a.data(), &ai, a.size());
            uint64_t bi = 0;
            nova::str_to_int(b.data(), &bi, b.size());

            if (ai < bi) {
                return -1;
            } else if (ai > bi) {
                return 1;
            }
            return 0;
        }

        // Ignore the following methods for now:
        const char *Name() const { return "YCSBKeyComparator"; }

        void
        FindShortestSeparator(std::string *,
                              const leveldb::Slice &) const {}

        void FindShortSuccessor(std::string *) const {}
    };

    leveldb::Options
    BuildDBOptions(int cfg_id, int db_index, leveldb::Cache *cache,
                   leveldb::MemTablePool *memtable_pool,
                   leveldb::MemManager *mem_manager,
                   leveldb::StoCClient *stoc_client,
                   const std::vector<leveldb::EnvBGThread *> &bg_compaction_threads,
                   const std::vector<leveldb::EnvBGThread *> &bg_flush_memtable_threads,
                   leveldb::EnvBGThread *reorg_thread,
                   leveldb::EnvBGThread *compaction_coord_thread,
                   leveldb::Env *env);

    leveldb::Options BuildStorageOptions(leveldb::MemManager *mem_manager,
                                         leveldb::Env *env);

    leveldb::DB *CreateDatabase(int cfg_id, int db_index, leveldb::Cache *cache,
                                leveldb::MemTablePool *memtable_pool,
                                leveldb::MemManager *mem_manager,
                                leveldb::StoCClient *stoc_client,
                                const std::vector<leveldb::EnvBGThread *> &bg_compaction_threads,
                                const std::vector<leveldb::EnvBGThread *> &bg_flush_memtable_threads,
                                leveldb::EnvBGThread *reorg_thread,
                                leveldb::EnvBGThread *compaction_coord_thread);
}


#endif //LEVELDB_DB_HELPER_H
