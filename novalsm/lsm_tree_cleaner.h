//
// Created by haoyuhua on 8/6/20.
//

#ifndef LEVELDB_LSM_TREE_CLEANER_H
#define LEVELDB_LSM_TREE_CLEANER_H

#include "log/stoc_log_manager.h"
#include "ltc/stoc_client_impl.h"

namespace leveldb {
    class LSMTreeCleaner {
    public:
        LSMTreeCleaner(nova::StoCInMemoryLogFileManager *log_manager, leveldb::StoCBlockClient *client);

        void FlushingMemTables() const;

        void CleanLSM() const;

        void CleanLSMAfterCfgChange() const;

    private:
        nova::StoCInMemoryLogFileManager *log_manager_;
        leveldb::StoCBlockClient *client_;
    };
}


#endif //LEVELDB_LSM_TREE_CLEANER_H
