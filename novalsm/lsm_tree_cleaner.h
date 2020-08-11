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
        LSMTreeCleaner(nova::StoCInMemoryLogFileManager* log_manager, leveldb::StoCBlockClient* client, bool delete_obsolete_files);

        void Start();
    private:
        bool delete_obsolete_files_ = false;
        nova::StoCInMemoryLogFileManager* log_manager_;
        leveldb::StoCBlockClient* client_;

        void CleanLSM() const;
        void CleanLSMAfterCfgChange() const;
    };
}


#endif //LEVELDB_LSM_TREE_CLEANER_H
