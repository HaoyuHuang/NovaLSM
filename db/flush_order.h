//
// Created by haoyu on 9/24/20.
//

#ifndef LEVELDB_FLUSH_ORDER_H
#define LEVELDB_FLUSH_ORDER_H

#include <atomic>
#include <set>
#include "common/nova_common.h"

#include "leveldb/subrange.h"
#include "memtable.h"
#include "version_set.h"

namespace leveldb {
    struct ImpactedDranges {
        uint32_t lower_drange_index;
        uint32_t upper_drange_index;
        uint64_t generation_id;
    };

    struct ImpactedDrangeCollection {
        std::vector<ImpactedDranges> impacted_dranges;
    };

    class FlushOrder {
    public:
        void UpdateImpactedDranges(const ImpactedDranges& impacted_dranges);

        bool IsSafeToFlush(uint32_t drange_idx, AtomicMemTable* memtable);
    private:
        uint64_t generation_sed_iq;
        std::vector<MemTablePartition *> *partitioned_active_memtables_ = nullptr;
        std::atomic<ImpactedDrangeCollection *> impacted_dranges_;
    };
}


#endif //LEVELDB_FLUSH_ORDER_H
