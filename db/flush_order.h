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

#define  INIT_GEN_ID 1

namespace leveldb {
    struct ImpactedDranges {
        uint32_t lower_drange_index = 0;
        uint32_t upper_drange_index = 0;
        uint64_t generation_id;

        std::string DebugString() const;
    };

    struct ImpactedDrangeCollection {
        std::vector<ImpactedDranges> impacted_dranges;
        std::string DebugString() const;
    };

    class FlushOrder {
    public:
        FlushOrder(std::vector<MemTablePartition *> *partitioned_active_memtables);

        void UpdateImpactedDranges(const ImpactedDranges& impacted_dranges);

        bool IsSafeToFlush(uint32_t drange_idx, uint64_t generation_id);

        std::atomic_uint_fast64_t latest_generation_id;
    private:
        std::vector<MemTablePartition *> *partitioned_active_memtables_ = nullptr;
        std::atomic<ImpactedDrangeCollection *> impacted_dranges_;
    };
}


#endif //LEVELDB_FLUSH_ORDER_H
