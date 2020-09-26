//
// Created by haoyu on 9/24/20.
//

#include "flush_order.h"

namespace leveldb {
    FlushOrder::FlushOrder(std::vector<MemTablePartition *> *partitioned_active_memtables)
            : partitioned_active_memtables_(partitioned_active_memtables) {
        latest_generation_id = INIT_GEN_ID;
    }

    bool FlushOrder::IsSafeToFlush(uint32_t drange_idx, MemTable *memtable) {
        // Safe only if older memtables of all overlapping dranges are flushed.
        auto dranges_col = impacted_dranges_.load();
        if (!dranges_col) {
            return true;
        }
        bool safe_to_flush = true;
        for (const auto &dranges : dranges_col->impacted_dranges) {
            if (!safe_to_flush) {
                break;
            }
            if (drange_idx >= dranges.lower_drange_index && drange_idx <= dranges.upper_drange_index &&
                memtable->generation_id_ >= dranges.generation_id) {
                // overlap and this memtable is newer than the generation of the impacted dranges.
                for (uint32_t drange_idx = dranges.lower_drange_index;
                     drange_idx <= dranges.upper_drange_index; drange_idx++) {
                    auto memtable_partition = (*partitioned_active_memtables_)[drange_idx];
                    memtable_partition->mutex.Lock();
                    auto it = memtable_partition->generation_num_memtables_.begin();
                    while (it != memtable_partition->generation_num_memtables_.end()) {
                        if (it->first < memtable->generation_id_) {
                            safe_to_flush = false;
                        } else {
                            break;
                        }
                        it++;
                    }
                    memtable_partition->mutex.Unlock();
                    if (!safe_to_flush) {
                        break;
                    }
                }
            }
        }
        return safe_to_flush;
    }

    void FlushOrder::UpdateImpactedDranges(const ImpactedDranges &impacted_dranges) {
        // Remove dranges where their memtables are in the current/newer generation.
        auto new_col = new ImpactedDrangeCollection;
        auto dranges_col = impacted_dranges_.load();
        if (dranges_col) {
            for (const auto &dranges : dranges_col->impacted_dranges) {
                bool drop = true;
                for (uint32_t drange_idx = dranges.lower_drange_index;
                     drange_idx <= dranges.upper_drange_index; drange_idx++) {
                    auto memtable_partition = (*partitioned_active_memtables_)[drange_idx];
                    memtable_partition->mutex.Lock();
                    auto it = memtable_partition->generation_num_memtables_.begin();
                    while (it != memtable_partition->generation_num_memtables_.end()) {
                        if (it->first < dranges.generation_id) {
                            // some older memtables are not flushed.
                            drop = false;
                        } else {
                            break;
                        }
                        it++;
                    }
                    memtable_partition->mutex.Unlock();
                    if (!drop) {
                        break;
                    }
                }
                if (!drop) {
                    //  Keep.
                    new_col->impacted_dranges.push_back(dranges);
                }
            }
        }
        new_col->impacted_dranges.push_back(impacted_dranges);
        impacted_dranges_.store(new_col);
        latest_generation_id += 1;
    }
}