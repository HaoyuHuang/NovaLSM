//
// Created by haoyu on 9/24/20.
//

#include "flush_order.h"

namespace leveldb {
    std::string ImpactedDranges::DebugString() const {
        return fmt::format("[{},{}]:{}", lower_drange_index, upper_drange_index, generation_id);
    }

    std::string ImpactedDrangeCollection::DebugString() const {
        std::string msg;
        for (auto &dranges : impacted_dranges) {
            msg = fmt::format("{},{}", msg, dranges.DebugString());
        }
        return msg;
    }

    FlushOrder::FlushOrder(std::vector<MemTablePartition *> *partitioned_active_memtables)
            : partitioned_active_memtables_(partitioned_active_memtables), impacted_dranges_(nullptr),
              latest_generation_id(INIT_GEN_ID) {
    }


    bool FlushOrder::IsSafeToFlush(uint32_t drange_idx, uint64_t generation_id) {
        if (!nova::NovaConfig::config->use_ordered_flush) {
            return true;
        }
//         Safe only if older memtables of all overlapping dranges are flushed.
        auto dranges_col = impacted_dranges_.load();
        if (!dranges_col) {
            return true;
        }
        bool safe_to_flush = true;
        for (const auto &dranges : dranges_col->impacted_dranges) {
            if (drange_idx >= dranges.lower_drange_index && drange_idx <= dranges.upper_drange_index &&
                generation_id >= dranges.generation_id) {
                // overlap and this memtable is newer than the generation of the impacted dranges.
                for (uint32_t idx = dranges.lower_drange_index; idx <= dranges.upper_drange_index; idx++) {
                    auto memtable_partition = (*partitioned_active_memtables_)[idx];
                    memtable_partition->mutex.Lock();
                    auto it = memtable_partition->generation_num_memtables_.begin();
                    while (it != memtable_partition->generation_num_memtables_.end()) {
                        if (it->first < dranges.generation_id) {
                            safe_to_flush = false;
                            NOVA_LOG(rdmaio::INFO) << fmt::format("Cannot flush {}, partition:{}", generation_id,
                                                                  memtable_partition->DebugString());
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
            if (!safe_to_flush) {
                break;
            }
        }
        return safe_to_flush;
    }

    void FlushOrder::UpdateImpactedDranges(const ImpactedDranges &impacted_dranges) {
        if (!nova::NovaConfig::config->use_ordered_flush) {
            return;
        }
        // Remove dranges where their memtables are in the current/newer generation.
        auto new_col = new ImpactedDrangeCollection;
        auto dranges_col = impacted_dranges_.load();
        if (dranges_col) {
            for (const auto &dranges : dranges_col->impacted_dranges) {
                bool keep = true;
                for (uint32_t idx = dranges.lower_drange_index; idx <= dranges.upper_drange_index; idx++) {
                    auto memtable_partition = (*partitioned_active_memtables_)[idx];
                    memtable_partition->mutex.Lock();
                    auto it = memtable_partition->generation_num_memtables_.begin();
                    while (it != memtable_partition->generation_num_memtables_.end()) {
                        if (it->first < dranges.generation_id) {
                            // some older memtables are not flushed.
                            keep = false;
                            break;
                        }
                        it++;
                    }
                    memtable_partition->mutex.Unlock();
                    if (!keep) {
                        break;
                    }
                }
                if (keep) {
                    //  Keep.
                    new_col->impacted_dranges.push_back(dranges);
                }
            }
        }
        new_col->impacted_dranges.push_back(impacted_dranges);
        NOVA_LOG(rdmaio::INFO) << "Latest flush order: " << new_col->DebugString();
        impacted_dranges_.store(new_col);
        latest_generation_id += 1;
    }
}