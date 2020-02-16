
//
// Created by Haoyu Huang on 4/8/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <fmt/core.h>

#include "nova_mem_manager.h"
#include "nova/nova_common.h"

namespace nova {

    Slab::Slab(char *base) {
        next_ = base;
    }

    void Slab::Init(uint32_t item_size) {
        uint64_t size = SLAB_SIZE_MB * 1024 * 1024;
        item_size_ = item_size;
        auto num_items = static_cast<uint32_t>(size / item_size);
        available_bytes_ = item_size * num_items;
    }

    char *Slab::AllocItem() {
        if (available_bytes_ < item_size_) {
            return nullptr;
        }
        char *buf = next_;
        next_ += item_size_;
        available_bytes_ -= item_size_;
        return buf;
    }

    char *SlabClass::AllocItem() {
        // check free list first.
        if (!free_list.empty()) {
            char *ptr = free_list.front();
            RDMA_ASSERT(ptr != nullptr);
            free_list.pop();
            return ptr;
        }

        if (slabs.empty()) {
            return nullptr;
        }

        Slab *slab = slabs[slabs.size() - 1];
        return slab->AllocItem();
    }

    void SlabClass::FreeItem(char *buf) {
        free_list.push(buf);
    }

    void SlabClass::AddSlab(Slab *slab) {
        slabs.push_back(slab);
    }

    NovaPartitionedMemManager::NovaPartitionedMemManager(int pid, char *buf,
                                                         uint64_t data_size) {
        uint64_t slab_size = SLAB_SIZE_MB * 1024 * 1024;
//        uint64_t slab_sizes[] = {8192, 1024 };

        uint64_t size = 1200;
        for (int i = 0; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
            slab_classes_[i].size = size;
            slab_classes_[i].nitems_per_slab = slab_size / size;
            if (pid == 0) {
                RDMA_LOG(INFO) << "slab class " << i << " size:" << size
                               << " nitems:"
                               << slab_size / size;
            }
            size *= SLAB_SIZE_FACTOR;
            if (size > slab_size) {
                size = slab_size;
            }
        }
        uint64_t ndataslabs = data_size / slab_size;
        if (pid == 0) {
            RDMA_LOG(INFO) << " nslabs: " << ndataslabs;
        }
        free_slabs_ = (Slab **) malloc(ndataslabs * sizeof(Slab *));
        free_slab_index_ = ndataslabs - 1;
        char *slab_buf = buf;
        for (int i = 0; i < ndataslabs; i++) {
            auto *slab = new Slab(slab_buf);
            free_slabs_[i] = slab;
            slab_buf += slab_size;
        }
    }

    uint32_t NovaPartitionedMemManager::slabclassid(uint32_t size) {
        RDMA_ASSERT(size > 0 && size < SLAB_SIZE_MB * 1024 * 1024) << size;
        uint32_t res = 0;
        while (size > slab_classes_[res].size) {
            res++;
        }
        RDMA_ASSERT(res < MAX_NUMBER_OF_SLAB_CLASSES) << size;
        return res;
    }

    char *NovaPartitionedMemManager::ItemAlloc(uint32_t scid) {
        char *free_item = nullptr;
        Slab *slab = nullptr;

        slab_class_mutex_[scid].lock();
        free_item = slab_classes_[scid].AllocItem();
        if (free_item != nullptr) {
            slab_class_mutex_[scid].unlock();
            return free_item;
        }
        // Grab a slab from the free list.
        free_slabs_mutex_.lock();
        if (free_slab_index_ == -1) {
            free_slabs_mutex_.unlock();
            slab_class_mutex_[scid].unlock();

            oom_lock.lock();
            if (!print_class_oom) {
                RDMA_LOG(INFO) << "No free slabs: Print slab class usages.";
                print_class_oom = true;
                for (int i = 0; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
                    slab_class_mutex_[scid].lock();
                    RDMA_LOG(INFO) << fmt::format(
                                "slab class {} size:{} nfreeitems:{} slabs:{}",
                                i,
                                slab_classes_[i].size,
                                slab_classes_[i].free_list.size(),
                                slab_classes_[i].slabs.size());
                    slab_class_mutex_[scid].unlock();
                }
            }
            oom_lock.unlock();
            return nullptr;
        }
        slab = free_slabs_[free_slab_index_];
        free_slab_index_--;
        free_slabs_mutex_.unlock();

        slab->Init(static_cast<uint32_t>(slab_classes_[scid].size));

        slab_classes_[scid].AddSlab(slab);
        free_item = slab->AllocItem();
        slab_class_mutex_[scid].unlock();
        return free_item;
    }

    void NovaPartitionedMemManager::FreeItem(char *buf, uint32_t scid) {
//        memset(buf, 0, slab_classes_[scid].size);
        slab_class_mutex_[scid].lock();
        slab_classes_[scid].FreeItem(buf);
        slab_class_mutex_[scid].unlock();
    }

    void NovaPartitionedMemManager::FreeItems(const std::vector<char *> &items,
                                              uint32_t scid) {
        slab_class_mutex_[scid].lock();
        for (auto buf : items) {
            slab_classes_[scid].FreeItem(buf);
        }
        slab_class_mutex_[scid].unlock();
    }

    NovaMemManager::NovaMemManager(char *buf, uint64_t mem_pool_size_gb) {
        uint64_t partition_size = mem_pool_size_gb * 1024 * 1024 * 1024 /
                NOVA_MEM_PARTITIONS;
        char *base = buf;
        for (int i = 0; i < NOVA_MEM_PARTITIONS; i++) {
            partitioned_mem_managers_.push_back(
                    new NovaPartitionedMemManager(i, base, partition_size));
            base += partition_size;
        }
    }

    char *NovaMemManager::ItemAlloc(uint64_t key, uint32_t scid) {
        return partitioned_mem_managers_[key %
                                         partitioned_mem_managers_.size()]->ItemAlloc(
                scid);
    }

    uint32_t NovaMemManager::slabclassid(uint64_t key, uint32_t size) {
        return partitioned_mem_managers_[key %
                                         partitioned_mem_managers_.size()]->slabclassid(
                size);
    }

    void NovaMemManager::FreeItem(uint64_t key, char *buf, uint32_t scid) {
        partitioned_mem_managers_[key %
                                  partitioned_mem_managers_.size()]->FreeItem(
                buf, scid);
    }

    void NovaMemManager::FreeItems(uint64_t key,
                                   const std::vector<char *> &items,
                                   uint32_t scid) {
        partitioned_mem_managers_[key %
                                  partitioned_mem_managers_.size()]->FreeItems(
                items, scid);
    }

}