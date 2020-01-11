
//
// Created by Haoyu Huang on 4/8/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <nova/nova_config.h>
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
        memset(buf, 0, item_size_);
        return buf;
    }

    char *SlabClass::AllocItem() {
        // check free list first.
        if (free_list.size() > 0) {
            char *ptr = free_list.poll();
            RDMA_ASSERT(ptr != nullptr);
            return ptr;
        }

        if (slabs.size() == 0) {
            return nullptr;
        }

        Slab *slab = slabs.value(slabs.size() - 1);
        return slab->AllocItem();
    }

    void SlabClass::FreeItem(char *buf) {
        free_list.append(buf);
    }

    void SlabClass::AddSlab(Slab *slab) {
        slabs.append(slab);
    }

    NovaMemManager::NovaMemManager(char *buf) {
        uint64_t slab_size = SLAB_SIZE_MB * 1024 * 1024;
        uint64_t size = 1024;
        for (int i = 0; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
            slab_classes_[i].size = size;
            slab_classes_[i].nitems_per_slab = slab_size / size;
            RDMA_LOG(INFO) << "slab class " << i << " size:" << size
                           << " nitems:"
                           << slab_size / size;
            size *= SLAB_SIZE_FACTOR;
        }
        uint64_t ndataslabs =
                NovaConfig::config->mem_pool_size_gb * 1024 * 1024 * 1024 /
                slab_size;
        RDMA_LOG(INFO) << " nslabs: " << ndataslabs;
        free_slabs_ = (Slab **) malloc(ndataslabs * sizeof(Slab *));
        free_slab_index_ = ndataslabs - 1;
        char *slab_buf = buf;
        for (int i = 0; i < ndataslabs; i++) {
            auto *slab = new Slab(slab_buf);
            free_slabs_[i] = slab;
            slab_buf += slab_size;
        }

        pthread_mutex_init(&free_slabs_mutex_, nullptr);
        for (auto &i : slab_class_mutex_) {
            pthread_mutex_init(&i, nullptr);
        }
    }

    uint32_t NovaMemManager::slabclassid(uint32_t size) {
        uint32_t res = 0;
        if (size == 0 || size > SLAB_SIZE_MB * 1024 * 1024)
            return 0;
        while (size > slab_classes_[res].size) {
            res++;
            if (res == MAX_NUMBER_OF_SLAB_CLASSES) {
                /* won't fit in the biggest slab */
                RDMA_LOG(WARNING) << "item larger than 2MB " << size;
                return MAX_NUMBER_OF_SLAB_CLASSES;
            }
        }
        return res;
    }

    char *NovaMemManager::ItemAlloc(uint32_t scid) {
        char *free_item = nullptr;
        Slab *slab = nullptr;

        pthread_mutex_lock(&slab_class_mutex_[scid]);
        free_item = slab_classes_[scid].AllocItem();
        if (free_item != nullptr) {
            pthread_mutex_unlock(&slab_class_mutex_[scid]);
            return free_item;
        }
        pthread_mutex_unlock(&slab_class_mutex_[scid]);

        // Grab a slab from the free list.
        pthread_mutex_lock(&free_slabs_mutex_);
        if (free_slab_index_ == -1) {
            pthread_mutex_unlock(&free_slabs_mutex_);
            return nullptr;
        }
        slab = free_slabs_[free_slab_index_];
        free_slab_index_--;
        pthread_mutex_unlock(&free_slabs_mutex_);

        //
        slab->Init(static_cast<uint32_t>(slab_classes_[scid].size));
        pthread_mutex_lock(&slab_class_mutex_[scid]);
        slab_classes_[scid].AddSlab(slab);
        free_item = slab->AllocItem();
        pthread_mutex_unlock(&slab_class_mutex_[scid]);
        return free_item;
    }

    void NovaMemManager::FreeItem(char *buf, uint32_t scid) {
        memset(buf, 0, slab_classes_[scid].size);
        pthread_mutex_lock(&slab_class_mutex_[scid]);
        slab_classes_[scid].FreeItem(buf);
        pthread_mutex_unlock(&slab_class_mutex_[scid]);
    }

    void NovaMemManager::FreeItems(const std::vector<char *> &items,
                                   uint32_t scid) {
        pthread_mutex_lock(&slab_class_mutex_[scid]);
        for (auto buf : items) {
            slab_classes_[scid].FreeItem(buf);
        }
        pthread_mutex_unlock(&slab_class_mutex_[scid]);
    }
}