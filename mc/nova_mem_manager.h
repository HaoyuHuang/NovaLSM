
//
// Created by Haoyu Huang on 4/8/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_MEM_MANAGER_H
#define RLIB_NOVA_MEM_MANAGER_H

#include <cstring>
#include <leveldb/slice.h>
#include "nova_chained_hashtable.h"
#include "nova/linked_list.h"
#include "nova/nova_mem_config.h"

#define NOVA_MEM_PARTITIONS 64

namespace nova {
    class Slab {
    public:
        Slab(char *base);

        void Init(uint32_t item_size);

        char *AllocItem();

        char *base;
    private:
        uint32_t item_size_;
        char *next_;
        uint64_t available_bytes_;
    };

    class SlabClass {
    public:
        char *AllocItem();

        void FreeItem(char *buf);

        void AddSlab(Slab *slab);

        uint64_t nitems_per_slab;
        uint64_t size;
        NovaList<Slab *> slabs;
        NovaList<char *> free_list;

        Slab *get_slab(int index) {
            return slabs.value(index);
        }

        int nslabs() {
            return slabs.size();
        }
    };

    class NovaPartitionedMemManager {
    public:
        NovaPartitionedMemManager(char *buf, uint64_t data_size);

        char *ItemAlloc(uint32_t scid);

        void FreeItem(char *buf, uint32_t scid);

        void FreeItems(const std::vector<char *> &items, uint32_t scid);

        uint32_t slabclassid(uint32_t size);

    private:
        pthread_mutex_t slab_class_mutex_[MAX_NUMBER_OF_SLAB_CLASSES];
        SlabClass slab_classes_[MAX_NUMBER_OF_SLAB_CLASSES];

        pthread_mutex_t free_slabs_mutex_;
        Slab **free_slabs_ = nullptr;
        uint64_t free_slab_index_ = 0;
    };

    class NovaMemManager {
    public:
        NovaMemManager(char *buf);

        char *ItemAlloc(uint64_t key, uint32_t scid);

        void FreeItem(uint64_t key, char *buf, uint32_t scid);

        void FreeItems(uint64_t key, const std::vector<char *> &items,
                       uint32_t scid);

        uint32_t slabclassid(uint64_t key, uint32_t size);

    private:
        std::vector<NovaPartitionedMemManager *> partitioned_mem_managers_;
    };
}


#endif //RLIB_NOVA_MEM_MANAGER_H
