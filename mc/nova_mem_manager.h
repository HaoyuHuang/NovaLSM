
//
// Created by Haoyu Huang on 4/8/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_MEM_MANAGER_H
#define RLIB_NOVA_MEM_MANAGER_H

#include <stdint.h>
#include <cstring>
#include <vector>
#include <queue>
#include <mutex>
#include "leveldb/db_types.h"

namespace nova {

#define MAX_NUMBER_OF_SLAB_CLASSES 64
#define SLAB_SIZE_FACTOR 2
#define NOVA_MEM_PARTITIONS 4

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
        std::vector<Slab *> slabs;
        std::queue<char *> free_list;

        Slab *get_slab(int index) {
            return slabs[index];
        }

        int nslabs() {
            return slabs.size();
        }
    };

    class NovaPartitionedMemManager {
    public:
        NovaPartitionedMemManager(int pid, char *buf, uint64_t data_size);

        char *ItemAlloc(uint32_t scid) ;

        void FreeItem(char *buf, uint32_t scid) ;

        void
        FreeItems(const std::vector<char *> &items, uint32_t scid) ;

        uint32_t slabclassid(uint32_t size) ;

    private:
        std::mutex slab_class_mutex_[MAX_NUMBER_OF_SLAB_CLASSES];
        SlabClass slab_classes_[MAX_NUMBER_OF_SLAB_CLASSES];
        std::mutex oom_lock;
        bool print_class_oom = false;
        std::mutex free_slabs_mutex_;
        Slab **free_slabs_ = nullptr;
        uint64_t free_slab_index_ = 0;
    };

    class NovaMemManager : public leveldb::MemManager {
    public:
        NovaMemManager(char *buf, uint32_t num_mem_partitions, uint64_t mem_pool_size_gb);

        char *ItemAlloc(uint64_t key, uint32_t scid) override;

        void FreeItem(uint64_t key, char *buf, uint32_t scid) override;

        void
        FreeItems(uint64_t key, const std::vector<char *> &items, uint32_t scid) override;

        uint32_t slabclassid(uint64_t key, uint32_t size) override;

    private:
        std::vector<NovaPartitionedMemManager *> partitioned_mem_managers_;
    };
}


#endif //RLIB_NOVA_MEM_MANAGER_H
