
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

    class NovaMemManager {
    public:
        NovaMemManager(char *buf);

        GetResult
        LocalGet(char *key, uint32_t nkey, bool increment_ref_count = true);

        PutResult
        LocalPut(char *key, uint32_t nkey, char *val, uint32_t nval,
                 bool acquire_ht_lock, bool delete_old_item);

        PutResult Delete(char *key, uint32_t nkey, bool acquire_ht_lock);

        void PrintHashTable() {
            local_ht_->PrintTable();
        };

        IndexEntry RemoteGet(char *key, uint32_t nkey);

        PutResult RemotePut(const IndexEntry &entry);

        void
        FreeDataEntry(const IndexEntry &index_entry,
                      const DataEntry &data_entry);

        char *ItemAlloc(uint32_t scid);

        void FreeItem(char *buf, uint32_t scid);

        void FreeItems(const std::vector<char *> &items, uint32_t scid);

        char *ItemEvict(uint32_t scid);

        uint32_t slabclassid(uint32_t size);

    private:
        pthread_mutex_t slab_class_mutex_[MAX_NUMBER_OF_SLAB_CLASSES];
        SlabClass slab_classes_[MAX_NUMBER_OF_SLAB_CLASSES];

        pthread_mutex_t free_slabs_mutex_;
        Slab **free_slabs_ = nullptr;
        uint64_t free_slab_index_ = 0;

        ChainedHashTable *local_ht_ = nullptr;
        ChainedHashTable *location_cache_ = nullptr;
    };
}


#endif //RLIB_NOVA_MEM_MANAGER_H
