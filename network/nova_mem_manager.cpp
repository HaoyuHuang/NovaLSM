
//
// Created by Haoyu Huang on 4/8/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova_mem_manager.h"
#include "nova_common.h"

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
    // the first two mbs are hash tables.
//    uint64_t seg_size = CUCKOO_SEGMENT_SIZE_MB * 1024 * 1024;
//    uint32_t nsegs = nindexsegments;
//    auto *index_backing_buf = (char *) malloc(seg_size * nsegs);
//    RDMA_ASSERT(index_backing_buf != nullptr);
//    local_cuckoo_table_ = new NovaCuckooHashTable(index_backing_buf, nsegs, 2, 4, 16);

    uint64_t data_size = NovaConfig::config->cache_size_gb * 1024 * 1024 * 1024;
    uint64_t index_size = NovaConfig::config->index_size_mb * 1024 * 1024;
    uint64_t location_cache_size = NovaConfig::config->lc_size_mb * 1024 * 1024;
    local_ht_ = new ChainedHashTable(buf, index_size, /*enable_eviction=*/
                                     false, /*index_only=*/false,
                                     NovaConfig::config->nindex_entry_per_bucket,
                                     NovaConfig::config->main_bucket_mem_percent);
    if (location_cache_size != 0) {
        char *location_cache_buf = buf + index_size;
        location_cache_ = new ChainedHashTable(location_cache_buf,
                                               location_cache_size, /*enable_eviction=*/
                                               true, /*index_only=*/true,
                                               NovaConfig::config->lc_nindex_entry_per_bucket,
                                               NovaConfig::config->lc_main_bucket_mem_percent);
    }
    char *data_buf = buf + index_size + location_cache_size;
    uint64_t slab_size = SLAB_SIZE_MB * 1024 * 1024;
    uint32_t size = 64;
    for (int i = 0; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
        if (size % CHUNK_ALIGN_BYTES) {
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        }
        slab_classes_[i].size = size;
        slab_classes_[i].nitems_per_slab = slab_size / size;
        size *= SLAB_SIZE_FACTOR;
        RDMA_LOG(DEBUG) << "slab class " << i << " size:" << size << " nitems:"
                        << slab_size / size;
    }
    uint64_t ndataslabs = data_size / slab_size;
    RDMA_LOG(INFO) << " nslabs: " << ndataslabs;
    free_slabs_ = (Slab **) malloc(ndataslabs * sizeof(Slab *));
    free_slab_index_ = ndataslabs - 1;
    char *slab_buf = data_buf;
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
            RDMA_LOG(WARNING) << "item larger than 1MB " << size;
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

char *NovaMemManager::ItemEvict(uint32_t scid) {
    if (slab_classes_[scid].nslabs() == 0) {
        return nullptr;
    }

    pthread_mutex_lock(&slab_class_mutex_[scid]);
    uint32_t last_access_time = UINT32_MAX;
    char *lru_buf = nullptr;
    IndexEntry lru_entry{};
    uint64_t lru_hv = 0;

    int candidates = 0;
    uint64_t locked_buckets[MAX_EVICT_CANDIDATES];
    while (candidates < MAX_EVICT_CANDIDATES) {
        locked_buckets[candidates] = 0;
        candidates++;
        uint32_t randV = fastrand();
        uint32_t item_ind = safe_mod(randV,
                                     slab_classes_[scid].nitems_per_slab);
        Slab *slab = slab_classes_[scid].get_slab(
                safe_mod(randV, slab_classes_[scid].nslabs()));
        char *buf = slab->base + slab_classes_[scid].size * item_ind;
        DataEntry data_entry = DataEntry::chars_to_dataitem(buf);
        if (data_entry.nkey == 0) {
            continue;
        }
        uint64_t hv = NovaConfig::keyhash(data_entry.user_key(),
                                          data_entry.nkey);
        if (!local_ht_->TryLock(hv)) {
            continue;
        }
        locked_buckets[candidates] = hv;
//        GetResult entry = local_ht_->Lookup(hv, key, data_entry.nkey,
//                                             true, /*update_time=*/false);
//        if (entry.hash != 0) {
//            if (entry.time < last_access_time) {
//                last_access_time = entry.time;
//                lru_buf = buf;
//                lru_entry = entry;
//                lru_hv = hv;
//            }
//        }
    }

    if (lru_hv != 0) {
        for (uint32_t i = 0; i < MAX_EVICT_CANDIDATES; i++) {
            if (locked_buckets[i] != 0 && locked_buckets[i] != lru_hv) {
                local_ht_->UnlockItem(locked_buckets[i]);
            }
        }
        DataEntry lru_data_entry = DataEntry::chars_to_dataitem(
                (char *) lru_entry.data_ptr);
        char *key = lru_data_entry.user_key();
        local_ht_->Delete(lru_hv, key, lru_data_entry.nkey, true);
        local_ht_->UnlockItem(lru_hv);
    }
    pthread_mutex_unlock(&slab_class_mutex_[scid]);
    return lru_buf;
}

GetResult
NovaMemManager::LocalGet(char *key, uint32_t nkey, bool increment_ref_count) {
    uint64_t hv = NovaConfig::keyhash(key, nkey);
    local_ht_->LockItem(hv);
    GetResult result = local_ht_->Find(hv, key, nkey, true, true,
                                       increment_ref_count, false);
    local_ht_->UnlockItem(hv);
    return result;
}

IndexEntry NovaMemManager::RemoteGet(char *key, uint32_t nkey) {
    if (!location_cache_) {
        return {};
    }
    uint64_t hv = NovaConfig::keyhash(key, nkey);
    location_cache_->LockItem(hv);
    GetResult result = location_cache_->Find(hv, key, nkey, false, true, true,
                                             true);
    location_cache_->UnlockItem(hv);
    return result.index_entry;
}

PutResult NovaMemManager::RemotePut(const IndexEntry &entry) {
    if (!location_cache_) {
        return {};
    }
    location_cache_->LockItem(entry.hash);
    PutResult put_result = location_cache_->Put(entry);
    location_cache_->UnlockItem(entry.hash);
    return put_result;
}

void NovaMemManager::FreeDataEntry(const IndexEntry &index_entry,
                                   const DataEntry &data_entry) {
    uint64_t hv = NovaConfig::keyhash(data_entry.user_key(), data_entry.nkey);
    local_ht_->LockItem(hv);
    uint32_t refs = data_entry.decrement_ref_count();
    local_ht_->UnlockItem(hv);
    if (refs == 0) {
        FreeItem((char *) (data_entry.data), index_entry.slab_class_id);
        RDMA_LOG(DEBUG) << "Free item in slabclass "
                        << index_entry.slab_class_id
                        << " free-list-size:"
                        << slab_classes_[index_entry.slab_class_id].free_list.size();
    }
}

GetResult
NovaMemManager::IQGet(char *key, uint32_t nkey, uint64_t lease_id,
                      bool increment_ref_count) {
    uint64_t hv = NovaConfig::keyhash(key, nkey);
    local_ht_->LockItem(hv);
    GetResult result = local_ht_->Find(hv, key, nkey, true, true,
                                       increment_ref_count, false);
    result.type = GetResultType::SUCCESS;
    if (result.data_entry.empty()) {
        // A miss. Grant I lease.
        char lease_key[nkey + 1];
        lease_key[0] = 'l';
        memcpy(lease_key + 1, key, nkey);

        uint64_t lease_hv = NovaConfig::keyhash(lease_key, nkey + 1);
        GetResult leases = local_ht_->Find(lease_hv, lease_key, nkey + 1, true,
                                           false, false, false);
        if (leases.data_entry.empty()) {
            // No lease.
            LeaseEntry ilease;
            ilease.type = LeaseType::ILEASE;
            ilease.lease_id = lease_id;
            char value[LeaseEntry::size() * 8];
            LeaseEntry::leaseitem_to_chars(value, ilease);
            LocalPut(lease_key, nkey + 1, value, LeaseEntry::size() * 8, false,
                     true);
        } else {
            // Lease.
            char *value = leases.data_entry.user_value();
            RDMA_ASSERT(leases.data_entry.nval % LeaseEntry::size() == 0);
            uint32_t nleases = leases.data_entry.nval / LeaseEntry::size();
            // Back-off.
            char *tmp = value;
            for (uint32_t i = 0; i < nleases; i++) {
                tmp += LeaseEntry::size();
                LeaseEntry lease = LeaseEntry::chars_to_leaseitem(tmp);
                if (lease.empty()) {
                    continue;
                }
                result.type = GetResultType::BACKOFF;
                break;
            }
        }
    }
    local_ht_->UnlockItem(hv);
    return result;
}

PutResult
NovaMemManager::IQSet(char *key, uint32_t nkey, char *val, uint32_t nval,
                      uint64_t lease_id) {
    uint64_t hv = NovaConfig::keyhash(key, nkey);
    char lease_key[nkey + 1];
    lease_key[0] = 'l';
    memcpy(lease_key + 1, key, nkey);
    uint64_t lease_hv = NovaConfig::keyhash(lease_key, nkey + 1);
    PutResult result;

    local_ht_->LockItem(hv);
    GetResult leases = local_ht_->Find(lease_hv, lease_key, nkey + 1, true,
                                       false, false, false);
    if (!leases.data_entry.empty()) {
        // Has lease.
        char *value = leases.data_entry.user_value();
        RDMA_ASSERT(leases.data_entry.nval % LeaseEntry::size() == 0);
        uint32_t nleases = leases.data_entry.nval / LeaseEntry::size();
        // Back-off.
        char *tmp = value;
        for (uint32_t i = 0; i < nleases; i++) {
            LeaseEntry lease = LeaseEntry::chars_to_leaseitem(tmp);
            tmp += LeaseEntry::size();
            if (lease.empty()) {
                continue;
            }
            if (lease.type == LeaseType::ILEASE && lease.lease_id == lease_id) {
                // Success.
                Delete(lease_key, nkey + 1, false);
                result = LocalPut(key, nkey, val, nval, false, false);
                break;
            }
        }
    }
    local_ht_->UnlockItem(hv);
    return result;
}

PutResult
NovaMemManager::QaReg(uint64_t session_id, uint64_t lease_id, char *key,
                      uint32_t nkey, char *val,
                      uint32_t nval) {
    char session_key[1024];
    uint64_t nsessionid = int_to_str(session_key, session_id);
    uint64_t sessionhv = NovaConfig::keyhash(session_key, nsessionid);
    uint64_t hv = NovaConfig::keyhash(key, nkey);

    char lease_key[nkey + 1];
    lease_key[0] = 'l';
    memcpy(lease_key + 1, key, nkey);
    uint64_t lease_hv = NovaConfig::keyhash(lease_key, nkey + 1);
    if (sessionhv < hv) {
        local_ht_->LockItem(sessionhv);
        local_ht_->LockItem(hv);
    } else {
        local_ht_->LockItem(hv);
        local_ht_->LockItem(sessionhv);
    }

    GetResult leases = local_ht_->Find(lease_hv, lease_key, nkey + 1, true,
                                       false, false, false);
    LeaseEntry qlease;
    qlease.type = LeaseType::QLEASE;
    qlease.lease_id = lease_id;
    qlease.session_id = session_id;
    if (leases.data_entry.empty()) {
        // No lease.
        char value[LeaseEntry::size() * 8];
        LeaseEntry::leaseitem_to_chars(value, qlease);
        LocalPut(lease_key, nkey + 1, value, LeaseEntry::size() * 8, false,
                 true);
    } else {
        // Lease.
        char *value = leases.data_entry.user_value();
        RDMA_ASSERT(leases.data_entry.nval % LeaseEntry::size() == 0);
        uint32_t nleases = leases.data_entry.nval / LeaseEntry::size();
        char *tmp = value;
        bool stored = false;
        for (uint32_t i = 0; i < nleases; i++) {
            LeaseEntry lease = LeaseEntry::chars_to_leaseitem(tmp);
            if (lease.empty()) {
                stored = true;
                LeaseEntry::leaseitem_to_chars(tmp, qlease);
                break;
            }
        }
        if (!stored) {
            char new_value[leases.data_entry.nval * 2];
            memcpy(new_value, value, leases.data_entry.nval);
            LeaseEntry::leaseitem_to_chars(new_value + leases.data_entry.nval,
                                           qlease);
            LocalPut(lease_key, nkey + 1, new_value, leases.data_entry.nval * 2,
                     false,
                     true);
        }
    }

    // Store application key in the session entry.
    SessionEntry session;
    session.type = LeaseType::QLEASE;
    session.nkey = nkey;
    GetResult s = local_ht_->Find(sessionhv, session_key, nsessionid, true,
                                  false, false, false);
    if (s.data_entry.empty()) {
        char value[session.size() * 8];
        SessionEntry::sessionitem_to_chars(value, session);
        LocalPut(session_key, nsessionid, value, session.size() * 8, false,
                 true);
    } else {
        char *value = s.data_entry.user_value();
        char *tmp = value;
        uint32_t index = 0;
        while (index < s.data_entry.nval) {
            SessionEntry es = SessionEntry::chars_to_sessionitem(tmp);
            if (es.empty()) {
                break;
            }
            tmp += es.total_size();
            index += es.total_size();
        }
        if (index + session.size() < s.data_entry.nval) {
            SessionEntry::sessionitem_to_chars(tmp, session);
        } else {
            char new_value[s.data_entry.nval * 2];
            memcpy(new_value, value, s.data_entry.nval);
            SessionEntry::sessionitem_to_chars(new_value + s.data_entry.nval,
                                               session);
            LocalPut(session_key, nsessionid, value, s.data_entry.nval * 2,
                     false,
                     true);
        }
    }
    if (sessionhv < hv) {
        local_ht_->UnlockItem(hv);
        local_ht_->UnlockItem(sessionhv);
    } else {
        local_ht_->UnlockItem(sessionhv);
        local_ht_->UnlockItem(hv);
    }
    PutResult result;
    result.success = true;
    return result;
}

PutResult NovaMemManager::Commit(uint64_t session_id) {
    char session_key[1024];
    uint64_t nsessionid = int_to_str(session_key, session_id);
    uint64_t sessionhv = NovaConfig::keyhash(session_key, nsessionid);
    novalist::NovaList<uint64_t> hashes;
    novalist::NovaList<SessionEntry> entries;
    hashes.append(sessionhv);

    local_ht_->LockItem(sessionhv);
    // Read the values.
    GetResult s = local_ht_->Find(sessionhv, session_key, nsessionid, true,
                                  false, false, false);
    char *value = s.data_entry.user_value();
    char *tmp = value;
    uint32_t index = 0;
    while (index < s.data_entry.nval) {
        SessionEntry es = SessionEntry::chars_to_sessionitem(tmp);
        if (es.empty()) {
            break;
        }
        es.key = tmp;
        entries.append(es);
        tmp += es.total_size();
        index += es.total_size();
    }
    local_ht_->UnlockItem(sessionhv);

    // Sort the hashes and acquire locks. 
    uint64_t *values = hashes.values();
    std::sort(values, values + hashes.size());
    for (uint32_t i = 0; i < hashes.size(); i++) {
        local_ht_->LockItem(values[i]);
    }

    // Sort the keys and acquire locks.
    for (uint32_t i = 0; i < entries.size(); i++) {
        SessionEntry entry = entries.value(i);
        RDMA_ASSERT(entry.type == LeaseType::QLEASE);
        char lease_key[entry.nkey + 1];
        lease_key[0] = 'l';
        memcpy(lease_key + 1, entry.key, entry.nkey);
        uint64_t lease_hv = NovaConfig::keyhash(lease_key, entry.nkey + 1);
        // Remove the Q lease.
        GetResult leases = local_ht_->Find(lease_hv, lease_key, entry.nkey + 1,
                                           true,
                                           false, false, false);
        RDMA_ASSERT(!leases.data_entry.empty());
        // Lease.
        char *value = leases.data_entry.user_value();
        RDMA_ASSERT(leases.data_entry.nval % LeaseEntry::size() == 0);
        uint32_t nleases = leases.data_entry.nval / LeaseEntry::size();
        char *tmp = value;
        uint32_t available_leases = 0;
        for (uint32_t i = 0; i < nleases; i++) {
            LeaseEntry lease = LeaseEntry::chars_to_leaseitem(tmp);
            if (lease.empty()) {
                tmp += LeaseEntry::size();
                continue;
            }
            available_leases++;
            if (lease.session_id == session_id) {
                RDMA_ASSERT(lease.type == LeaseType::QLEASE);
                memset(tmp, 0, LeaseEntry::size());
                available_leases--;
            }
            tmp += LeaseEntry::size();
        }
        if (available_leases == 0) {
            Delete(lease_key, entry.nkey + 1, false);
        }
        if (NovaConfig::config->cache_mode == NovaCacheMode::WRITE_AROUND) {
            Delete(entry.key, entry.nkey, false);
        }
    }
    Delete(session_key, nsessionid, false);
    for (uint32_t i = 0; i < hashes.size(); i++) {
        local_ht_->UnlockItem(values[i]);
    }
}

PutResult
NovaMemManager::LocalPut(char *key, uint32_t nkey, char *val, uint32_t nval,
                         bool acquire_ht_lock, bool delete_old_item) {
    uint64_t hv = NovaConfig::keyhash(key, nkey);
    uint32_t ntotal = DataEntry::sizeof_data_entry(nkey, nval);
    uint32_t scid = slabclassid(ntotal);
    if (scid == MAX_NUMBER_OF_SLAB_CLASSES) {
        return {};
    }
    char *buf = ItemAlloc(scid);
    if (buf == nullptr) {
        RDMA_ASSERT(false) << "Evict not supported";
        buf = ItemEvict(scid);
    }
    if (buf != nullptr) {
        RDMA_LOG(DEBUG) << "Putting entry to slabclass " << scid << " scsize "
                        << slab_classes_[scid].size << " free-list-size:"
                        << slab_classes_[scid].free_list.size() << " size "
                        << ntotal;
        DataEntry::dataitem_to_chars(buf, key, nkey, val, nval, 0);
        uint64_t data_size = DataEntry::sizeof_data_entry(nkey, nval);
        uint32_t refs = 1;

        if (acquire_ht_lock) {
            local_ht_->LockItem(hv);
        }
        PutResult result = local_ht_->Put(buf, scid, hv, key, nkey, data_size,
                                          true);
        if (result.old_index_entry.type == IndexEntryType::DATA) {
            refs = result.old_data_entry.decrement_ref_count();
        }
        RDMA_ASSERT(result.success);
        if (acquire_ht_lock) {
            local_ht_->UnlockItem(hv);
        }
        if (result.old_index_entry.type == IndexEntryType::DATA) {
            if (refs == 0 || delete_old_item) {
                auto *data = (char *) result.old_index_entry.data_ptr;
                FreeItem(data, result.old_index_entry.slab_class_id);
                RDMA_LOG(DEBUG) << "Free item in slabclass "
                                << result.old_index_entry.slab_class_id
                                << " free-list-size:"
                                << slab_classes_[scid].free_list.size();
            }
        }
        return result;
    }
    RDMA_LOG(WARNING) << "not enough memory";
    return {};
}

PutResult
NovaMemManager::Delete(char *key, uint32_t nkey, bool acquire_ht_lock) {
    uint64_t hv = NovaConfig::keyhash(key, nkey);
    uint32_t ntotal = 0;
    uint32_t scid = 0;
    uint32_t refs = 1;
    if (acquire_ht_lock) {
        local_ht_->LockItem(hv);
    }
    PutResult result = local_ht_->Delete(hv, key, nkey, true);
    if (result.old_index_entry.type == IndexEntryType::DATA) {
        refs = result.old_data_entry.decrement_ref_count();
        ntotal = DataEntry::sizeof_data_entry(nkey, result.old_data_entry.nval);
        scid = slabclassid(ntotal);
    }
    RDMA_ASSERT(result.success);
    if (acquire_ht_lock) {
        local_ht_->UnlockItem(hv);
    }
    if (result.old_index_entry.type == IndexEntryType::DATA) {
        if (refs == 0) {
            auto *data = (char *) result.old_index_entry.data_ptr;
            FreeItem(data, result.old_index_entry.slab_class_id);
            RDMA_LOG(DEBUG) << "Free item in slabclass "
                            << result.old_index_entry.slab_class_id
                            << " free-list-size:"
                            << slab_classes_[scid].free_list.size();
        }
    }
}