
//
// Created by Haoyu Huang on 4/22/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef NOVA_CHAINED_HASHTABLE_H
#define NOVA_CHAINED_HASHTABLE_H

#include <atomic>
#include <pthread.h>
#include "nova_common.h"
#include "nova_config.h"

namespace nova {
    enum IndexEntryType : uint8_t {
        EMPTY = 0,
        MAIN_HEADER = 1,
        INDRECT_HEADER = 2,
        DATA = 3
    };

    enum GetResultType : uint8_t {
        BACKOFF = 0,
        SUCCESS = 1,
    };

    struct PutResult {
        bool success = false;
        IndexEntry old_index_entry = {};
        DataEntry old_data_entry = {};
    };

    struct GetResult {
        IndexEntry index_entry = {};
        DataEntry data_entry = {};
        GetResultType type;
    };

    class ChainedHashTable {
    public:

        ChainedHashTable(char *backing_mem, uint64_t index_size,
                         bool enable_eviction, bool index_only,
                         uint32_t nindex_entry_per_bucket,
                         uint32_t main_bucket_mem_percent) {
            gettimeofday(&create_time_, nullptr);
            memset(backing_mem, 0, index_size);
            auto main_bucket_mem_size =
                    static_cast<uint64_t>(index_size / 100) *
                    main_bucket_mem_percent;
            uint64_t free_indirect_bucket_mem_size =
                    index_size - main_bucket_mem_size;
            index_base_ = backing_mem;
            nindex_entry_per_bucket_ = nindex_entry_per_bucket;
            nbuckets_ = static_cast<uint32_t>(main_bucket_mem_size /
                                              bucket_size());
            enable_eviction_ = enable_eviction;
            index_only_ = index_only;

            free_buckets_base_ = backing_mem + main_bucket_mem_size;
            nfree_buckets_ = static_cast<uint32_t>(
                    free_indirect_bucket_mem_size /
                    bucket_size());
            free_bucket_index_ = 0;
            is_full_.store(false);
            NOVA_LOG(INFO) << "Hash table index entry size "
                           << IndexEntry::size()
                           << " bucket size "
                           << bucket_size();
            NOVA_LOG(INFO) << "Create " << nbuckets_ << " novalsm buckets and "
                           << nfree_buckets_ << " free indirect buckets.";

            item_locks_ = (pthread_mutex_t *) calloc(nbuckets_,
                                                     sizeof(pthread_mutex_t));
            if (!item_locks_) {
                perror("Can't allocate item locks");
                exit(1);
            }
            for (int i = 0; i < nbuckets_; i++) {
                pthread_mutex_init(&item_locks_[i], nullptr);
            }
        }

        void LockItem(uint64_t hv) {
            pthread_mutex_lock(&item_locks_[bucket_index(hv)]);
        }

        bool TryLock(uint64_t hv) {
            return pthread_mutex_trylock(&item_locks_[bucket_index(hv)]) == 0;
        }

        GetResult Find(uint64_t hv, char *key, uint32_t nkey, bool check_key,
                       bool update_time, bool increment_ref_count,
                       bool return_index_only) {
            // The bucket is locked.
            GetResult result;
            struct timeval now{};
            gettimeofday(&now, nullptr);
            uint32_t access_time = (now.tv_sec - create_time_.tv_sec) * 1000 +
                                   (now.tv_usec - create_time_.tv_usec) / 1000;
            uint32_t index = bucket_index(hv);
            char *bucket = Bucket(index);
            while (bucket) {
                bool has_indirect_header = false;
                for (uint32_t i = 0; i < nindex_entry_per_bucket_; i++) {
                    char *index_entry_buf = bucket + i * IndexEntry::size();
                    IndexEntry index_entry = IndexEntry::chars_to_indexitem(
                            index_entry_buf);
                    if (index_entry.type == IndexEntryType::EMPTY) {
                        // empty.
                        continue;
                    }
                    if (index_entry.type == IndexEntryType::INDRECT_HEADER) {
                        bucket = (char *) index_entry.data_ptr;
                        has_indirect_header = true;
                        break;
                    }
                    NOVA_ASSERT(index_entry.type == IndexEntryType::DATA);
                    if (index_entry.hash == hv) {
                        if (return_index_only) {
                            result.index_entry = index_entry;
                            return result;
                        }
                        DataEntry data_entry = DataEntry::chars_to_dataitem(
                                (char *) index_entry.data_ptr);
                        // found
                        if (check_key) {
                            if (data_entry.nkey != nkey ||
                                memcmp(key, data_entry.user_key(), nkey) != 0) {
                                continue;
                            }
                        }
                        if (update_time) {
                            index_entry.write_time(index_entry_buf,
                                                   access_time);
                        }
                        if (increment_ref_count) {
                            data_entry.increment_ref_count();
                        }
                        result.data_entry = data_entry;
                        result.index_entry = index_entry;
                        return result;
                    }
                }
                if (!has_indirect_header) {
                    break;
                }
            }
            return {};
        }

        PutResult
        Delete(uint64_t hv, char *key, uint32_t nkey, bool check_key) {
            PutResult result;
            result.success = false;
            uint32_t index = bucket_index(hv);
            char *bucket = Bucket(index);
            while (bucket) {
                bool has_indirect_header = false;
                for (uint32_t i = 0; i < nindex_entry_per_bucket_; i++) {
                    char *index_entry_buf = bucket + i * IndexEntry::size();
                    IndexEntry index_entry = IndexEntry::chars_to_indexitem(
                            index_entry_buf);
                    if (index_entry.type == IndexEntryType::EMPTY) {
                        // empty.
                        continue;
                    }
                    if (index_entry.type == IndexEntryType::INDRECT_HEADER) {
                        bucket = (char *) index_entry.data_ptr;
                        has_indirect_header = true;
                        break;
                    }
                    NOVA_ASSERT(index_entry.type == IndexEntryType::DATA);
                    DataEntry data_entry = DataEntry::chars_to_dataitem(
                            (char *) index_entry.data_ptr);
                    if (index_entry.hash == hv) {
                        // found
                        if (check_key) {
                            if (data_entry.nkey != nkey ||
                                memcmp(key, data_entry.user_key(), nkey) != 0) {
                                continue;
                            }
                        }
                        auto old_data_ptr = (char *) index_entry.data_ptr;
                        data_entry.write_stale(old_data_ptr);
                        index_entry.write_type(index_entry_buf,
                                               IndexEntryType::EMPTY);
                        result.old_index_entry = index_entry;
                        result.old_data_entry = data_entry;
                        return result;
                    }
                }
                if (!has_indirect_header) {
                    break;
                }
            }
            return result;
        }

        PutResult Put(const IndexEntry &new_index_entry) {
            return Put(new_index_entry, nullptr, 0, false);
        }

        PutResult
        Put(const char *data_ptr, uint32_t slab_class_id, uint64_t hv,
            char *key,
            uint32_t nkey, uint64_t ndata_size, bool check_key) {
            if (index_only_) {
                NOVA_ASSERT(
                        data_ptr == nullptr && slab_class_id == 0 &&
                        !check_key);
            }
            // The novalsm header is locked.
            IndexEntry new_index_entry{};
            new_index_entry.type = IndexEntryType::DATA;
            new_index_entry.slab_class_id = slab_class_id;
            new_index_entry.hash = hv;
            new_index_entry.data_size = ndata_size;
            new_index_entry.data_ptr = (uint64_t) data_ptr;
            new_index_entry.time = UINT32_MAX; // max so we deprioritize evicting newly inserted entry.
            return Put(new_index_entry, key, nkey, check_key);
        }

        PutResult
        Put(const IndexEntry &new_index_entry, char *key, uint32_t nkey,
            bool check_key) {
            PutResult result{};
            result.success = false;
            uint64_t hv = new_index_entry.hash;
            uint32_t index = bucket_index(hv);
            NOVA_LOG(DEBUG) << "associate item " << hv << " to " << index;
            char *bucket = Bucket(index);
            char *empty_index_entry_buf = nullptr;
            char *lru_index_entry_buf = nullptr;
            uint64_t lru_entry_time = UINT64_MAX;
            // Search the table until either the key already exists or the key does not exist.
            while (bucket) {
                bool has_indirect_header = false;
                for (uint32_t i = 0; i < nindex_entry_per_bucket_; i++) {
                    char *index_entry_buf = bucket + i * IndexEntry::size();
                    IndexEntry index_entry = IndexEntry::chars_to_indexitem(
                            index_entry_buf);
                    if (index_entry.type == IndexEntryType::EMPTY) {
                        // the last entry is reserved for indirect bucket.
                        if (i != nindex_entry_per_bucket_ - 1 &&
                            empty_index_entry_buf == nullptr) {
                            empty_index_entry_buf = index_entry_buf;
                        }
                        continue;
                    } else if (index_entry.type ==
                               IndexEntryType::INDRECT_HEADER) {
                        bucket = (char *) index_entry.data_ptr;
                        has_indirect_header = true;
                        break;
                    }
                    if (index_entry.time < lru_entry_time) {
                        lru_entry_time = index_entry.time;
                        lru_index_entry_buf = index_entry_buf;
                    }
                    NOVA_ASSERT(index_entry.type == IndexEntryType::DATA);
                    if (index_entry.hash == hv) {
                        if (!index_only_) {
                            // Check the key if it stores pointer to the data entry.
                            auto old_data_ptr = (char *) index_entry.data_ptr;
                            DataEntry data_entry = DataEntry::chars_to_dataitem(
                                    old_data_ptr);
                            if (check_key) {
                                if (data_entry.nkey != nkey ||
                                    memcmp(key, data_entry.user_key(), nkey) !=
                                    0) {
                                    continue;
                                }
                            }
                            // the entry exists in the table.
                            // mark data entry as stale.
                            data_entry.write_stale(old_data_ptr);
                            result.old_data_entry = data_entry;
                        }
                        // overwrite with the new index entry.
                        IndexEntry::indexitem_to_chars(index_entry_buf,
                                                       new_index_entry);
                        // compute and write the new checksum.
                        new_index_entry.compute_and_write_checksum(
                                index_entry_buf);
                        result.old_index_entry = index_entry;
                        result.success = true;
                        return result;
                    }
                }
                if (!has_indirect_header) {
                    break;
                }
            }

            if (!empty_index_entry_buf) {
                // Try create indirect header bucket.
                if (!is_full_.load()) {
                    char *index_entry_buf = bucket +
                                            (nindex_entry_per_bucket_ - 1) *
                                            IndexEntry::size();
                    IndexEntry index_entry = IndexEntry::chars_to_indexitem(
                            index_entry_buf);
                    NOVA_ASSERT(index_entry.type == 0);
                    NOVA_LOG(DEBUG) << "Create indirect header for key " << hv;
                    indirect_header_mutex.lock();
                    if (free_bucket_index_ < nfree_buckets_) {
                        index_entry.type = IndexEntryType::INDRECT_HEADER;
                        index_entry.data_ptr = (uint64_t) (free_buckets_base_ +
                                                           free_bucket_index_ *
                                                           bucket_size());
                        IndexEntry::indexitem_to_chars(index_entry_buf,
                                                       index_entry);
                        empty_index_entry_buf = (char *) index_entry.data_ptr;
                        free_bucket_index_++;
                    } else {
                        is_full_.store(true);
                    }
                    indirect_header_mutex.unlock();
                }
            }

            // Put the new entry to the empty space.
            if (empty_index_entry_buf) {
                IndexEntry::indexitem_to_chars(empty_index_entry_buf,
                                               new_index_entry);
                IndexEntry test = IndexEntry::chars_to_indexitem(
                        empty_index_entry_buf);
                // compute and write the new checksum.
                new_index_entry.compute_and_write_checksum(
                        empty_index_entry_buf);
                result.success = true;
                return result;
            }

            // No space left. overwrite the LRU entry.
            if (enable_eviction_ && lru_index_entry_buf) {
                result.old_index_entry = IndexEntry::chars_to_indexitem(
                        lru_index_entry_buf);
                IndexEntry::indexitem_to_chars(lru_index_entry_buf,
                                               new_index_entry);
                //  Compute and write the new checksum.
                new_index_entry.compute_and_write_checksum(lru_index_entry_buf);
                result.success = true;
                return result;
            }
            return result;
        }

        void UnlockItem(uint64_t hv) {
            pthread_mutex_unlock(&item_locks_[bucket_index(hv)]);
        }

        void PrintTable() {
        }

        uint32_t bucket_index(uint64_t hv) {
            return static_cast<uint32_t>(hv) % nbuckets_;
        }

        uint32_t bucket_size() {
            return IndexEntry::size() * nindex_entry_per_bucket_;
        }

        char *Bucket(uint32_t index) {
            return index_base_ + index * bucket_size();
        }

    private:
        struct timeval create_time_{};
        char *index_base_;
        uint32_t nbuckets_;
        uint32_t nindex_entry_per_bucket_;
        pthread_mutex_t *item_locks_;

        std::mutex indirect_header_mutex;
        char *free_buckets_base_;
        uint32_t free_bucket_index_;
        uint32_t nfree_buckets_;
        std::atomic<bool> is_full_{};
        bool enable_eviction_;
        bool index_only_;
    };
}
#endif //NOVA_CHAINED_HASHTABLE_H
