// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

    static const int kBlockSize = 18874368;

    Arena::Arena(MemManager *mem_manager)
            : alloc_ptr_(nullptr), alloc_bytes_remaining_(0),
              memory_usage_(0), mem_manager_(mem_manager) {
        if (mem_manager) {
            AllocateFallback(kBlockSize);
        }
    }

    Arena::~Arena() {
        if (mem_manager_) {
            for (size_t i = 0; i < blocks_.size(); i++) {
                uint32_t scid = mem_manager_->slabclassid(0, kBlockSize);
                mem_manager_->FreeItem(0, blocks_[i], scid);
            }
        } else {
            for (size_t i = 0; i < blocks_.size(); i++) {
                delete[] blocks_[i];
            }
        }
    }

    char *Arena::AllocateFallback(size_t bytes) {
//        if (bytes > kBlockSize / 4) {
//            // Object is more than a quarter of our block size.  Allocate it separately
//            // to avoid wasting too much space in leftover bytes.
//            char *result = AllocateNewBlock(bytes);
//            return result;
//        }

        // We waste the remaining space in the current block.
        alloc_ptr_ = AllocateNewBlock(kBlockSize);
        alloc_bytes_remaining_ = kBlockSize;

        char *result = alloc_ptr_;
        alloc_ptr_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        return result;
    }

    char *Arena::AllocateAligned(size_t bytes) {
        const int align = (sizeof(void *) > 8) ? sizeof(void *) : 8;
        static_assert((align & (align - 1)) == 0,
                      "Pointer size should be a power of 2");
        size_t current_mod =
                reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
        size_t slop = (current_mod == 0 ? 0 : align - current_mod);
        size_t needed = bytes + slop;
        char *result;
        if (needed <= alloc_bytes_remaining_) {
            result = alloc_ptr_ + slop;
            alloc_ptr_ += needed;
            alloc_bytes_remaining_ -= needed;
        } else {
            // AllocateFallback always returned aligned memory
            result = AllocateFallback(bytes);
        }
        assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
        return result;
    }

    char *Arena::AllocateNewBlock(size_t block_bytes) {
        char *result;
        if (mem_manager_) {
            uint32_t scid = mem_manager_->slabclassid(0, block_bytes);
            result = mem_manager_->ItemAlloc(0, scid);
        } else {
            result = new char[block_bytes];
            blocks_.push_back(result);
            memory_usage_.fetch_add(block_bytes + sizeof(char *),
                                    std::memory_order_relaxed);
        }
        return result;
    }

}  // namespace leveldb
