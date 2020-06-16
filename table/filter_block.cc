// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <common/nova_console_logging.h>
#include <fmt/core.h>
#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
    static const size_t kFilterBaseLg = 11;
    static const size_t kFilterBase = 1 << kFilterBaseLg;

    FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy *policy)
            : policy_(policy) {}

    void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
        uint64_t filter_index = (block_offset / kFilterBase);
        assert(filter_index >= filter_offsets_.size());
        while (filter_index > filter_offsets_.size()) {
            GenerateFilter();
        }
    }

    void FilterBlockBuilder::AddKey(const Slice &key) {
        Slice k = key;
        start_.push_back(keys_.size());
        keys_.append(k.data(), k.size());
    }

    Slice FilterBlockBuilder::Finish() {
        if (!start_.empty()) {
            GenerateFilter();
        }

        // Append array of per-filter offsets
        const uint32_t filter_block_size = result_.size();
        for (size_t i = 0; i < filter_offsets_.size(); i++) {
            PutFixed32(&result_, filter_offsets_[i]);
        }
        PutFixed32(&result_, filter_offsets_.size());
        PutFixed32(&result_, filter_block_size);
        result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
        return Slice(result_);
    }

    void FilterBlockBuilder::GenerateFilter() {
        const size_t num_keys = start_.size();
        if (num_keys == 0) {
            // Fast path if there are no keys for this filter
            filter_offsets_.push_back(result_.size());
            return;
        }

        // Make list of keys from flattened key structure
        start_.push_back(keys_.size());  // Simplify length computation
        tmp_keys_.resize(num_keys);
        for (size_t i = 0; i < num_keys; i++) {
            const char *base = keys_.data() + start_[i];
            size_t length = start_[i + 1] - start_[i];
            tmp_keys_[i] = Slice(base, length);
        }

        // Generate filter for current set of keys and append to result_.
        filter_offsets_.push_back(result_.size());
        policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys),
                              &result_);

        tmp_keys_.clear();
        keys_.clear();
        start_.clear();
    }

    FilterBlockReader::FilterBlockReader(const FilterPolicy *policy,
                                         const Slice &contents)
            : policy_(policy), data_(nullptr),
              size_(contents.size()),
              base_lg_(0) {
        size_t n = contents.size();
        if (n < 5)
            return;  // 1 byte for base_lg_ and 4 for start of offset array
        base_lg_ = contents[n - 1];
        filter_size_ = DecodeFixed32(contents.data() + n - 5);
        uint32_t num_filter_offsets = DecodeFixed32(contents.data() + n - 9);

        NOVA_ASSERT(base_lg_ == kFilterBaseLg);
        uint32_t size = filter_size_ + num_filter_offsets * 4 + 4 + 4 + 1;
        NOVA_ASSERT(size == n) << fmt::format("{} {}", size, n);
        data_ = contents.data();

        for (int i = 0; i < num_filter_offsets; i++) {
            filter_offsets_.push_back(
                    DecodeFixed32(data_ + filter_size_ + i * 4));
        }
    }

    bool
    FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice &key) {
        uint64_t index = block_offset / kFilterBase;
        if (index >= filter_offsets_.size()) {
            return true;
        }
        uint32_t start = filter_offsets_[index];
        uint32_t end = filter_size_;
        if (index + 1 < filter_offsets_.size()) {
            end = filter_offsets_[index + 1];
        }
        Slice filter = Slice(data_ + start, end - start);
        return policy_->KeyMayMatch(key, filter);
    }

    std::string FilterBlockReader::DebugString(uint64_t block_offset,
                                               const leveldb::Slice &key) {
        std::string debug;
        uint64_t index = block_offset / kFilterBase;
        if (index < filter_offsets_.size()) {
            uint32_t start = filter_offsets_[index];
            uint32_t end = filter_size_;
            if (index + 1 < filter_offsets_.size()) {
                end = filter_offsets_[index + 1];
            }
            debug = fmt::format("{} {}", start, end);
        }
        return debug;
    }

}  // namespace leveldb