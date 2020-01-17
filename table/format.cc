// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <fmt/core.h>
#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

    void BlockHandle::EncodeTo(std::string *dst) const {
        // Sanity check that all fields have been set
        assert(offset_ != ~static_cast<uint64_t>(0));
        assert(size_ != ~static_cast<uint64_t>(0));
        PutVarint64(dst, offset_);
        PutVarint64(dst, size_);
    }

    Status BlockHandle::DecodeFrom(Slice *input) {
        if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
            return Status::OK();
        } else {
            return Status::Corruption("bad block handle");
        }
    }

    void Footer::EncodeTo(std::string *dst) const {
        const size_t original_size = dst->size();
        metaindex_handle_.EncodeTo(dst);
        index_handle_.EncodeTo(dst);
        dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
        PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
        PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
        assert(dst->size() == original_size + kEncodedLength);
        (void) original_size;  // Disable unused variable warning.
    }

    Status Footer::DecodeFrom(Slice *input) {
        const char *magic_ptr = input->data() + kEncodedLength - 8;
        const uint32_t magic_lo = DecodeFixed32(magic_ptr);
        const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
        const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                                (static_cast<uint64_t>(magic_lo)));

        if (magic != kTableMagicNumber) {
            return Status::Corruption("not an sstable (bad magic number)" +
                                      std::to_string(magic) + " expected " +
                                      std::to_string(kTableMagicNumber));
        }

        Status result = metaindex_handle_.DecodeFrom(input);
        if (result.ok()) {
            result = index_handle_.DecodeFrom(input);
        }
        if (result.ok()) {
            // We skip over any leftover data (just padding for now) in "input"
            const char *end = magic_ptr + 8;
            *input = Slice(end, input->data() + input->size() - end);
        }
        return result;
    }
}  // namespace leveldb
