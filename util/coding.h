// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Endian-neutral encoding:
// * Fixed-length numbers are encoded with least-significant byte first
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format

#ifndef STORAGE_LEVELDB_UTIL_CODING_H_
#define STORAGE_LEVELDB_UTIL_CODING_H_

#include <cstdint>
#include <cstring>
#include <string>

#include "leveldb/slice.h"
#include "port/port.h"

namespace leveldb {

// Standard Put... routines append to a string
    void PutFixed32(std::string *dst, uint32_t value);

    void PutFixed64(std::string *dst, uint64_t value);

    void PutVarint32(std::string *dst, uint32_t value);

    void PutVarint64(std::string *dst, uint64_t value);

    void PutLengthPrefixedSlice(std::string *dst, const Slice &value);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
    bool GetVarint32(Slice *input, uint32_t *value);

    bool GetVarint64(Slice *input, uint64_t *value);

    bool GetLengthPrefixedSlice(Slice *input, Slice *result);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// nullptr on error.  These routines only look at bytes in the range
// [p..limit-1]
    const char *GetVarint32Ptr(const char *p, const char *limit, uint32_t *v);

    const char *GetVarint64Ptr(const char *p, const char *limit, uint64_t *v);

// Returns the length of the varint32 or varint64 encoding of "v"
    int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
    char *EncodeVarint32(char *dst, uint32_t value);

    char *EncodeVarint64(char *dst, uint64_t value);

// TODO(costan): Remove port::kLittleEndian and the fast paths based on
//               std::memcpy when clang learns to optimize the generic code, as
//               described in https://bugs.llvm.org/show_bug.cgi?id=41761
//
// The platform-independent code in DecodeFixed{32,64}() gets optimized to mov
// on x86 and ldr on ARM64, by both clang and gcc. However, only gcc optimizes
// the platform-independent code in EncodeFixed{32,64}() to mov / str.

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written

    inline uint32_t EncodeFixed32(char *dst, uint32_t value) {
        uint8_t *const buffer = reinterpret_cast<uint8_t *>(dst);

//        if (port::kLittleEndian) {
//            // Fast path for little-endian CPUs. All major compilers optimize this to a
//            // single mov (x86_64) / str (ARM) instruction.
//            std::memcpy(buffer, &value, sizeof(uint32_t));
//            return;
//        }

        // Platform-independent code.
        // Currently, only gcc optimizes this to a single mov / str instruction.
        buffer[0] = static_cast<uint8_t>(value);
        buffer[1] = static_cast<uint8_t>(value >> 8);
        buffer[2] = static_cast<uint8_t>(value >> 16);
        buffer[3] = static_cast<uint8_t>(value >> 24);
        return 4;
    }

    inline uint32_t EncodeStr(char *dst, const std::string &src) {
        EncodeFixed32(dst, src.size());
        dst += 4;
        memcpy(dst, src.data(), src.size());
        return 4 + src.size();
    }

    inline uint32_t EncodeFixed64(char *dst, uint64_t value) {
        uint8_t *const buffer = reinterpret_cast<uint8_t *>(dst);

//        if (port::kLittleEndian) {
//            // Fast path for little-endian CPUs. All major compilers optimize this to a
//            // single mov (x86_64) / str (ARM) instruction.
//            std::memcpy(buffer, &value, sizeof(uint64_t));
//            return;
//        }

        // Platform-independent code.
        // Currently, only gcc optimizes this to a single mov / str instruction.
        buffer[0] = static_cast<uint8_t>(value);
        buffer[1] = static_cast<uint8_t>(value >> 8);
        buffer[2] = static_cast<uint8_t>(value >> 16);
        buffer[3] = static_cast<uint8_t>(value >> 24);
        buffer[4] = static_cast<uint8_t>(value >> 32);
        buffer[5] = static_cast<uint8_t>(value >> 40);
        buffer[6] = static_cast<uint8_t>(value >> 48);
        buffer[7] = static_cast<uint8_t>(value >> 56);
        return 8;
    }

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

    inline uint32_t DecodeFixed32(const char *ptr) {
        const uint8_t *const buffer = reinterpret_cast<const uint8_t *>(ptr);
        // Platform-independent code.
        // Clang and gcc optimize this to a single mov / ldr instruction.
        return (static_cast<uint32_t>(buffer[0])) |
               (static_cast<uint32_t>(buffer[1]) << 8) |
               (static_cast<uint32_t>(buffer[2]) << 16) |
               (static_cast<uint32_t>(buffer[3]) << 24);
    }

    inline uint64_t DecodeFixed64(const char *ptr) {
        const uint8_t *const buffer = reinterpret_cast<const uint8_t *>(ptr);
        // Platform-independent code.
        // Clang and gcc optimize this to a single mov / ldr instruction.
        return (static_cast<uint64_t>(buffer[0])) |
               (static_cast<uint64_t>(buffer[1]) << 8) |
               (static_cast<uint64_t>(buffer[2]) << 16) |
               (static_cast<uint64_t>(buffer[3]) << 24) |
               (static_cast<uint64_t>(buffer[4]) << 32) |
               (static_cast<uint64_t>(buffer[5]) << 40) |
               (static_cast<uint64_t>(buffer[6]) << 48) |
               (static_cast<uint64_t>(buffer[7]) << 56);
    }

    inline uint32_t DecodeStr(const char *src, std::string *result) {
        uint32_t size = DecodeFixed32(src);
        result->append(src + 4, size);
        return size + 4;
    }

    inline bool DecodeFixed32(Slice *ptr, uint32_t *n) {
        if (ptr->size() < 4) {
            return false;
        }
        *n = DecodeFixed32(ptr->data());
        *ptr = Slice(ptr->data() + 4, ptr->size() - 4);
        return true;
    }

    inline bool DecodeFixed64(Slice *ptr, uint64_t *n) {
        if (ptr->size() < 8) {
            return false;
        }
        *n = DecodeFixed64(ptr->data());
        *ptr = Slice(ptr->data() + 8, ptr->size() - 8);
        return true;
    }

    inline bool DecodeStr(Slice *ptr, Slice *str, bool copy) {
        uint32_t n = 0;
        if (DecodeFixed32(ptr, &n) && ptr->size() >= n) {
            if (copy) {
                char *mem = new char[n];
                memcpy(mem, ptr->data(), n);
                *str = Slice(mem, n);
            } else {
                *str = Slice(ptr->data(), n);
            }
            *ptr = Slice(ptr->data() + n, ptr->size() - n);
            return true;
        }
        return false;
    }

    inline uint32_t EncodeSlice(char *dst, const Slice &src) {
        EncodeFixed32(dst, src.size());
        dst += 4;
        memcpy(dst, src.data(), src.size());
        return 4 + src.size();
    }

    inline uint32_t EncodeBool(char *dst, bool value) {
        if (value) {
            dst[0] = '1';
        } else {
            dst[0] = '2';
        }
        return 1;
    }

    inline bool DecodeBool(char *dst) {
        if (dst[0] == '1') {
            return true;
        } else if (dst[0] == '2') {
            return false;
        }
        assert(false);
    }

    inline bool DecodeBool(Slice *ptr, bool* value) {
        if (ptr->size() >= 1) {
            if (ptr->data()[0] == '1') {
                *value = true;
            } else if (ptr->data()[0] == '2') {
                *value = false;
            } else {
                return false;
            }
            *ptr = Slice(ptr->data() + 1, ptr->size() - 1);
            return true;
        }
        return false;
    }

    inline Slice DecodeSlice(const char *src) {
        uint32_t size = DecodeFixed32(src);
        Slice slice(src + 4, size);
        return slice;
    }

    inline bool DecodeStr(Slice *src, std::string *result) {
        uint32_t size;
        if (!DecodeFixed32(src, &size)) {
            return false;
        }
        if (src->size() < size) {
            return false;
        }
        result->append(src->data(), size);
        *src = Slice(src->data() + size, src->size() - size);
        return true;
    }

// Internal routine for use by fallback path of GetVarint32Ptr
    const char *GetVarint32PtrFallback(const char *p, const char *limit,
                                       uint32_t *value);

    inline const char *GetVarint32Ptr(const char *p, const char *limit,
                                      uint32_t *value) {
        if (p < limit) {
            uint32_t result = *(reinterpret_cast<const uint8_t *>(p));
            if ((result & 128) == 0) {
                *value = result;
                return p + 1;
            }
        }
        return GetVarint32PtrFallback(p, limit, value);
    }

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_CODING_H_
