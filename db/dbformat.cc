// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"

#include <stdio.h>

#include <sstream>
#include "nova/logging.hpp"

#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

    std::string FileMetaData::ShortDebugString() const {
        std::string r;
        r.append("[");
        r.append(smallest.DebugString());
        r.append(",");
        r.append(largest.DebugString());
        r.append("]");
        r.append(" fn:");
        AppendNumberTo(&r, number);
        return r;
    }

    std::string FileMetaData::DebugString() const {
        std::string r;
        r.append("[");
        r.append(smallest.DebugString());
        r.append(",");
        r.append(largest.DebugString());
        r.append("]");
        r.append(" fn:");
        AppendNumberTo(&r, number);
        r.append(" fs:");
        AppendNumberTo(&r, file_size);
        r.append(" cfs:");
        AppendNumberTo(&r, converted_file_size);
        r.append(" mid:");
        AppendNumberTo(&r, memtable_id);
        r.append(" time:");
        AppendNumberTo(&r, flush_timestamp);
        r.append(" level:");
        AppendNumberTo(&r, level);
        r.append(" meta:");
        r.append(meta_block_handle.DebugString());
        r.append(" data:");
        for (auto &data : data_block_group_handles) {
            r.append(data.DebugString());
            r.append(" ");
        }
        return r;
    }

    uint32_t
    EncodeFileMetaData(const FileMetaData &meta, char *buf, uint32_t buf_size) {
        char *tmp = buf;
        uint32_t used_size = 0;
        EncodeFixed64(tmp + used_size, meta.number);
        used_size += 8;
        EncodeFixed64(tmp + used_size, meta.file_size);
        used_size += 8;
        EncodeFixed64(tmp + used_size, meta.converted_file_size);
        used_size += 8;
        EncodeFixed32(tmp + used_size, meta.data_block_group_handles.size());
        used_size += 4;
        for (auto handle : meta.data_block_group_handles) {

        }

        Slice smallest = meta.smallest.Encode();
        Slice largest = meta.largest.Encode();
        EncodeFixed32(tmp + used_size, smallest.size());
        used_size += 4;
        EncodeFixed32(tmp + used_size, largest.size());
        used_size += 4;
        memcpy(tmp + used_size, smallest.data(), smallest.size());
        used_size += smallest.size();
        memcpy(tmp + used_size, largest.data(), largest.size());
        used_size += largest.size();


        RDMA_ASSERT(used_size < buf_size);
        return used_size;
    }

    void DecodeFileMetaData(const Slice &s, FileMetaData *meta) {
        const char *mem = s.data();
        uint32_t used_size = 0;
        meta->number = DecodeFixed64(mem + used_size);
        used_size += 8;
        meta->file_size = DecodeFixed64(mem + used_size);
        used_size += 8;
        uint32_t smallest_size = DecodeFixed32(mem + used_size);
        used_size += 4;
        uint32_t largest_size = DecodeFixed32(mem + used_size);
        used_size += 4;
        Slice smallest_ik(mem + used_size, smallest_size);
        Slice largest_ik(mem + used_size + smallest_size, largest_size);
        RDMA_ASSERT(meta->smallest.DecodeFrom(smallest_ik, true));
        RDMA_ASSERT(meta->largest.DecodeFrom(largest_ik, true));
    }

    static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
        assert(seq <= kMaxSequenceNumber);
        assert(t <= kValueTypeForSeek);
        return (seq << 8) | t;
    }


    InternalKey::InternalKey(const leveldb::Slice &user_key,
                             leveldb::SequenceNumber s, leveldb::ValueType t) {
        AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
    }

    bool InternalKey::DecodeFrom(const leveldb::Slice &s, bool copy) {
        if (copy) {
            rep_.append(s.data(), s.size());
        } else {
            rep_.assign(s.data(), s.size());
        }
        return !rep_.empty();
    }

    Slice InternalKey::user_key() const {
        return ExtractUserKey(rep_);
    }

    void InternalKey::SetFrom(const leveldb::ParsedInternalKey &p) {
        rep_.clear();
        AppendInternalKey(&rep_, p);
    }

    void InternalKey::Clear() {
        rep_.clear();
    }

    void AppendInternalKey(std::string *result, const ParsedInternalKey &key) {
        result->append(key.user_key.data(), key.user_key.size());
        PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
    }

    std::string ParsedInternalKey::DebugString() const {
        std::ostringstream ss;
        ss << '\'' << EscapeString(user_key.ToString());
//        << "'@" << sequence
//           << ":"
//           << static_cast<int>(type);
        return ss.str();
    }

    std::string ParsedInternalKey::FullDebugString() const {
        std::ostringstream ss;
        ss << '\'' << EscapeString(user_key.ToString())
           << "'@" << sequence
           << ":"
           << static_cast<int>(type);
        return ss.str();
    }

    std::string InternalKey::DebugString() const {
        ParsedInternalKey parsed;
        if (ParseInternalKey(rep_, &parsed)) {
            return parsed.DebugString();
        }
        std::ostringstream ss;
        ss << "(bad)" << EscapeString(rep_);
        return ss.str();
    }

    const char *InternalKeyComparator::Name() const {
        return "leveldb.InternalKeyComparator";
    }

    int
    InternalKeyComparator::Compare(const Slice &akey, const Slice &bkey) const {
        // Order by:
        //    increasing user key (according to user-supplied comparator)
        //    decreasing sequence number
        //    decreasing type (though sequence# should be enough to disambiguate)
        int r = user_comparator_->Compare(ExtractUserKey(akey),
                                          ExtractUserKey(bkey));
        if (r == 0) {
            const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
            const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
            if (anum > bnum) {
                r = -1;
            } else if (anum < bnum) {
                r = +1;
            }
        }
        return r;
    }

    void InternalKeyComparator::FindShortestSeparator(std::string *start,
                                                      const Slice &limit) const {
        // Attempt to shorten the user portion of the key
        Slice user_start = ExtractUserKey(*start);
        Slice user_limit = ExtractUserKey(limit);
        std::string tmp(user_start.data(), user_start.size());
        user_comparator_->FindShortestSeparator(&tmp, user_limit);
        if (tmp.size() < user_start.size() &&
            user_comparator_->Compare(user_start, tmp) < 0) {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            PutFixed64(&tmp,
                       PackSequenceAndType(kMaxSequenceNumber,
                                           kValueTypeForSeek));
            assert(this->Compare(*start, tmp) < 0);
            assert(this->Compare(tmp, limit) < 0);
            start->swap(tmp);
        }
    }

    void InternalKeyComparator::FindShortSuccessor(std::string *key) const {
        Slice user_key = ExtractUserKey(*key);
        std::string tmp(user_key.data(), user_key.size());
        user_comparator_->FindShortSuccessor(&tmp);
        if (tmp.size() < user_key.size() &&
            user_comparator_->Compare(user_key, tmp) < 0) {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            PutFixed64(&tmp,
                       PackSequenceAndType(kMaxSequenceNumber,
                                           kValueTypeForSeek));
            assert(this->Compare(*key, tmp) < 0);
            key->swap(tmp);
        }
    }

    const char *
    InternalFilterPolicy::Name() const { return user_policy_->Name(); }

    void InternalFilterPolicy::CreateFilter(const Slice *keys, int n,
                                            std::string *dst) const {
        // We rely on the fact that the code in table.cc does not mind us
        // adjusting keys[].
        Slice *mkey = const_cast<Slice *>(keys);
        for (int i = 0; i < n; i++) {
            mkey[i] = ExtractUserKey(keys[i]);
            // TODO(sanjay): Suppress dups?
        }
        user_policy_->CreateFilter(keys, n, dst);
    }

    bool
    InternalFilterPolicy::KeyMayMatch(const Slice &key, const Slice &f) const {
        return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
    }

    LookupKey::LookupKey(const Slice &user_key, SequenceNumber s) {
        size_t usize = user_key.size();
        size_t needed = usize + 13;  // A conservative estimate
        char *dst;
        if (needed <= sizeof(space_)) {
            dst = space_;
        } else {
            dst = new char[needed];
        }
        start_ = dst;
        dst = EncodeVarint32(dst, usize + 8);
        kstart_ = dst;
        memcpy(dst, user_key.data(), usize);
        dst += usize;
        EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
        dst += 8;
        end_ = dst;
    }

}  // namespace leveldb
