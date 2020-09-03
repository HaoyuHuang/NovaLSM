// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"

#include <stdio.h>

#include <sstream>
#include "common/nova_console_logging.h"
#include "ltc/storage_selector.h"

#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

    void CompactionRequest::FreeMemoryLTC() {
        for (auto f : outputs) {
            delete f;
        }
    }

    void CompactionRequest::FreeMemoryStoC() {
        for (int which = 0; which < 2; which++) {
            for (int i = 0; i < inputs[which].size(); i++) {
                delete inputs[which][i];
            }
        }
        for (auto f : guides) {
            delete f;
        }
    }

    uint32_t CompactionRequest::EncodeRequest(char *buf) {
        char *sendbuf = buf;
        uint32_t msg_size = 0;
        msg_size += EncodeStr(sendbuf + msg_size, dbname);
        msg_size += EncodeFixed64(sendbuf + msg_size, smallest_snapshot);
        msg_size += EncodeFixed32(sendbuf + msg_size, source_level);
        msg_size += EncodeFixed32(sendbuf + msg_size, target_level);
        for (int which = 0; which < 2; which++) {
            msg_size += leveldb::EncodeFixed32(sendbuf + msg_size,
                                               inputs[which].size());
            for (auto &input : inputs[which]) {
                msg_size += input->Encode(sendbuf + msg_size);
            }
        }
        msg_size += leveldb::EncodeFixed32(sendbuf + msg_size, guides.size());
        for (auto &guide : guides) {
            msg_size += guide->Encode(sendbuf + msg_size);
        }
        msg_size += leveldb::EncodeFixed32(sendbuf + msg_size,
                                           subranges.size());
        for (int i = 0; i < subranges.size(); i++) {
            const auto &sr = subranges[i];
            msg_size += sr.EncodeForCompaction(sendbuf + msg_size, i);
        }
        return msg_size;
    }

    void CompactionRequest::DecodeRequest(char *buf, uint32_t buf_size) {
        uint32_t num_inputs = 0;
        uint32_t num_guides = 0;
        uint32_t num_subranges = 0;

        char *sendbuf = buf;
        sendbuf += DecodeStr(sendbuf, &dbname);

        Slice input(sendbuf, buf_size);
        NOVA_ASSERT(DecodeFixed64(&input, &smallest_snapshot));
        NOVA_ASSERT(DecodeFixed32(&input, &source_level));
        NOVA_ASSERT(DecodeFixed32(&input, &target_level));
        for (int which = 0; which < 2; which++) {
            NOVA_ASSERT(DecodeFixed32(&input, &num_inputs));
            for (int i = 0; i < num_inputs; i++) {
                FileMetaData *meta = new FileMetaData;
                NOVA_ASSERT(meta->Decode(&input, false));
                inputs[which].push_back(meta);
            }
        }
        NOVA_ASSERT(DecodeFixed32(&input, &num_guides));
        for (int i = 0; i < num_guides; i++) {
            FileMetaData *meta = new FileMetaData;
            NOVA_ASSERT(meta->Decode(&input, false));
            guides.push_back(meta);
        }
        NOVA_ASSERT(DecodeFixed32(&input, &num_subranges));
        for (int i = 0; i < num_subranges; i++) {
            SubRange sr = {};
            NOVA_ASSERT(sr.DecodeForCompaction(&input));
            subranges.push_back(std::move(sr));
        }
    }

    uint32_t FileMetaData::Encode(char *buf) const {
        char *dst = buf;
        uint32_t msg_size = 0;

//        int refs = 0;
//        int allowed_seeks = 0;  // Seeks allowed until compaction
//        //
//        std::set<uint32_t> memtable_ids;
//        uint64_t number = 0;
//        uint64_t file_size = 0;    // File size in bytes in original SSTable format.
//        uint64_t converted_file_size = 0; // File size in bytes after converted to StoC file.
//        uint64_t flush_timestamp = 0;
//        uint32_t level = 0;
//        InternalKey smallest;  // Smallest internal key served by table
//        InternalKey largest;   // Largest internal key served by table
//        FileCompactionStatus compaction_status;
//        std::vector<FileReplicaMetaData> block_replica_handles = {};

        msg_size += EncodeFixed64(dst + msg_size, number);
        msg_size += EncodeFixed64(dst + msg_size, file_size);
        msg_size += EncodeFixed64(dst + msg_size, converted_file_size);
        msg_size += EncodeStr(dst + msg_size, smallest.Encode().ToString());
        msg_size += EncodeStr(dst + msg_size, largest.Encode().ToString());
        msg_size += EncodeFixed64(dst + msg_size, flush_timestamp);
        msg_size += EncodeFixed32(dst + msg_size, level);
        msg_size += EncodeFixed32(dst + msg_size, memtable_ids.size());
        for (uint32_t mid : memtable_ids) {
            msg_size += EncodeFixed32(dst + msg_size, mid);
        }
        msg_size += EncodeFixed32(dst + msg_size, block_replica_handles.size());
        for (int i = 0; i < block_replica_handles.size(); i++) {
            block_replica_handles[i].meta_block_handle.EncodeHandle(dst + msg_size);
            msg_size += StoCBlockHandle::HandleSize();
            msg_size += EncodeFixed32(dst + msg_size, block_replica_handles[i].data_block_group_handles.size());
            for (auto &handle : block_replica_handles[i].data_block_group_handles) {
                handle.EncodeHandle(dst + msg_size);
                msg_size += StoCBlockHandle::HandleSize();
            }
        }
        parity_block_handle.EncodeHandle(dst + msg_size);
        msg_size += StoCBlockHandle::HandleSize();
        return msg_size;
    }

    static bool GetInternalKey(Slice *input, InternalKey *dst, bool copy) {
        Slice str;
        if (DecodeStr(input, &str, copy)) {
            return dst->DecodeFrom(str);
        } else {
            return false;
        }
    }

    bool FileMetaData::DecodeMemTableIds(Slice *ptr) {
        uint32_t num_ids;
        if (!DecodeFixed32(ptr, &num_ids)) {
            return false;
        }
        for (int i = 0; i < num_ids; i++) {
            uint32_t mid = 0;
            if (!DecodeFixed32(ptr, &mid)) {
                return false;
            }
            memtable_ids.insert(mid);
        }
        return true;
    }

    bool FileMetaData::DecodeReplicas(Slice *ptr) {
        uint32_t num_replicas;
        if (!DecodeFixed32(ptr, &num_replicas)) {
            return false;
        }
        for (int i = 0; i < num_replicas; i++) {
            FileReplicaMetaData replica = {};
            if (!StoCBlockHandle::DecodeHandle(ptr,
                                               &replica.meta_block_handle)) {
                return false;
            }
            if (!StoCBlockHandle::DecodeHandles(ptr,
                                                &replica.data_block_group_handles)) {
                return false;
            }
            block_replica_handles.push_back(std::move(replica));
        }
        return true;
    }

    bool FileMetaData::Decode(leveldb::Slice *input, bool copy) {
        return DecodeFixed64(input, &number) &&
               DecodeFixed64(input, &file_size) &&
               DecodeFixed64(input, &converted_file_size) &&
               GetInternalKey(input, &smallest, copy) &&
               GetInternalKey(input, &largest, copy) &&
               DecodeFixed64(input, &flush_timestamp) &&
               DecodeFixed32(input, &level) && DecodeMemTableIds(input) && DecodeReplicas(input) &&
               StoCBlockHandle::DecodeHandle(input, &parity_block_handle);
    }

    uint32_t ReplicationPair::Encode(char *buf) const {
        uint32_t msg_size = 0;
        msg_size += EncodeFixed32(buf, source_stoc_file_id);
        buf[msg_size] = internal_type;
        msg_size += 1;
        msg_size += EncodeFixed32(buf + msg_size, source_file_size);
        msg_size += EncodeFixed32(buf + msg_size, dest_stoc_id);
        msg_size += EncodeFixed64(buf + msg_size, sstable_file_number);
        msg_size += EncodeFixed32(buf + msg_size, replica_id);
        msg_size += EncodeFixed32(buf + msg_size, dest_stoc_file_id);
        return msg_size;
    }

    std::string ReplicationPair::DebugString() const {
        return fmt::format("stocfid:{}-meta:{}-fs:{}-dest:{}-fn:{}-rid:{}-destfid:{}",
                           source_stoc_file_id, internal_type,
                           source_file_size, dest_stoc_id, sstable_file_number,
                           replica_id, dest_stoc_file_id);
    }

    bool DecodeInternalFileType(Slice *ptr, FileInternalType *internal_type) {
        char type = (*ptr)[0];
        bool success = true;
        if (type == 'm') {
            *internal_type = FileInternalType::kFileMetadata;
        } else if (type == 'd') {
            *internal_type = FileInternalType::kFileData;
        } else if (type == 'p') {
            *internal_type = FileInternalType::kFileParity;
        } else {
            success = false;
        }
        if (success) {
            *ptr = Slice(ptr->data() + 1, ptr->size() - 1);
        }
        return success;
    }

    bool ReplicationPair::Decode(Slice *ptr) {
        return DecodeFixed32(ptr, &source_stoc_file_id) &&
               DecodeInternalFileType(ptr, &internal_type) &&
               DecodeFixed32(ptr, &source_file_size) &&
               DecodeFixed32(ptr, &dest_stoc_id) &&
               DecodeFixed64(ptr, &sstable_file_number) &&
               DecodeFixed32(ptr, &replica_id) && DecodeFixed32(ptr, &dest_stoc_file_id);
    }

    int FileMetaData::SelectReplica() const {
        if (block_replica_handles.size() == 1) {
            return 0;
        }
        int replica_id = -1;
        auto servers = leveldb::StorageSelector::available_stoc_servers.load();
        for (int i = 0; i < block_replica_handles.size(); i++) {
            if (servers->server_ids.find(block_replica_handles[i].meta_block_handle.server_id) !=
                servers->server_ids.end()) {
                replica_id = i;
                break;
            }
        }
        NOVA_ASSERT(replica_id != -1);
        return replica_id;
    }

    std::string FileMetaData::ShortDebugString() const {
        std::string r;
        r.append("fn:");
        AppendNumberTo(&r, number);
        r.append(" size:");
        AppendNumberTo(&r, file_size);
        r.append(" cs:");
        AppendNumberTo(&r, converted_file_size);
        r.append("[");
        r.append(smallest.DebugString());
        r.append(",");
        r.append(largest.DebugString());
        r.append("]");

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
        r.append(" mids:");
        for (auto id : memtable_ids) {
            AppendNumberTo(&r, id);
            r.append(" ");
        }
        r.append(" time:");
        AppendNumberTo(&r, flush_timestamp);
        r.append(" level:");
        AppendNumberTo(&r, level);
        r.append(" replicas:");
        for (int i = 0; i < block_replica_handles.size(); i++) {
            r.append(fmt::format("r[{}]: m-{} d-", i,
                                 block_replica_handles[i].meta_block_handle.DebugString()));
            for (auto &data : block_replica_handles[i].data_block_group_handles) {
                r.append(data.DebugString());
                r.append(" ");
            }
        }
        r.append(" parity:");
        r.append(parity_block_handle.DebugString());
        return r;
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
        // We rely on the fact that the code in table.ltc does not mind us
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
