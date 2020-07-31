// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
    static void DumpInternalIter(Iterator* iter) {
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ParsedInternalKey k;
        if (!ParseInternalKey(iter->key(), &k)) {
          fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
        } else {
          fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
        }
      }
    }
#endif

    namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
        class DBIter : public Iterator {
        public:
            // Which direction is the iterator currently moving?
            // (1) When moving forward, the internal iterator is positioned at
            //     the exact entry that yields this->key(), this->value()
            // (2) When moving backwards, the internal iterator is positioned
            //     just before all entries whose user key == this->key().
            enum Direction {
                kForward, kReverse
            };

            DBIter(DBImpl *db, const Comparator *cmp, Iterator *iter, SequenceNumber s, uint32_t seed,
                   const nova::RangePartition &range_partition)
                    : db_(db),
                      user_comparator_(cmp),
                      iter_(iter),
                      sequence_(s),
                      direction_(kForward),
                      valid_(false),
                      rnd_(seed),
                      bytes_until_read_sampling_(RandomCompactionPeriod()),
                      range_partition_(range_partition) {}

            DBIter(const DBIter &) = delete;

            DBIter &operator=(const DBIter &) = delete;

            ~DBIter() override { delete iter_; }

            bool Valid() const override { return valid_; }

            Slice key() const override {
                assert(valid_);
                return (direction_ == kForward) ? ExtractUserKey(iter_->key())
                                                : ExtractUserKey(saved_ikey_);
            }

            Slice value() const override {
                assert(valid_);
                return (direction_ == kForward) ? iter_->value() : saved_value_;
            }

            void SkipToNextUserKey(const Slice &userkey) override {
                assert(false);
            }

            Status status() const override {
                if (status_.ok()) {
                    return iter_->status();
                } else {
                    return status_;
                }
            }

            void Next() override;

            void Prev() override;

            void Seek(const Slice &target) override;

            void SeekToFirst() override;

            void SeekToLast() override;

        private:
            void FindNextUserEntry(bool skipping, std::string *skip);

            bool GoToNextEntry(std::string *current_key);

            void FindPrevUserEntry();

            bool ParseKey(ParsedInternalKey *key);

            inline void SaveKey(const Slice &k, std::string *dst) {
                dst->assign(k.data(), k.size());
            }

            inline void ClearSavedValue() {
                if (saved_value_.capacity() > 1048576) {
                    std::string empty;
                    swap(empty, saved_value_);
                } else {
                    saved_value_.clear();
                }
            }

            // Picks the number of bytes that can be read until a compaction is scheduled.
            size_t RandomCompactionPeriod() {
                return rnd_.Uniform(2 * config::kReadBytesPeriod);
            }

            DBImpl *db_;
            const Comparator *const user_comparator_;
            Iterator *const iter_;
            SequenceNumber const sequence_;
            Status status_;
            std::string saved_ikey_;    // == current key when direction_==kReverse
            std::string saved_value_;  // == current raw value when direction_==kReverse
            Direction direction_;
            bool valid_;
            Random rnd_;
            size_t bytes_until_read_sampling_;
            nova::RangePartition range_partition_;
        };

        inline bool DBIter::ParseKey(ParsedInternalKey *ikey) {
            Slice k = iter_->key();

            size_t bytes_read = k.size() + iter_->value().size();
            while (bytes_until_read_sampling_ < bytes_read) {
                bytes_until_read_sampling_ += RandomCompactionPeriod();
            }
            assert(bytes_until_read_sampling_ >= bytes_read);
            bytes_until_read_sampling_ -= bytes_read;

            if (!ParseInternalKey(k, ikey)) {
                status_ = Status::Corruption(
                        "corrupted internal key in DBIter");
                return false;
            } else {
                return true;
            }
        }

        void DBIter::Next() {
            assert(valid_);
            if (direction_ == kReverse) {  // Switch directions?
                direction_ = kForward;
                // iter_ is pointing just before the entries for this->key(),
                // so advance into the range of entries for this->key() and then
                // use the normal skipping code below.
                if (!iter_->Valid()) {
                    iter_->SeekToFirst();
                } else {
                    iter_->Next();
                }
                if (!iter_->Valid()) {
                    valid_ = false;
                    saved_ikey_.clear();
                    return;
                }
                // saved_key_ already contains the key to skip past.
            } else {
                // Store in saved_key_ the current key so we skip it below.
                // The current key is the last key in this range. There is no need to call next.
                Slice ikey = iter_->key();
                SaveKey(ikey, &saved_ikey_);
                uint64_t key = 0;
                Slice ukey = ExtractUserKey(ikey);
                nova::str_to_int(ukey.data(), &key, ukey.size());
                if (key == range_partition_.key_end - 1) {
//                    NOVA_LOG(rdmaio::INFO)
//                        << fmt::format("Stop iterating since reaching the end of range partition {}:{}:{}",
//                                       ukey.ToString(), range_partition_.key_start, range_partition_.key_end);
                    valid_ = false;
                    saved_ikey_.clear();
                    return;
                }
                // iter_ is pointing to current key. We can now safely move to the next to
                // avoid checking current key.
                if (!GoToNextEntry(&saved_ikey_) && iter_->Valid()) {
                    iter_->SkipToNextUserKey(saved_ikey_);
                }
                if (!iter_->Valid()) {
                    valid_ = false;
                    saved_ikey_.clear();
                    return;
                }
            }
        }

        bool DBIter::GoToNextEntry(std::string *current_key) {
            // Loop until we hit an acceptable entry to yield
            assert(iter_->Valid());
            assert(direction_ == kForward);
            // Go to the next entry.
            iter_->Next();
            if (iter_->Valid()) {
                ParsedInternalKey ikey;
                if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
                    switch (ikey.type) {
                        case kTypeValue:
                            if (user_comparator_->Compare(ikey.user_key, ExtractUserKey(*current_key)) <= 0) {
                                // Entry hidden
                            } else {
                                // The next unique key. DONE. :)
                                valid_ = true;
                                saved_ikey_.clear();
                                return true;
                            }
                            break;
                    }
                }
            } else {
                saved_ikey_.clear();
                valid_ = false;
            }
            return false;
        }

        void DBIter::FindNextUserEntry(bool skipping, std::string *skip) {
            // Loop until we hit an acceptable entry to yield
            assert(iter_->Valid());
            assert(direction_ == kForward);
            do {
                ParsedInternalKey ikey;
                if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
                    switch (ikey.type) {
                        case kTypeDeletion:
                            // Arrange to skip all upcoming entries for this key since
                            // they are hidden by this deletion.
                            SaveKey(ikey.user_key, skip);
                            skipping = true;
                            break;
                        case kTypeValue:
                            if (skipping && user_comparator_->Compare(ikey.user_key, ExtractUserKey(*skip)) <= 0) {
                                // Entry hidden
                            } else {
                                valid_ = true;
                                saved_ikey_.clear();
                                return;
                            }
                            break;
                    }
                }
                iter_->Next();
            } while (iter_->Valid());
            saved_ikey_.clear();
            valid_ = false;
        }

        void DBIter::Prev() {
            assert(valid_);

            if (direction_ == kForward) {  // Switch directions?
                // iter_ is pointing at the current entry.  Scan backwards until
                // the key changes so we can use the normal reverse scanning code.
                assert(iter_->Valid());  // Otherwise valid_ would have been false
                SaveKey(iter_->key(), &saved_ikey_);
                while (true) {
                    iter_->Prev();
                    if (!iter_->Valid()) {
                        valid_ = false;
                        saved_ikey_.clear();
                        ClearSavedValue();
                        return;
                    }
                    if (user_comparator_->Compare(ExtractUserKey(iter_->key()), ExtractUserKey(saved_ikey_)) < 0) {
                        break;
                    }
                }
                direction_ = kReverse;
            }

            FindPrevUserEntry();
        }

        void DBIter::FindPrevUserEntry() {
            assert(direction_ == kReverse);

            ValueType value_type = kTypeDeletion;
            if (iter_->Valid()) {
                do {
                    ParsedInternalKey ikey;
                    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
                        if ((value_type != kTypeDeletion) &&
                            user_comparator_->Compare(ikey.user_key, ExtractUserKey(saved_ikey_)) < 0) {
                            // We encountered a non-deleted value in entries for previous keys,
                            break;
                        }
                        value_type = ikey.type;
                        if (value_type == kTypeDeletion) {
                            saved_ikey_.clear();
                            ClearSavedValue();
                        } else {
                            Slice raw_value = iter_->value();
                            if (saved_value_.capacity() >
                                raw_value.size() + 1048576) {
                                std::string empty;
                                swap(empty, saved_value_);
                            }
                            SaveKey(iter_->key(), &saved_ikey_);
                            saved_value_.assign(raw_value.data(), raw_value.size());
                        }
                    }
                    iter_->Prev();
                } while (iter_->Valid());
            }

            if (value_type == kTypeDeletion) {
                // End
                valid_ = false;
                saved_ikey_.clear();
                ClearSavedValue();
                direction_ = kForward;
            } else {
                valid_ = true;
            }
        }

        void DBIter::Seek(const Slice &target) {
            direction_ = kForward;
            ClearSavedValue();
            saved_ikey_.clear();
            AppendInternalKey(&saved_ikey_, ParsedInternalKey(target, sequence_, kValueTypeForSeek));
            iter_->Seek(saved_ikey_);
            if (iter_->Valid()) {
                FindNextUserEntry(false, &saved_ikey_ /* temporary storage */);
            } else {
                valid_ = false;
            }
        }

        void DBIter::SeekToFirst() {
            direction_ = kForward;
            ClearSavedValue();
            iter_->SeekToFirst();
            if (iter_->Valid()) {
                FindNextUserEntry(false, &saved_ikey_ /* temporary storage */);
            } else {
                valid_ = false;
            }
        }

        void DBIter::SeekToLast() {
            direction_ = kReverse;
            ClearSavedValue();
            iter_->SeekToLast();
            FindPrevUserEntry();
        }

    }  // anonymous namespace

    Iterator *
    NewDBIterator(DBImpl *db, const Comparator *user_key_comparator, Iterator *internal_iter, SequenceNumber sequence,
                  uint32_t seed, const nova::RangePartition &range_partition) {
        return new DBIter(db, user_key_comparator, internal_iter, sequence, seed, range_partition);
    }

}  // namespace leveldb
