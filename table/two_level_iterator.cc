// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "merger.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"
#include "db/dbformat.h"
#include "common/nova_common.h"

namespace leveldb {

    namespace {

        typedef Iterator *(*BlockFunction)(void *,
                                           void *arg2,
                                           BlockReadContext context,
                                           const ReadOptions &,
                                           const Slice &,
                                           std::string *);

        // TODO: Support merging. When you call next, the data_iter will merge the current with the new iter. The newly merged iterator will then seek to the new key.
        //  Support forwarding only if no_reset is true.
        class TwoLevelIterator : public Iterator {
        public:
            TwoLevelIterator(
                    Iterator *index_iter,
                    BlockReadContext context,
                    BlockFunction block_function,
                    void *arg, void *arg2, const ReadOptions &options,
                    const Comparator *comparator,
                    bool merging);

            ~TwoLevelIterator() override;

            void Seek(const Slice &target) override;

            void SeekToFirst() override;

            void SeekToLast() override;

            void Next() override;

            void Prev() override;

            void SkipToNextUserKey(const Slice &target) override;

            bool Valid() const override { return data_iter_.Valid(); }

            Slice key() const override {
                assert(Valid());
                return data_iter_.key();
            }

            Slice value() const override {
                assert(Valid());
                return data_iter_.value();
            }

            Status status() const override {
                // It'd be nice if status() returned a const Status& instead of a Status
                if (!index_iter_.status().ok()) {
                    return index_iter_.status();
                } else if (data_iter_.iter() != nullptr &&
                           !data_iter_.status().ok()) {
                    return data_iter_.status();
                } else {
                    return status_;
                }
            }

        private:
            void SaveError(const Status &s) {
                if (status_.ok() && !s.ok()) status_ = s;
            }

            void MergingSeek(const Slice &target);

            void MergingNext();

            void MergingDataBlocksForward();

            void SkipEmptyDataBlocksForward();

            void SkipEmptyDataBlocksBackward();

            void SetDataIterator(Iterator *data_iter);

            bool InitDataBlock(std::string *);

            const BlockReadContext context_;
            BlockFunction block_function_;
            void *arg_;
            void *arg2_;
            const ReadOptions options_;
            Status status_;
            IteratorWrapper index_iter_;
            IteratorWrapper data_iter_;  // May be nullptr
            // If data_iter_ is non-null, then "data_block_handle_" holds the
            // "index_value" passed to block_function_ to create the data_iter_.
            std::string data_block_handle_;
            const bool merging_;
            const Comparator *comparator_;
        };

        TwoLevelIterator::TwoLevelIterator(Iterator *index_iter,
                                           BlockReadContext context,
                                           BlockFunction block_function,
                                           void *arg,
                                           void *arg2,
                                           const ReadOptions &options,
                                           const Comparator *comparator,
                                           bool merging)
                : comparator_(comparator),
                  block_function_(block_function),
                  arg_(arg),
                  arg2_(arg2),
                  context_(context),
                  options_(options),
                  index_iter_(index_iter),
                  data_iter_(nullptr), merging_(merging) {}

        TwoLevelIterator::~TwoLevelIterator() = default;

        void TwoLevelIterator::Seek(const Slice &target) {
            if (merging_) {
                MergingSeek(target);
                return;
            }

            index_iter_.Seek(target);
            InitDataBlock(nullptr);
            if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
            SkipEmptyDataBlocksForward();
        }

        void TwoLevelIterator::SkipToNextUserKey(const Slice &target) {

            NOVA_LOG(rdmaio::DEBUG) << fmt::format("Two level skip index {}",
                                                   ExtractUserKey(
                                                           target).ToString());
            index_iter_.SkipToNextUserKey(target);
            bool new_data_block = InitDataBlock(nullptr);
            NOVA_LOG(rdmaio::DEBUG) << fmt::format("Two level skip data {}",
                                                   ExtractUserKey(
                                                           target).ToString());
            if (data_iter_.iter() != nullptr)
                data_iter_.SkipToNextUserKey(target);
            if (merging_) {
                MergingDataBlocksForward();
            } else {
                SkipEmptyDataBlocksForward();
            }
        }

        void TwoLevelIterator::SeekToFirst() {
            NOVA_ASSERT(!merging_);
            index_iter_.SeekToFirst();
            InitDataBlock(nullptr);
            if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
            SkipEmptyDataBlocksForward();
        }

        void TwoLevelIterator::SeekToLast() {
            NOVA_ASSERT(!merging_);
            index_iter_.SeekToLast();
            InitDataBlock(nullptr);
            if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
            SkipEmptyDataBlocksBackward();
        }

        void TwoLevelIterator::Next() {
            assert(Valid());
            if (merging_) {
                MergingNext();
                return;
            }
            data_iter_.Next();
            SkipEmptyDataBlocksForward();
        }

        void TwoLevelIterator::Prev() {
            NOVA_ASSERT(!merging_);
            assert(Valid());
            data_iter_.Prev();
            SkipEmptyDataBlocksBackward();
        }

        void TwoLevelIterator::SkipEmptyDataBlocksForward() {
            NOVA_ASSERT(!merging_);
            while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
                // Move to next block
                if (!index_iter_.Valid()) {
                    SetDataIterator(nullptr);
                    return;
                }
                index_iter_.Next();
                InitDataBlock(nullptr);
                if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
            }
        }

        void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
            NOVA_ASSERT(!merging_);
            while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
                // Move to next block
                if (!index_iter_.Valid()) {
                    SetDataIterator(nullptr);
                    return;
                }
                index_iter_.Prev();
                InitDataBlock(nullptr);
                if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
            }
        }

        void TwoLevelIterator::SetDataIterator(Iterator *data_iter) {
            if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
            data_iter_.Set(data_iter);
        }

        bool TwoLevelIterator::InitDataBlock(std::string *next_key) {
            if (!index_iter_.Valid()) {
                SetDataIterator(nullptr);
                return true;
            } else {
                Slice handle = index_iter_.value();
                if (data_iter_.iter() != nullptr &&
                    handle.compare(data_block_handle_) == 0) {
                    // data_iter_ is already constructed with this iterator, so
                    // no need to change anything
                    return false;
                } else {
                    Iterator *iter = (*block_function_)(arg_, arg2_, context_,
                                                        options_, handle,
                                                        next_key);
                    data_block_handle_.assign(handle.data(), handle.size());
                    SetDataIterator(iter);
                    return true;
                }
            }
        }

        void TwoLevelIterator::MergingDataBlocksForward() {
            while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
                // Move to next block
                std::string next_key;
                if (!index_iter_.Valid()) {
                    SetDataIterator(nullptr);
                    return;
                }
                index_iter_.Next();
                InitDataBlock(nullptr);
            }
        }

        void TwoLevelIterator::MergingSeek(const Slice &target) {
            std::string next_key;
            index_iter_.Seek(target);
            InitDataBlock(&next_key);
            if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
            MergingDataBlocksForward();
        }

        void TwoLevelIterator::MergingNext() {
            data_iter_.Next();
            MergingDataBlocksForward();
        }

    }  // namespace

    Iterator *
    NewTwoLevelIterator(Iterator *index_iter,
                        BlockReadContext context,
                        BlockFunction block_function, void *arg, void *arg2,
                        const ReadOptions &options,
                        const Comparator *comparator, bool merging) {
        return new TwoLevelIterator(index_iter, context,
                                    block_function,
                                    arg, arg2, options, comparator, merging);
    }

}  // namespace leveldb
