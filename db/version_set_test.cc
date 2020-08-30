// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "ltc/storage_selector.h"


#include <stdlib.h>
#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <thread>
#include <assert.h>
#include <csignal>
#include <gflags/gflags.h>

namespace leveldb {

    class YCSBKeyComparator : public leveldb::Comparator {
    public:
        int
        Compare(const leveldb::Slice &a, const leveldb::Slice &b) const {
            uint64_t ai = 0;
            nova::str_to_int(a.data(), &ai, a.size());
            uint64_t bi = 0;
            nova::str_to_int(b.data(), &bi, b.size());

            if (ai < bi) {
                return -1;
            } else if (ai > bi) {
                return 1;
            }
            return 0;
        }

        // Ignore the following methods for now:
        const char *Name() const { return "YCSBKeyComparator"; }

        void
        FindShortestSeparator(std::string *,
                              const leveldb::Slice &) const {}

        void FindShortSuccessor(std::string *) const {}
    };

//    class FindFileTest {
//    public:
//        FindFileTest() : disjoint_sorted_files_(true) {}
//
//        ~FindFileTest() {
//            for (int i = 0; i < files_.size(); i++) {
//                delete files_[i];
//            }
//        }
//
//        void Add(const char *smallest, const char *largest,
//                 SequenceNumber smallest_seq = 100,
//                 SequenceNumber largest_seq = 100) {
//            FileMetaData *f = new FileMetaData;
//            f->number = files_.size() + 1;
//            f->smallest = InternalKey(smallest, smallest_seq, kTypeValue);
//            f->largest = InternalKey(largest, largest_seq, kTypeValue);
//            files_.push_back(f);
//        }
//
//        int Find(const char *key) {
//            InternalKey target(key, 100, kTypeValue);
//            InternalKeyComparator cmp(BytewiseComparator());
//            return FindFile(cmp, files_, target.Encode());
//        }
//
//        bool Overlaps(const char *smallest, const char *largest) {
//            InternalKeyComparator cmp(BytewiseComparator());
//            Slice s(smallest != nullptr ? smallest : "");
//            Slice l(largest != nullptr ? largest : "");
//            return SomeFileOverlapsRange(cmp, disjoint_sorted_files_, files_,
//                                         (smallest != nullptr ? &s : nullptr),
//                                         (largest != nullptr ? &l : nullptr));
//        }
//
//        bool disjoint_sorted_files_;
//
//    private:
//        std::vector<FileMetaData *> files_;
//    };
//
//    TEST(FindFileTest, Empty) {
//        ASSERT_EQ(0, Find("foo"));
//        ASSERT_TRUE(!Overlaps("a", "z"));
//        ASSERT_TRUE(!Overlaps(nullptr, "z"));
//        ASSERT_TRUE(!Overlaps("a", nullptr));
//        ASSERT_TRUE(!Overlaps(nullptr, nullptr));
//    }
//
//    TEST(FindFileTest, Single) {
//        Add("p", "q");
//        ASSERT_EQ(0, Find("a"));
//        ASSERT_EQ(0, Find("p"));
//        ASSERT_EQ(0, Find("p1"));
//        ASSERT_EQ(0, Find("q"));
//        ASSERT_EQ(1, Find("q1"));
//        ASSERT_EQ(1, Find("z"));
//
//        ASSERT_TRUE(!Overlaps("a", "b"));
//        ASSERT_TRUE(!Overlaps("z1", "z2"));
//        ASSERT_TRUE(Overlaps("a", "p"));
//        ASSERT_TRUE(Overlaps("a", "q"));
//        ASSERT_TRUE(Overlaps("a", "z"));
//        ASSERT_TRUE(Overlaps("p", "p1"));
//        ASSERT_TRUE(Overlaps("p", "q"));
//        ASSERT_TRUE(Overlaps("p", "z"));
//        ASSERT_TRUE(Overlaps("p1", "p2"));
//        ASSERT_TRUE(Overlaps("p1", "z"));
//        ASSERT_TRUE(Overlaps("q", "q"));
//        ASSERT_TRUE(Overlaps("q", "q1"));
//
//        ASSERT_TRUE(!Overlaps(nullptr, "j"));
//        ASSERT_TRUE(!Overlaps("r", nullptr));
//        ASSERT_TRUE(Overlaps(nullptr, "p"));
//        ASSERT_TRUE(Overlaps(nullptr, "p1"));
//        ASSERT_TRUE(Overlaps("q", nullptr));
//        ASSERT_TRUE(Overlaps(nullptr, nullptr));
//    }
//
//    TEST(FindFileTest, Multiple) {
//        Add("150", "200");
//        Add("200", "250");
//        Add("300", "350");
//        Add("400", "450");
//        ASSERT_EQ(0, Find("100"));
//        ASSERT_EQ(0, Find("150"));
//        ASSERT_EQ(0, Find("151"));
//        ASSERT_EQ(0, Find("199"));
//        ASSERT_EQ(0, Find("200"));
//        ASSERT_EQ(1, Find("201"));
//        ASSERT_EQ(1, Find("249"));
//        ASSERT_EQ(1, Find("250"));
//        ASSERT_EQ(2, Find("251"));
//        ASSERT_EQ(2, Find("299"));
//        ASSERT_EQ(2, Find("300"));
//        ASSERT_EQ(2, Find("349"));
//        ASSERT_EQ(2, Find("350"));
//        ASSERT_EQ(3, Find("351"));
//        ASSERT_EQ(3, Find("400"));
//        ASSERT_EQ(3, Find("450"));
//        ASSERT_EQ(4, Find("451"));
//
//        ASSERT_TRUE(!Overlaps("100", "149"));
//        ASSERT_TRUE(!Overlaps("251", "299"));
//        ASSERT_TRUE(!Overlaps("451", "500"));
//        ASSERT_TRUE(!Overlaps("351", "399"));
//
//        ASSERT_TRUE(Overlaps("100", "150"));
//        ASSERT_TRUE(Overlaps("100", "200"));
//        ASSERT_TRUE(Overlaps("100", "300"));
//        ASSERT_TRUE(Overlaps("100", "400"));
//        ASSERT_TRUE(Overlaps("100", "500"));
//        ASSERT_TRUE(Overlaps("375", "400"));
//        ASSERT_TRUE(Overlaps("450", "450"));
//        ASSERT_TRUE(Overlaps("450", "500"));
//    }
//
//    TEST(FindFileTest, MultipleNullBoundaries) {
//        Add("150", "200");
//        Add("200", "250");
//        Add("300", "350");
//        Add("400", "450");
//        ASSERT_TRUE(!Overlaps(nullptr, "149"));
//        ASSERT_TRUE(!Overlaps("451", nullptr));
//        ASSERT_TRUE(Overlaps(nullptr, nullptr));
//        ASSERT_TRUE(Overlaps(nullptr, "150"));
//        ASSERT_TRUE(Overlaps(nullptr, "199"));
//        ASSERT_TRUE(Overlaps(nullptr, "200"));
//        ASSERT_TRUE(Overlaps(nullptr, "201"));
//        ASSERT_TRUE(Overlaps(nullptr, "400"));
//        ASSERT_TRUE(Overlaps(nullptr, "800"));
//        ASSERT_TRUE(Overlaps("100", nullptr));
//        ASSERT_TRUE(Overlaps("200", nullptr));
//        ASSERT_TRUE(Overlaps("449", nullptr));
//        ASSERT_TRUE(Overlaps("450", nullptr));
//    }
//
//    TEST(FindFileTest, OverlapSequenceChecks) {
//        Add("200", "200", 5000, 3000);
//        ASSERT_TRUE(!Overlaps("199", "199"));
//        ASSERT_TRUE(!Overlaps("201", "300"));
//        ASSERT_TRUE(Overlaps("200", "200"));
//        ASSERT_TRUE(Overlaps("190", "200"));
//        ASSERT_TRUE(Overlaps("200", "210"));
//    }
//
//    TEST(FindFileTest, OverlappingFiles) {
//        Add("150", "600");
//        Add("400", "500");
//        disjoint_sorted_files_ = false;
//        ASSERT_TRUE(!Overlaps("100", "149"));
//        ASSERT_TRUE(!Overlaps("601", "700"));
//        ASSERT_TRUE(Overlaps("100", "150"));
//        ASSERT_TRUE(Overlaps("100", "200"));
//        ASSERT_TRUE(Overlaps("100", "300"));
//        ASSERT_TRUE(Overlaps("100", "400"));
//        ASSERT_TRUE(Overlaps("100", "500"));
//        ASSERT_TRUE(Overlaps("375", "400"));
//        ASSERT_TRUE(Overlaps("450", "450"));
//        ASSERT_TRUE(Overlaps("450", "500"));
//        ASSERT_TRUE(Overlaps("450", "700"));
//        ASSERT_TRUE(Overlaps("600", "700"));
//    }
//
//    void AddBoundaryInputs(const InternalKeyComparator &icmp,
//                           const std::vector<FileMetaData *> &level_files,
//                           std::vector<FileMetaData *> *compaction_files);
//
//    class AddBoundaryInputsTest {
//    public:
//        std::vector<FileMetaData *> level_files_;
//        std::vector<FileMetaData *> compaction_files_;
//        std::vector<FileMetaData *> all_files_;
//        InternalKeyComparator icmp_;
//
//        AddBoundaryInputsTest() : icmp_(BytewiseComparator()) {}
//
//        ~AddBoundaryInputsTest() {
//            for (size_t i = 0; i < all_files_.size(); ++i) {
//                delete all_files_[i];
//            }
//            all_files_.clear();
//        }
//
//        FileMetaData *CreateFileMetaData(uint64_t number, InternalKey smallest,
//                                         InternalKey largest) {
//            FileMetaData *f = new FileMetaData();
//            f->number = number;
//            f->smallest = smallest;
//            f->largest = largest;
//            all_files_.push_back(f);
//            return f;
//        }
//    };
//
//    TEST(AddBoundaryInputsTest, TestEmptyFileSets) {
//        AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
//        ASSERT_TRUE(compaction_files_.empty());
//        ASSERT_TRUE(level_files_.empty());
//    }
//
//    TEST(AddBoundaryInputsTest, TestEmptyLevelFiles) {
//        FileMetaData *f1 =
//                CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("100", 1, kTypeValue)));
//        compaction_files_.push_back(f1);
//
//        AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
//        ASSERT_EQ(1, compaction_files_.size());
//        ASSERT_EQ(f1, compaction_files_[0]);
//        ASSERT_TRUE(level_files_.empty());
//    }
//
//    TEST(AddBoundaryInputsTest, TestEmptyCompactionFiles) {
//        FileMetaData *f1 =
//                CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("100", 1, kTypeValue)));
//        level_files_.push_back(f1);
//
//        AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
//        ASSERT_TRUE(compaction_files_.empty());
//        ASSERT_EQ(1, level_files_.size());
//        ASSERT_EQ(f1, level_files_[0]);
//    }
//
//    TEST(AddBoundaryInputsTest, TestNoBoundaryFiles) {
//        FileMetaData *f1 =
//                CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("100", 1, kTypeValue)));
//        FileMetaData *f2 =
//                CreateFileMetaData(1, InternalKey("200", 2, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("200", 1, kTypeValue)));
//        FileMetaData *f3 =
//                CreateFileMetaData(1, InternalKey("300", 2, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("300", 1, kTypeValue)));
//
//        level_files_.push_back(f3);
//        level_files_.push_back(f2);
//        level_files_.push_back(f1);
//        compaction_files_.push_back(f2);
//        compaction_files_.push_back(f3);
//
//        AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
//        ASSERT_EQ(2, compaction_files_.size());
//    }
//
//    TEST(AddBoundaryInputsTest, TestOneBoundaryFiles) {
//        FileMetaData *f1 =
//                CreateFileMetaData(1, InternalKey("100", 3, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("100", 2, kTypeValue)));
//        FileMetaData *f2 =
//                CreateFileMetaData(1, InternalKey("100", 1, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("200", 3, kTypeValue)));
//        FileMetaData *f3 =
//                CreateFileMetaData(1, InternalKey("300", 2, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("300", 1, kTypeValue)));
//
//        level_files_.push_back(f3);
//        level_files_.push_back(f2);
//        level_files_.push_back(f1);
//        compaction_files_.push_back(f1);
//
//        AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
//        ASSERT_EQ(2, compaction_files_.size());
//        ASSERT_EQ(f1, compaction_files_[0]);
//        ASSERT_EQ(f2, compaction_files_[1]);
//    }
//
//    TEST(AddBoundaryInputsTest, TestTwoBoundaryFiles) {
//        FileMetaData *f1 =
//                CreateFileMetaData(1, InternalKey("100", 6, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("100", 5, kTypeValue)));
//        FileMetaData *f2 =
//                CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("300", 1, kTypeValue)));
//        FileMetaData *f3 =
//                CreateFileMetaData(1, InternalKey("100", 4, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("100", 3, kTypeValue)));
//
//        level_files_.push_back(f2);
//        level_files_.push_back(f3);
//        level_files_.push_back(f1);
//        compaction_files_.push_back(f1);
//
//        AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
//        ASSERT_EQ(3, compaction_files_.size());
//        ASSERT_EQ(f1, compaction_files_[0]);
//        ASSERT_EQ(f3, compaction_files_[1]);
//        ASSERT_EQ(f2, compaction_files_[2]);
//    }
//
//    TEST(AddBoundaryInputsTest, TestDisjoinFilePointers) {
//        FileMetaData *f1 =
//                CreateFileMetaData(1, InternalKey("100", 6, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("100", 5, kTypeValue)));
//        FileMetaData *f2 =
//                CreateFileMetaData(1, InternalKey("100", 6, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("100", 5, kTypeValue)));
//        FileMetaData *f3 =
//                CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("300", 1, kTypeValue)));
//        FileMetaData *f4 =
//                CreateFileMetaData(1, InternalKey("100", 4, kTypeValue),
//                                   InternalKey(
//                                           InternalKey("100", 3, kTypeValue)));
//
//        level_files_.push_back(f2);
//        level_files_.push_back(f3);
//        level_files_.push_back(f4);
//
//        compaction_files_.push_back(f1);
//
//        AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
//        ASSERT_EQ(3, compaction_files_.size());
//        ASSERT_EQ(f1, compaction_files_[0]);
//        ASSERT_EQ(f4, compaction_files_[1]);
//        ASSERT_EQ(f3, compaction_files_[2]);
//    }

    class VersionTest {
    public:
        void
        CreateFileMetaData(Version *version, int level,
                           int smallest, int largest) {
            FileMetaData *f = new FileMetaData();
            f->number = fn++;
            auto s = new std::string(std::to_string(smallest));
            auto l = new std::string(std::to_string(largest));
            f->smallest = InternalKey(*s, seq_id++, ValueType::kTypeValue);
            f->largest = InternalKey(*l, seq_id++, ValueType::kTypeValue);;
            version->files_[level].push_back(f);
            version->fn_files_[f->number] = f;
        }

        void PrintCompactions(const Comparator *user_comparator,
                              std::vector<Compaction *> &compactions) {
            std::string debug;
            for (int i = 0; i < compactions.size(); i++) {
                auto c = compactions[i];
                debug += fmt::format("\nset{}:{}", i,
                                     c->DebugString(user_comparator));
            }

            NOVA_LOG(rdmaio::INFO) << debug;
        }

        uint32_t fn = 0;
        uint32_t seq_id = 0;
    };

//    TEST(VersionTest, TestNonOverlappingSetWideSSTable) {
//        InternalKeyComparator icmp(new YCSBKeyComparator);
//        Options options;
//        options.max_num_coordinated_compaction_nonoverlapping_sets = 3;
//        options.max_num_sstables_in_nonoverlapping_set = 4;
//        Version *version = new Version(&icmp, nullptr, &options, 1);
//        CreateFileMetaData(version, 0, 0, 100);
//
//        CreateFileMetaData(version, 0, 6, 10);
//        CreateFileMetaData(version, 0, 5, 12);
//        CreateFileMetaData(version, 0, 11, 19);
//
//        CreateFileMetaData(version, 0, 20, 30);
//
//        CreateFileMetaData(version, 0, 45, 50);
//
//        CreateFileMetaData(version, 1, 0, 6);
//        CreateFileMetaData(version, 1, 6, 12);
//        CreateFileMetaData(version, 1, 12, 15);
//        CreateFileMetaData(version, 1, 25, 35);
//        CreateFileMetaData(version, 1, 43, 47);
//
//        CreateFileMetaData(version, 1, 50, 69);
//        CreateFileMetaData(version, 1, 70, 87);
//        CreateFileMetaData(version, 1, 90, 95);
//        CreateFileMetaData(version, 1, 95, 100);
//
//        std::vector<Compaction *> compactions;
//        version->ComputeNonOverlappingSet(&compactions);
//        PrintCompactions(icmp.user_comparator(), compactions);
//        std::string reason;
//        ASSERT_TRUE(version->AssertNonOverlappingSet(compactions, &reason));
//    }
//
//    TEST(VersionTest, TestNonOverlappingSet) {
//        InternalKeyComparator icmp(new YCSBKeyComparator);
//        Options options;
//        options.max_num_coordinated_compaction_nonoverlapping_sets = 3;
//        options.max_num_sstables_in_nonoverlapping_set = 4;
//        Version *version = new Version(&icmp, nullptr, &options, 1);
//        CreateFileMetaData(version, 0, 6, 10);
//        CreateFileMetaData(version, 0, 5, 12);
//        CreateFileMetaData(version, 0, 11, 19);
//
//        CreateFileMetaData(version, 0, 20, 30);
//
//        CreateFileMetaData(version, 0, 45, 50);
//
//        CreateFileMetaData(version, 1, 0, 6);
//        CreateFileMetaData(version, 1, 6, 12);
//        CreateFileMetaData(version, 1, 12, 15);
//        CreateFileMetaData(version, 1, 25, 35);
//        CreateFileMetaData(version, 1, 43, 47);
//
//        CreateFileMetaData(version, 1, 50, 69);
//        CreateFileMetaData(version, 1, 70, 87);
//        CreateFileMetaData(version, 1, 90, 95);
//        CreateFileMetaData(version, 1, 95, 100);
//
//        std::vector<Compaction *> compactions;
//        version->ComputeNonOverlappingSet(&compactions);
//        PrintCompactions(icmp.user_comparator(), compactions);
//        std::string reason;
//        ASSERT_TRUE(version->AssertNonOverlappingSet(compactions,
//                                                     &reason));
//    }
//
//    TEST(VersionTest, TestNonOverlappingSetExpandL0) {
//        InternalKeyComparator icmp(new YCSBKeyComparator);
//        Options options;
//        options.max_num_coordinated_compaction_nonoverlapping_sets = 3;
//        options.max_num_sstables_in_nonoverlapping_set = 4;
//        Version *version = new Version(&icmp, nullptr, &options, 1);
//        CreateFileMetaData(version, 0, 6, 10);
//        CreateFileMetaData(version, 0, 5, 11);
//        CreateFileMetaData(version, 0, 10, 19);
//
//
//        CreateFileMetaData(version, 1, 0, 6);
//        CreateFileMetaData(version, 1, 6, 12);
//        CreateFileMetaData(version, 1, 13, 15);
//
//        std::vector<Compaction *> compactions;
//        version->ComputeNonOverlappingSet(&compactions);
//        PrintCompactions(icmp.user_comparator(), compactions);
//        std::string reason;
//        ASSERT_TRUE(version->AssertNonOverlappingSet(compactions,
//                                                     &reason));
//    }
//
//    TEST(VersionTest, TestNonOverlappingSetDEBUG) {
//        InternalKeyComparator icmp(new YCSBKeyComparator);
//        Options options;
//        options.max_num_coordinated_compaction_nonoverlapping_sets = 1;
//        options.max_num_sstables_in_nonoverlapping_set = 15;
//        Version *version = new Version(&icmp, nullptr, &options, 1);
//        CreateFileMetaData(version, 0, 117163, 264201);
//        CreateFileMetaData(version, 0, 264204, 406865);
//
//        CreateFileMetaData(version, 1, 0, 24620);
//        CreateFileMetaData(version, 1, 24621, 49173);
//        CreateFileMetaData(version, 1, 49174, 73628);
//        CreateFileMetaData(version, 1, 73629, 98178);
//        CreateFileMetaData(version, 1, 98180, 117162);
//
//        CreateFileMetaData(version, 1, 117163, 142359);
//        CreateFileMetaData(version, 1, 142360, 167616);
//        CreateFileMetaData(version, 1, 167619, 192936);
//        CreateFileMetaData(version, 1, 192937, 218347);
//        CreateFileMetaData(version, 1, 218348, 243654);
//        CreateFileMetaData(version, 1, 243655, 264203);
//
//        std::vector<Compaction *> compactions;
//        version->ComputeNonOverlappingSet(&compactions);
//        PrintCompactions(icmp.user_comparator(), compactions);
//        std::string reason;
//        ASSERT_TRUE(version->AssertNonOverlappingSet(compactions, &reason));
//    }

//    TEST(VersionTest, TestNonOverlappingSetDEBUG2) {
//        InternalKeyComparator icmp(new YCSBKeyComparator);
//        Options options;
//        options.max_num_coordinated_compaction_nonoverlapping_sets = 1;
//        options.max_num_sstables_in_nonoverlapping_set = 15;
//        Version *version = new Version(&icmp, nullptr, &options, 1);
//
////        ['2072559,'2775335] fn:110,
////        ['2080888,'2777627] fn:112,
////        ['2556565,'5150437] fn:94,
////        ['2750761,'3453268] fn:111,
////        ['1391249,'2080409] fn:49,
////        ['2080413,'2766187] fn:52,
////        ['2766232,'3457725] fn:54,
//
//        CreateFileMetaData(version, 0, 2072559, 2775335);
//        CreateFileMetaData(version, 0, 2080888, 2777627);
//        CreateFileMetaData(version, 0, 2556565, 5150437);
//        CreateFileMetaData(version, 0, 2750761, 3453268);
//
//        CreateFileMetaData(version, 1, 1391249, 2080409);
//        CreateFileMetaData(version, 1, 2080413, 2766187);
//        CreateFileMetaData(version, 1, 2766232, 3457725);
//        CreateFileMetaData(version, 1, 3457756, 4147918);
//
//        std::vector<Compaction *> compactions;
//        version->ComputeNonOverlappingSet(&compactions);
//        PrintCompactions(icmp.user_comparator(), compactions);
//        std::string reason;
//        ASSERT_TRUE(version->AssertNonOverlappingSet(compactions, &reason));
//    }

    TEST(VersionTest, TestNonOverlappingSetDEBUG3) {
        InternalKeyComparator icmp(new YCSBKeyComparator);
        Options options;
        options.max_num_coordinated_compaction_nonoverlapping_sets = 2;
        options.max_num_sstables_in_nonoverlapping_set = 3;
        Version *version = new Version(&icmp, nullptr, &options, 1, nullptr);

        CreateFileMetaData(version, 0, 0, 8);
        CreateFileMetaData(version, 0, 10, 15);

        CreateFileMetaData(version, 1, 4, 5);
        CreateFileMetaData(version, 1, 5, 9);
        CreateFileMetaData(version, 1, 9, 12);
        CreateFileMetaData(version, 1, 12, 18);

//        CreateFileMetaData(version, 1, 2080413, 2766187);
//        CreateFileMetaData(version, 1, 2766232, 3457725);
//        CreateFileMetaData(version, 1, 3457756, 4147918);

        std::vector<Compaction *> compactions;
        bool delete_due_to_low_overlap;
        version->ComputeNonOverlappingSet(&compactions, &delete_due_to_low_overlap);
        PrintCompactions(icmp.user_comparator(), compactions);
        std::string reason;
        ASSERT_TRUE(version->AssertNonOverlappingSet(compactions, &reason));
    }

}  // namespace leveldb

using namespace std;
using namespace rdmaio;
using namespace nova;

NovaConfig *NovaConfig::config;
std::atomic_int_fast32_t leveldb::EnvBGThread::bg_flush_memtable_thread_id_seq;
std::atomic_int_fast32_t nova::RDMAServerImpl::bg_storage_worker_seq_id_;
std::atomic_int_fast32_t leveldb::StoCBlockClient::rdma_worker_seq_id_;
std::unordered_map<uint64_t, leveldb::FileMetaData *> leveldb::Version::last_fnfile;
nova::NovaGlobalVariables nova::NovaGlobalVariables::global;
std::atomic<nova::Servers *> leveldb::StorageSelector::available_stoc_servers;
std::atomic_int_fast32_t leveldb::StorageSelector::stoc_for_compaction_seq_id;

int main(int argc, char **argv) { return leveldb::test::RunAllTests(); }
