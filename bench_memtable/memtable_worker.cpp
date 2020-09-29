
//
// Created by Haoyu Huang on 2/29/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "memtable_worker.h"

namespace {
    class YCSBKeyComparator : public leveldb::Comparator {
    public:
        //   if a < b: negative result
        //   if a > b: positive result
        //   else: zero result
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
}

namespace leveldb {
    PartitionedMemTableBench::PartitionedMemTableBench(uint32_t partition,
                                                       uint64_t memtable_size)
            : memtable_size_(memtable_size) {
        for (int i = 0; i < partition; i++) {
            auto cmp = new YCSBKeyComparator();
            leveldb::InternalKeyComparator *comp = new leveldb::InternalKeyComparator(
                    cmp);
            MemTable *table = new MemTable(*comp, 0, nullptr, true);
            table->Ref();
            active_memtables_.push_back(table);
            mutexs_.push_back(new std::mutex);
        }
    }

    void PartitionedMemTableBench::Add(leveldb::SequenceNumber seq,
                                       leveldb::ValueType type,
                                       const leveldb::Slice &key,
                                       const leveldb::Slice &value) {
        uint32_t partition_id = seq % active_memtables_.size();
        mutexs_[partition_id]->lock();
        MemTable *table = active_memtables_[partition_id];
        if (table->ApproximateMemoryUsage() > memtable_size_) {
            table->Unref();
            auto cmp = new YCSBKeyComparator();
            leveldb::InternalKeyComparator *comp = new leveldb::InternalKeyComparator(
                    cmp);
            table = new MemTable(*comp, 0, nullptr, true);
            table->Ref();
            active_memtables_[partition_id] = table;
        }
        table->Add(seq, type, key, value);
        mutexs_[partition_id]->unlock();
    }

    MemTableWorker::MemTableWorker(uint32_t thread_id,
                                   MemTableBenchWrapper *mem_table,
                                   uint64_t max_ops, uint32_t nkeys,
                                   uint32_t value_size, uint64_t memtable_size)
            : thread_id_(thread_id), memtable_(
            mem_table), max_ops_(max_ops), nkeys_(nkeys), value_size_(
            value_size), memtable_size_(memtable_size) {}

    void MemTableWorker::Start() {
        char value[value_size_];
        for (int i = 0; i < value_size_; i++) {
            value[i] = i;
        }
        uint32_t id = 0;
        char key_buf[1024];

        struct ::timeval start_timeval;
        ::gettimeofday(&start_timeval, nullptr);
        int64_t start_unix_time = start_timeval.tv_sec;

        for (uint32_t i = 0; i < max_ops_; i++) {
            id = rand() % nkeys_;
            uint32_t key_size = nova::int_to_str(key_buf, id);

            Slice key(key_buf, key_size);
            Slice val(value, value_size_);

            leveldb::SequenceNumber seq = (thread_id_ << 32) | i;
            memtable_->Add(seq, ValueType::kTypeValue, key, val);
        }

        struct ::timeval end_timeval;
        ::gettimeofday(&end_timeval, nullptr);
        int64_t end_unix_time = end_timeval.tv_sec;
        throughput_ = max_ops_ / (end_unix_time - start_unix_time);
    }
}