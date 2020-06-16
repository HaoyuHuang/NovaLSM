
//
// Created by Haoyu Huang on 2/29/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_MEMTABLE_WORKER_H
#define LEVELDB_MEMTABLE_WORKER_H

#include "common/nova_common.h"
#include "db/memtable.h"

#include <queue>

namespace leveldb {

    class MemTableBenchWrapper {
    public:
        virtual void Add(SequenceNumber seq, ValueType type, const Slice &key,
                         const Slice &value) = 0;
    };

    class PartitionedMemTableBench : public MemTableBenchWrapper {
    public:
        PartitionedMemTableBench(uint32_t partition, uint64_t memtable_size);

        void Add(SequenceNumber seq, ValueType type, const Slice &key,
                 const Slice &value) override;

    private:
        std::vector<leveldb::MemTable *> active_memtables_;
        std::vector<std::mutex*> mutexs_;
        uint64_t memtable_size_;
    };

    class MemTableWorker {
    public:
        MemTableWorker(uint32_t thread_id, MemTableBenchWrapper *memtable,
                       uint64_t max_ops, uint32_t nkeys,
                       uint32_t value_size, uint64_t memtable_size);

        void Start();

        double throughput_ = 0;

    private:
        MemTableBenchWrapper *memtable_;
        uint64_t thread_id_;
        uint64_t max_ops_;
        uint32_t nkeys_;
        uint32_t value_size_;
        uint64_t memtable_size_;
    };
}


#endif //LEVELDB_MEMTABLE_WORKER_H
