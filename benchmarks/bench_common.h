
//
// Created by Haoyu Huang on 2/12/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_BENCH_COMMON_H
#define LEVELDB_BENCH_COMMON_H

#include <gflags/gflags.h>
#include "common/nova_common.h"

enum BenchRequestType : char {
    BENCH_ALLOCATE = 'a',
    BENCH_ALLOCATE_RESPONSE = 'b',
    BENCH_PERSIST = 'c',
    BENCH_PERSIST_RESPONSE = 'd'
};

extern std::vector<nova::Host> servers;

#endif //LEVELDB_BENCH_COMMON_H
