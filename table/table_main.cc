
//
// Created by Haoyu Huang on 1/16/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//


#include "leveldb/table.h"

#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <thread>
#include <assert.h>
#include <csignal>
#include <gflags/gflags.h>
#include <util/env_posix.h>
#include "db/filename.h"
#include "common/nova_console_logging.h"
#include <fmt/core.h>
#include "db/dbformat.h"

using namespace std;

DEFINE_string(dbname, "", "");
DEFINE_uint32(fn, 0, "");
DEFINE_uint32(size, 0, "");

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    std::vector<gflags::CommandLineFlagInfo> flags;
    gflags::GetAllFlags(&flags);
    for (const auto &flag : flags) {
        printf("%s=%s\n", flag.name.c_str(),
               flag.current_value.c_str());
    }

    leveldb::Options opt;
    leveldb::EnvOptions env_option;
    env_option.sstable_mode = leveldb::NovaSSTableMode::SSTABLE_DISK;
    leveldb::PosixEnv *env = new leveldb::PosixEnv;
    env->set_env_option(env_option);
    opt.env = env;
    leveldb::RandomAccessFile *r;
    env->NewRandomAccessFile(leveldb::TableFileName(FLAGS_dbname, FLAGS_fn),
                             &r);
    leveldb::Table *table;
    leveldb::Status s = leveldb::Table::Open(opt, leveldb::ReadOptions(), r, FLAGS_size, 0, FLAGS_fn,
                                             &table, nullptr);

    leveldb::Iterator *it = table->NewIterator(
            leveldb::AccessCaller::kUserIterator,
            leveldb::ReadOptions());
    it->SeekToFirst();
    while (it->Valid()) {
        leveldb::ParsedInternalKey result;
        leveldb::ParseInternalKey(it->key(), &result);
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("key:{} value:{}", result.DebugString(),
                           it->value().size());
        it->Next();
    }

    RDMA_ASSERT(s.ok()) << s.ToString();
}