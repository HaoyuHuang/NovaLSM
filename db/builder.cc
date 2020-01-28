// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/env_mem.h"
#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "cc/nova_cc.h"

#include <fmt/core.h>

namespace leveldb {

    Status
    BuildTable(const std::string &dbname, Env *env, const Options &options,
               TableCache *table_cache, Iterator *iter, FileMetaData *meta) {
        Status s;
        meta->file_size = 0;
        iter->SeekToFirst();
        std::string fname = TableFileName(dbname, meta->number);
        if (iter->Valid()) {
            MemManager *mem_manager = options.bg_thread->mem_manager();
            uint64_t key = options.bg_thread->thread_id();
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("CompactMemTable tid:{} alloc_size:{}",
                               key, options.max_dc_file_size);
            NovaCCMemFile *cc_file = new NovaCCMemFile(env,
                                                       meta->number,
                                                       mem_manager,
                                                       options.sstable_manager,
                                                       options.bg_thread->dc_client(),
                                                       dbname,
                                                       options.bg_thread->thread_id(),
                                                       options.max_dc_file_size);
            WritableFile *file = new MemWritableFile(cc_file);
            TableBuilder *builder = new TableBuilder(options, file);
            meta->smallest.DecodeFrom(iter->key());
            for (; iter->Valid(); iter->Next()) {
                Slice key = iter->key();
                meta->largest.DecodeFrom(key);
                builder->Add(key, iter->value());
            }

            // Finish and check for builder errors
            s = builder->Finish();
            if (s.ok()) {
                meta->file_size = builder->FileSize();
                assert(meta->file_size > 0);
            }
            delete builder;

            cc_file->set_meta(*meta);
            // Finish and check for file errors
            if (s.ok()) {
                s = file->Sync();
            }
            if (s.ok()) {
                s = file->Close();
            }

            // Wait for writes to complete.
            cc_file->WaitForWRITEs();

            delete cc_file;
            cc_file = nullptr;
            delete file;
            file = nullptr;

            if (cc_file->backing_mem()) {
                options.sstable_manager->AddSSTable(dbname,
                                                    cc_file->file_number(),
                                                    cc_file->thread_id(),
                                                    (char *) cc_file->backing_mem(),
                                                    cc_file->used_size(),
                                                    cc_file->allocated_size(),
                        /*async_flush=*/true);
            }

            if (s.ok()) {
                // Verify that the table is usable
//                Iterator *it = table_cache->NewIterator(
//                        AccessCaller::kCompaction, ReadOptions(),
//                        meta->number,
//                        0,
//                        meta->file_size);
//                s = it->status();
//                delete it;
            }
        }

        // Check for input iterator errors
        if (!iter->status().ok()) {
            s = iter->status();
        }

        if (s.ok() && meta->file_size > 0) {
            // Keep it
        } else {
            env->DeleteFile(fname);
        }
        return s;
    }

}  // namespace leveldb
