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
               TableCache *table_cache, Iterator *iter, FileMetaData *meta,
               EnvBGThread *bg_thread) {
        // TODO: Prune memtables. Support compacting mulitple memtables.

        Status s;
        meta->file_size = 0;
        iter->SeekToFirst();
        std::string fname = TableFileName(dbname, meta->number);
        if (iter->Valid()) {
            MemManager *mem_manager = bg_thread->mem_manager();
            uint64_t key = bg_thread->thread_id();
            NovaCCMemFile *cc_file = new NovaCCMemFile(env,
                                                       options,
                                                       meta->number,
                                                       mem_manager,
                                                       bg_thread->dc_client(),
                                                       dbname,
                                                       bg_thread->thread_id(),
                                                       options.max_dc_file_size,
                                                       bg_thread->rand_seed());
            WritableFile *file = new MemWritableFile(cc_file);
            TableBuilder *builder = new TableBuilder(options, file);
            meta->smallest.DecodeFrom(iter->key());
            for (; iter->Valid(); iter->Next()) {
                Slice key = iter->key();
                meta->largest.DecodeFrom(key);
                builder->Add(key, iter->value());
            }

            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format(
                        "!!!!!!!!!!!!!!!!!!!!! CompactMemTable tid:{} alloc_size:{} nentries:{} nblocks:{}",
                        key, options.max_dc_file_size, builder->NumEntries(),
                        builder->NumDataBlocks());

            // Finish and check for builder errors
            s = builder->Finish();
            if (s.ok()) {
                meta->file_size = builder->FileSize();
                meta->converted_file_size = meta->file_size;
                assert(meta->file_size > 0);

                cc_file->set_meta(*meta);
                cc_file->set_num_data_blocks(builder->NumDataBlocks());
            }
            delete builder;
            builder = nullptr;

            // Finish and check for file errors
            if (s.ok()) {
                s = file->Sync();
            }
            if (s.ok()) {
                s = file->Close();
            }

            // Make sure WRITEs are complete before we persist them.
            cc_file->WaitForPersistingDataBlocks();
            uint32_t new_file_size = cc_file->Finalize();
            meta->meta_block_handle = cc_file->meta_block_handle();
            meta->data_block_group_handles = cc_file->rhs();
            meta->converted_file_size = new_file_size;

            delete cc_file;
            cc_file = nullptr;
            delete file;
            file = nullptr;
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
