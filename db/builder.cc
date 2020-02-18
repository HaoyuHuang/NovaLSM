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
            NovaCCMemFile *cc_file = new NovaCCMemFile(env,
                                                       options,
                                                       meta->number,
                                                       mem_manager,
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
            meta->data_block_group_handles = cc_file->rhs();
            meta->converted_file_size = new_file_size;

            delete cc_file;
            cc_file = nullptr;
            delete file;
            file = nullptr;

//            if (cc_file->backing_mem()) {
//                options.sstable_manager->AddSSTable(dbname,
//                                                    cc_file->file_number(),
//                                                    cc_file->thread_id(),
//                                                    (char *) cc_file->backing_mem(),
//                                                    cc_file->used_size(),
//                                                    cc_file->allocated_size(),
//                        /*async_flush=*/true);
//            }

            if (s.ok()) {
                // Verify that the table is usable
//                ReadOptions read_options = {};
//                read_options.thread_id = options.bg_thread->thread_id();
//                read_options.dc_client = options.bg_thread->dc_client();
//                read_options.mem_manager = options.bg_thread->mem_manager();
//                Iterator *it = table_cache->NewIterator(
//                        AccessCaller::kCompaction, read_options,
//                        *meta,
//                        meta->number,
//                        0,
//                        meta->converted_file_size);
//                s = it->status();
//                it->SeekToFirst();
//                std::string first_key;
//                leveldb::ParsedInternalKey last_key;
//                while (it->Valid()) {
//                    leveldb::ParseInternalKey(it->key(), &last_key);
//                    if (first_key.empty()) {
//                        first_key = last_key.user_key.ToString();
//                    }
//                    it->Next();
//                }
//                RDMA_LOG(rdmaio::DEBUG)
//                    << fmt::format(
//                            "Verify new SSTable {} original size {} new size {} range {}:{}",
//                            meta->number, meta->file_size,
//                            meta->converted_file_size,
//                            first_key,
//                            last_key.user_key.ToString());
//                RDMA_ASSERT(s.ok());
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
