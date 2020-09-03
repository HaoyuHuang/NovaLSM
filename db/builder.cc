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
#include "ltc/stoc_file_client_impl.h"

#include <fmt/core.h>

namespace leveldb {

    Status
    TestBuildTable(const std::string &dbname, Env *env, const Options &options,
                   TableCache *table_cache, Iterator *iter, FileMetaData *meta,
                   EnvBGThread *bg_thread) {
        Status s;
        meta->file_size = 0;
        iter->SeekToFirst();
        std::string fname = TableFileName(dbname, meta->number, FileInternalType::kFileData, 0);
        if (iter->Valid()) {
            MemManager *mem_manager = bg_thread->mem_manager();
            uint64_t key = bg_thread->thread_id();

            ParsedInternalKey ik;
            Slice user_key;
            bool insert = true;
            meta->smallest.DecodeFrom(iter->key());
            int nentries = 0;
            uint64_t file_size = 0;
            for (; iter->Valid(); iter->Next()) {
                insert = true;
                Slice key = iter->key();
                if (options.prune_memtable_before_flushing) {
                    NOVA_ASSERT(ParseInternalKey(key, &ik));
                    if (user_key.empty()) {
                        user_key = ik.user_key;
                    } else {
                        if (options.comparator->Compare(ik.user_key,
                                                        user_key) == 0) {
                            insert = false;
                        }
                    }
                }
                if (insert) {
                    nentries += 1;
                    file_size += (key.size() + iter->value().size());
                    meta->largest.DecodeFrom(key);
                }

            }

            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format(
                        "!!!!!!!!!!!!!!!!!!!!! CompactMemTable tid:{} alloc_size:{} nentries:{} nblocks:{}",
                        key, options.max_stoc_file_size, nentries, -1);

            // Finish and check for builder errors
            meta->file_size = file_size;
            meta->converted_file_size = meta->file_size;
            assert(meta->file_size > 0);
            // Make sure WRITEs are complete before we persist them.
            meta->converted_file_size = file_size;
        }

        // Check for input iterator errors
        if (!iter->status().ok()) {
            s = iter->status();
        }
        return s;
    }

    Status
    BuildTable(const std::string &dbname, Env *env, const Options &options,
               TableCache *table_cache, Iterator *iter, FileMetaData *meta,
               EnvBGThread *bg_thread, bool prune_memtables) {
        Status s;
        meta->file_size = 0;
        iter->SeekToFirst();
        std::string filename = TableFileName(dbname, meta->number, FileInternalType::kFileData, 0);
        if (iter->Valid()) {
            const Comparator *user_comp = reinterpret_cast<const InternalKeyComparator *>(options.comparator)->user_comparator();
            MemManager *mem_manager = bg_thread->mem_manager();
            StoCWritableFileClient *stoc_writable_file = new StoCWritableFileClient(
                    env,
                    options,
                    meta->number,
                    mem_manager,
                    bg_thread->stoc_client(),
                    dbname,
                    bg_thread->thread_id(),
                    options.max_stoc_file_size,
                    bg_thread->rand_seed(),
                    filename);
            WritableFile *file = new MemWritableFile(stoc_writable_file);
            TableBuilder *builder = new TableBuilder(options, file);

            Slice user_key;
            bool insert = true;
            meta->smallest.DecodeFrom(iter->key());
            for (; iter->Valid(); iter->Next()) {
                insert = true;
                Slice key = iter->key();
                if (prune_memtables) {
                    Slice ukey = ExtractUserKey(key);
                    if (!user_key.empty() && user_comp->Compare(ukey, user_key) == 0) {
                        insert = false;
                    }
                    user_key = ukey;
                }
                if (insert) {
                    meta->largest.DecodeFrom(key);
                    builder->Add(key, iter->value());
                }
            }
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format(
                        "!!!!!!!!!!!!!!!!!!!!! CompactMemTable tid:{} alloc_size:{} nentries:{} nblocks:{}",
                        bg_thread->thread_id(), options.max_stoc_file_size,
                        builder->NumEntries(),
                        builder->NumDataBlocks());

            // Finish and check for builder errors
            s = builder->Finish();
            if (s.ok()) {
                meta->file_size = builder->FileSize();
                meta->converted_file_size = meta->file_size;
                assert(meta->file_size > 0);

                stoc_writable_file->set_meta(*meta);
                stoc_writable_file->set_num_data_blocks(builder->NumDataBlocks());
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
            stoc_writable_file->WaitForPersistingDataBlocks();
            uint32_t new_file_size = stoc_writable_file->Finalize();

            meta->block_replica_handles = stoc_writable_file->replicas();
            meta->parity_block_handle = stoc_writable_file->parity_block_handle();
            meta->converted_file_size = new_file_size;
            stoc_writable_file->Validate(meta->block_replica_handles, meta->parity_block_handle);

            delete stoc_writable_file;
            stoc_writable_file = nullptr;
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
            env->DeleteFile(filename);
        }
        return s;
    }

}  // namespace leveldb
