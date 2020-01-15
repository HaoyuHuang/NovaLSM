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
            uint32_t scid = mem_manager->slabclassid(key,
                                                     options.write_buffer_size +
                                                     options.table_appendum_size);
            char *buf = options.bg_thread->mem_manager()->ItemAlloc(key, scid);
            NovaCCRemoteMemFile *cc_file = new NovaCCRemoteMemFile(env, fname,
                                                                   mem_manager,
                                                                   options.bg_thread->dc_client(),
                                                                   dbname, buf,
                                                                   options.bg_thread->thread_id(),
                                                                   options.write_buffer_size +
                                                                   options.table_appendum_size);
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

            delete file;
            file = nullptr;

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
