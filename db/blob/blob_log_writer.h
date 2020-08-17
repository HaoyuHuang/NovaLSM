//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#ifndef ROCKSDB_LITE

#include <cstdint>
#include <memory>
#include <string>

#include "db/blob/blob_log_format.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class WritableFileWriter;

/**
 * BlobLogWriter is the blob log stream writer. It provides an append-only
 * abstraction for writing blob data.
 *
 *
 * Look at blob_db_format.h to see the details of the record formats.
 */

class BlobLogWriter {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this BlobLogWriter is in use.
  BlobLogWriter(std::unique_ptr<WritableFileWriter>&& dest, Env* env,
                Statistics* statistics, uint64_t log_number, bool use_fsync,
                uint64_t boffset = 0);
  // No copying allowed
  BlobLogWriter(const BlobLogWriter&) = delete;
  BlobLogWriter& operator=(const BlobLogWriter&) = delete;

  ~BlobLogWriter() = default;

  static void ConstructBlobHeader(std::string* buf, const Slice& key,
                                  const Slice& val, uint64_t expiration);

  Status AddRecord(const Slice& key, const Slice& val, uint64_t* key_offset,
                   uint64_t* blob_offset);

  Status AddRecord(const Slice& key, const Slice& val, uint64_t expiration,
                   uint64_t* key_offset, uint64_t* blob_offset);

  Status EmitPhysicalRecord(const std::string& headerbuf, const Slice& key,
                            const Slice& val, uint64_t* key_offset,
                            uint64_t* blob_offset);

  Status AppendFooter(BlobLogFooter& footer);

  Status WriteHeader(BlobLogHeader& header);

  WritableFileWriter* file() { return dest_.get(); }

  const WritableFileWriter* file() const { return dest_.get(); }

  uint64_t get_log_number() const { return log_number_; }

  Status Sync();

 private:
  std::unique_ptr<WritableFileWriter> dest_;
  Env* env_;
  Statistics* statistics_;
  uint64_t log_number_;
  uint64_t block_offset_;  // Current offset in block
  bool use_fsync_;

 public:
  enum ElemType { kEtNone, kEtFileHdr, kEtRecord, kEtFileFooter };
  ElemType last_elem_type_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
