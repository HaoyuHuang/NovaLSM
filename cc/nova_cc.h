
//
// Created by Haoyu Huang on 1/11/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_NOVA_CC_H
#define LEVELDB_NOVA_CC_H

#include <semaphore.h>

#include "util/env_mem.h"
#include "nova_cc_client.h"
#include "leveldb/env.h"
#include "leveldb/table.h"

#define PERSIST_META_BLOCKS_TO_RTABLE true

namespace leveldb {

    struct DCStatsStatus {
        uint32_t remote_dc_id = 0;
        uint32_t req_id = 0;
        CCResponse response;
    };

    class NovaCCMemFile : public MemFile {
    public:
        NovaCCMemFile(Env *env,
                      const Options &options,
                      uint64_t file_number,
                      MemManager *mem_manager,
                      CCClient *cc_client,
                      const std::string &dbname,
                      uint64_t thread_id,
                      uint64_t file_size, unsigned int *rand_seed);

        ~NovaCCMemFile();

        uint64_t Size() const override { return used_size_; }

        Status
        Read(uint64_t offset, size_t n, Slice *result, char *scratch) override;

        Status Write(uint64_t offset, const Slice &data) override;

        Status Append(const Slice &data) override;

        char * Buf();

        Status Append(uint32_t size);

        Status Fsync() override;

        Status SyncAppend(const Slice& data);

        void Format();

        void WaitForPersistingDataBlocks();

        uint32_t Finalize();

        const std::string &sstable_id() {
            return fname_;
        }

        const char *backing_mem() override { return backing_mem_; }

        void set_meta(const FileMetaData &meta) { meta_ = meta; }

        uint64_t used_size() { return used_size_; }

        uint64_t allocated_size() { return allocated_size_; }

        uint64_t thread_id() { return thread_id_; }

        uint64_t file_number() { return file_number_; }

        void set_num_data_blocks(uint32_t num_data_blocks) {
            num_data_blocks_ = num_data_blocks;
        }

        RTableHandle meta_block_handle() {
            return meta_block_handle_;
        }

        std::vector<RTableHandle> rhs() {
            std::vector<RTableHandle> rhs;
            for (int i = 0; i < status_.size(); i++) {
                rhs.push_back(status_[i].result_handle);
            }
            return rhs;
        }

    private:
        struct PersistStatus {
            uint32_t remote_server_id = 0;
            uint32_t WRITE_req_id = 0;
            RTableHandle result_handle;
        };

        uint32_t WriteBlock(BlockBuilder *block, uint64_t offset);

        uint32_t WriteRawBlock(const Slice &block_contents,
                               CompressionType type,
                               uint64_t offset);

        Env *env_;
        unsigned int *rand_seed_;
        uint64_t file_number_;
        const std::string fname_;
        MemManager *mem_manager_;
        CCClient *cc_client_;
        const std::string &dbname_;
        FileMetaData meta_;
        uint64_t thread_id_;

        Block *index_block_;
        int num_data_blocks_;
        const Options &options_;

        char *backing_mem_ = nullptr;
        uint64_t allocated_size_ = 0;
        uint64_t used_size_ = 0;
        std::vector<int> nblocks_in_group_;
        std::vector<DCStatsStatus> dc_stats_status_;
        std::vector<PersistStatus> status_;
        RTableHandle meta_block_handle_;
    };

    class NovaCCRandomAccessFile : public CCRandomAccessFile {
    public:
        NovaCCRandomAccessFile(Env *env, const std::string &dbname,
                               uint64_t file_number,
                               const FileMetaData &meta,
                               CCClient *dc_client,
                               MemManager *mem_manager,
                               uint64_t thread_id,
                               bool prefetch_all);

        ~NovaCCRandomAccessFile() override;

        Status
        Read(const RTableHandle &rtable_handle, uint64_t offset, size_t n,
             Slice *result, char *scratch) override;

        Status
        Read(const ReadOptions &read_options, const RTableHandle &rtable_handle,
             uint64_t offset, size_t n,
             Slice *result, char *scratch) override;

    private:
        struct DataBlockRTableLocalBuf {
            uint64_t offset;
            uint32_t size;
            uint64_t local_offset;
        };

        Status ReadAll(CCClient *dc_client);

        const std::string &dbname_;
        uint64_t file_number_;
        FileMetaData meta_;

        bool prefetch_all_ = false;
        char *backing_mem_table_ = nullptr;

        std::map<uint64_t, DataBlockRTableLocalBuf> rtable_local_offset_;
        std::mutex mutex_;

        MemManager *mem_manager_ = nullptr;
        uint64_t thread_id_ = 0;
        uint32_t dbid_ = 0;
        Env *env_ = nullptr;
        RandomAccessFile *local_ra_file_ = nullptr;
    };

    struct DeleteTableRequest {
        std::string dbname;
        uint32_t file_number;
    };
}


#endif //LEVELDB_NOVA_CC_H
