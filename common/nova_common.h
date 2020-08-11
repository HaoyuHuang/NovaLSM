
//
// Created by Haoyu Huang on 4/1/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef NOVA_COMMON_H
#define NOVA_COMMON_H


#pragma once

#include <stddef.h>
#include <stdint.h>
#include <assert.h>
#include <vector>
#include <atomic>
#include "city_hash.h"

#include <string>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <event.h>
#include <list>

#include "include/port/port_config.h"

#include "rdma/rdma_ctrl.hpp"
#include "table/format.h"
#include "util/coding.h"
#include <stdexcept>

namespace nova {
#define RDMA_POLL_MIN_TIMEOUT_US 10
#define RDMA_POLL_MAX_TIMEOUT_US 10
#define LEVELDB_TABLE_PADDING_SIZE_MB 2
#define MAX_BLOCK_SIZE 10240
    using namespace std;
    using namespace rdmaio;

    class NovaGlobalVariables {
    public:
        void Initialize() {
            stoc_pending_disk_reads = 0;
            stoc_pending_disk_writes = 0;
            stoc_queue_depth = 0;
            generated_memtable_sizes = 0;
            written_memtable_sizes = 0;
            total_disk_writes = 0;
            total_disk_reads = 0;
            is_ready_to_process_requests = false;
        }

        // DC stats
        std::atomic_int_fast64_t stoc_pending_disk_writes;
        std::atomic_int_fast64_t stoc_pending_disk_reads;
        std::atomic_int_fast64_t stoc_queue_depth;

        std::atomic_int_fast64_t generated_memtable_sizes;
        std::atomic_int_fast64_t written_memtable_sizes;
        std::atomic_int_fast64_t total_disk_writes;
        std::atomic_int_fast64_t total_disk_reads;
        std::atomic_bool is_ready_to_process_requests;
        static NovaGlobalVariables global;
    };

    enum NovaRDMAPartitionMode {
        RANGE = 0,
        HASH = 1,
        DEBUG_RDMA = 2
    };

    enum NovaLogRecordMode {
        LOG_LOCAL = 0,
        LOG_RDMA = 1,
        LOG_NIC = 2,
        LOG_NONE = 3,
    };

    struct RangePartition {
        uint64_t key_start;
        uint64_t key_end;
    };

    struct LTCFragment {
        LTCFragment();

        std::string DebugString();

        // for range partition only.
        RangePartition range;
        uint32_t dbid;
        uint32_t ltc_server_id;
        std::vector<uint32_t> log_replica_stoc_ids;
        void *db = nullptr;

        std::atomic_bool is_stoc_migrated_;
        std::atomic_bool is_complete_;
        std::atomic_bool is_ready_;
        leveldb::port::Mutex is_ready_mutex_;
        leveldb::port::CondVar is_ready_signal_;
    };

    uint64_t keyhash(const char *key, uint64_t nkey);

    enum ConnState {
        READ, WRITE
    };

    enum SocketState {
        INCOMPLETE, COMPLETE, CLOSED
    };

    void MarkCharAsWaitingForRDMAWRITE(char *ptr, uint32_t size);

    bool IsRDMAWRITEComplete(char *ptr, uint32_t size);

    class Connection {
    public:
        int fd;
//        int req_ind;
        int req_size;
        int response_ind;
        uint32_t response_size;
//        char *request_buf;
        char *response_buf = nullptr; // A pointer points to the response buffer.
        ConnState state;
        void *worker;
        struct event event;
        int event_flags;
//    bool try_free_entry_after_transmit_response = false;
//    IndexEntry index_entry;
//    DataEntry data_entry;
        uint32_t number_get_retries = 0;

        void Init(int f, void *store);

        void UpdateEventFlags(int new_flags);
    };

    SocketState socket_read_handler(int fd, short which, Connection *conn);

    bool process_socket_request_handler(int fd, Connection *conn);

    void write_socket_complete(int fd, Connection *conn);

    SocketState socket_write_handler(int fd, Connection *conn);

#define EPOLL_MAX_EVENT 1024
#define EPOLL_WAIT_TIME 0

    enum RequestType : char {
        GET = 'g',
        PUT = 'p',
        VERIFY_LOAD = 'v',
        REQ_SCAN = 'r',
        REDIRECT = 'r',
        GET_INDEX = 'i',
        EXISTS = 'h',
        MISS = 'm',
        REINITIALIZE_QP = 'a',
        CLOSE_STOC_FILES = 'c',
        STATS = 's',
        CHANGE_CONFIG = 'b',
        QUERY_CONFIG_CHANGE = 'R',
    };

    static RequestType char_to_req_type(char c) {
        switch (c) {
            case 'g':
                return GET;
            case 'p' :
                return PUT;
            case 'r' :
                return REDIRECT;
            case 'i':
                return GET_INDEX;
        }
        NOVA_ASSERT(false) << "Unknown request type " << c;
    }

    std::vector<std::string>
    SplitByDelimiter(std::string *s, std::string delimiter);

    std::vector<uint32_t>
    SplitByDelimiterToInt(std::string *s, std::string delimiter);

    std::string ToString(const std::vector<uint32_t> &x);

    std::string
    DBName(const std::string &dbname, uint32_t index);

    void ParseDBIndexFromDBName(const std::string &dbname,
                                uint32_t *index);

    void mkdirs(const char *dir);

    enum ResponseType : char {
        GETR = 'G',
        PUTR,
        REDIRECTR
    };

#define TERMINATER_CHAR '!'
#define MSG_TERMINATER_CHAR '\n'
#define GRH_SIZE 40
#define EWOULDBLOCK_SLEEP 10000
#define DEBUG_KEY_SIZE 36
#define DEBUG_VALUE_SIZE 365
#define CONN_SLEEP 50000

#define NOVA_LIST_BACK_ARRAY_SIZE 32
#define NOVA_MAX_CONN 1000000

#define CUCKOO_SEGMENT_SIZE_MB 1

#define CHUNK_ALIGN_BYTES 8
#define MAX_EVICT_CANDIDATES 8
#define MAX_CUCKOO_BUMPS 5

/* Initial power multiplier for the hash table */
#define HASHPOWER_DEFAULT 16

//    class Semaphore {
//    public:
//        Semaphore(int count_ = 0)
//                : count(count_) {}
//
//        inline void notify() {
//            std::unique_lock<std::mutex> lock(mtx);
//            count++;
//            cv.notify_all();
//        }
//
//        inline void wait() {
//            std::unique_lock<std::mutex> lock(mtx);
//            while (count == 0) {
//                cv.wait(lock);
//            }
//            count--;
//        }
//
//    private:
//        std::mutex mtx;
//        std::condition_variable cv;
//        int count = 0;
//    };


    uint32_t fastrand();

// Hash related.
    uint32_t MurmurHash3(const void *key, uint64_t length);

    uint32_t jenkins_hash(const void *key, uint64_t length);

    uint64_t tab_hash(const char *key, size_t len);

    uint64_t sbox_hash(char *key, size_t len);

    uint64_t noop_hash(const char *key, size_t len);

    uint64_t mul_hash(const char *key, size_t len);

    std::string ibv_wr_opcode_str(ibv_wr_opcode code);

    std::string ibv_wc_opcode_str(ibv_wc_opcode code);

// MD4 truncated to 12 B
//#include <openssl/md4.h>
//
//static uint64_t hash_md4(const char *key, size_t len) {
//    size_t temp_hash[(MD4_DIGEST_LENGTH + sizeof(size_t) - 1) / sizeof(size_t)];
//    MD4(key, len, (uint8_t *) temp_hash);
//    assert(8 <= MD4_DIGEST_LENGTH);
//    return *(size_t *) temp_hash;
//}

    uint64_t cityhash(const char *key, size_t len);

// utility functions.

    uint32_t safe_mod(uint32_t key, uint32_t n);

    uint32_t nint_to_str(uint64_t x);

    uint32_t int_to_str(char *str, uint64_t x);

    inline uint32_t
    str_to_int(const char *str, uint64_t *out, uint32_t nkey = 0) {
        if (str[0] == MSG_TERMINATER_CHAR) {
            return 0;
        }
        uint32_t len = 0;
        uint64_t x = 0;
        while (str[len] != TERMINATER_CHAR) {
            if (nkey != 0 && len == nkey) {
                break;
            }
            if (str[len] > '9' || str[len] < '0') {
                break;
            }
            x = x * 10 + (str[len] - '0');
            len += 1;
        }
        *out = x;
        return len + 1;
    }

    inline std::string
    LogFileName(uint32_t db_id, uint32_t memtableid) {
        return fmt::format("{}-{}", db_id, memtableid);
    }

    inline void
    ParseDBIndexFromLogFileName(const std::string &logname, uint32_t *index) {
        uint32_t data = 0;
        int i = 0;
        while (i < logname.size()) {
            if (logname[i] == '-') {
                *index = data;
                i++;
                break;
            }
            data = data * 10 + logname[i] - '0';
            i++;
        }
    }

    inline void
    ParseDBIndexFromLogFileName(const std::string &logname, uint32_t *dbindex, uint32_t *memtableid) {
        uint32_t data = 0;
        int i = 0;
        while (i < logname.size()) {
            if (logname[i] == '-') {
                *dbindex = data;
                i++;
                break;
            }
            data = data * 10 + logname[i] - '0';
            i++;
        }
        data = 0;
        while (i < logname.size()) {
            if (logname[i] == '-') {
                *memtableid = data;
                return;
            }
            data = data * 10 + logname[i] - '0';
            i++;
        }
        *memtableid = data;
    }

    int
    GenerateRDMARequest(RequestType req_type, char *buf,
                        uint64_t from_server_id,
                        uint64_t from_sock_fd, char *key, uint64_t nkey);

    uint32_t
    ParseRDMARequest(char *buf, RequestType *req_type, uint64_t *from_server_id,
                     uint64_t *from_sock_fd, char **key, uint64_t *nkey);

    struct Host {
        uint32_t server_id;
        string ip;
        int port;

        std::string DebugString() const;
    };

    struct Servers {
        std::vector<uint32_t> servers;
        std::set<uint32_t> server_ids;
    };

    struct QPEndPoint {
        Host host;
        uint32_t server_id;
        uint32_t thread_id;
    };

    Host convert_host(string host_str);

    vector<Host> convert_hosts(string hosts_str);

// Index entry and data entry.
    struct IndexEntry {
        // Immutable fields.
        uint8_t type = 0;
        uint32_t slab_class_id = 0;
        uint64_t hash = 0;
        uint64_t data_size = 0;
        uint64_t data_ptr = 0;
        uint64_t checksum = 0;

        // Mutable fields.
        uint32_t time = 0;       /* least recent access */

        bool empty() {
            return data_ptr == 0;
        }

        static uint64_t immutable_size() {
            return sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint64_t) * 4;
        }

        static uint64_t size() {
            return sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint64_t) * 4 +
                   sizeof(uint32_t);
        }

        static IndexEntry chars_to_indexitem(char *buf) {
            IndexEntry it{};
            char *tmp = buf;
            memcpy(&it.type, tmp, sizeof(uint8_t));
            tmp += sizeof(uint8_t);
            memcpy(&it.slab_class_id, tmp, sizeof(uint32_t));
            tmp += sizeof(uint32_t);
            memcpy(&it.hash, tmp, sizeof(uint64_t));
            tmp += sizeof(uint64_t);
            memcpy(&it.data_size, tmp, sizeof(uint64_t));
            tmp += sizeof(uint64_t);
            memcpy(&it.data_ptr, tmp, sizeof(uint64_t));
            tmp += sizeof(uint64_t);
            memcpy(&it.checksum, tmp, sizeof(uint64_t));
            tmp += sizeof(uint64_t);
            memcpy(&it.time, tmp, sizeof(uint32_t));
            return it;
        }

        static void indexitem_to_chars(char *_base, const IndexEntry &entry) {
            char *buf = _base;
            memcpy(buf, &entry.type, sizeof(uint8_t));
            buf += sizeof(uint8_t);
            memcpy(buf, &entry.slab_class_id, sizeof(uint32_t));
            buf += sizeof(uint32_t);
            memcpy(buf, &entry.hash, sizeof(uint64_t));
            buf += sizeof(uint64_t);
            memcpy(buf, &entry.data_size, sizeof(uint64_t));
            buf += sizeof(uint64_t);
            memcpy(buf, &entry.data_ptr, sizeof(uint64_t));
            buf += sizeof(uint64_t);
            memcpy(buf, &entry.checksum, sizeof(uint64_t));
            buf += sizeof(uint64_t);
            memcpy(buf, &entry.time, sizeof(uint32_t));
        }

        void write_type(char *_base, uint8_t _type) {
            type = _type;
            memcpy(_base, &type, sizeof(uint8_t));
        }

        void write_time(char *_base, uint32_t _time) {
            time = _time;
            char *ptr = _base + immutable_size();
            memcpy(ptr, &time, sizeof(uint32_t));
        }

        uint64_t compute_checksum(char *_base) const {
            return cityhash(_base, immutable_size() - sizeof(uint64_t));
        }

        void compute_and_write_checksum(char *_base) const {
            uint64_t checksum = compute_checksum(_base);
            char *buf = _base;
            buf += immutable_size() - sizeof(uint64_t);
            memcpy(buf, &checksum, sizeof(uint64_t));
        }
    };

// Memory layout:
// stale, refs, nkey, key, nval (string representation + 1), 'h', val, checksum.
// data points to the beginning of the backing array.
    struct DataEntry {
        uint8_t stale = 0;
        uint32_t refs = 1; /*ref count*/
        uint32_t nkey = 0;       /* key length, w/terminating null and padding */
        uint32_t nval = 0;     /* size of data */
        uint64_t data = 0; /*pointer to the beginning of the char array.*/
        uint64_t checksum = 0; // covers nkey - data.

        static uint32_t sizeof_data_entry(uint32_t nkey, uint32_t nval) {
            return sizeof(uint8_t) + sizeof(uint32_t) * 2 + nint_to_str(nval) +
                   1 +
                   1 +
                   nkey + nval +
                   sizeof(uint64_t);
        }

        uint32_t size() {
            return sizeof(uint8_t) + sizeof(uint32_t) * 2 + nint_to_str(nval) +
                   1 +
                   1 + nkey + nval;
        }

        uint64_t compute_checksum(char *buf) {
            char *tmp = buf;
            tmp += sizeof(uint8_t);
            tmp += sizeof(uint32_t);
            return cityhash(tmp,
                            sizeof(uint32_t) + nint_to_str(nval) + 1 +
                            1 + nkey + nval);
        }

        char *user_key() const {
            char *base = (char *) data;
            base += sizeof(uint8_t);
            base += sizeof(uint32_t);
            base += sizeof(uint32_t);
            return base;
        }

        char *user_value() const {
            char *base = (char *) data;
            base += sizeof(uint8_t);
            base += sizeof(uint32_t);
            base += sizeof(uint32_t);
            base += nkey;
            base += nint_to_str(nval);
            base += 1;
            base += 1;
            return base;
        }

        char *value() const {
            char *base = (char *) data;
            base += sizeof(uint8_t);
            base += sizeof(uint32_t);
            base += sizeof(uint32_t);
            base += nkey;
            return base;
        }

        static void
        dataitem_to_chars(char *buf, char *key, uint32_t nkey, const char *val,
                          uint32_t nval, uint8_t stale) {
            char *start = buf;
            start += sizeof(uint8_t);
            start += sizeof(uint32_t);
            memcpy(buf, &stale, sizeof(uint8_t));
            buf += sizeof(uint8_t);

            uint32_t refs = 1;
            memcpy(buf, &refs, sizeof(uint32_t));
            buf += sizeof(uint32_t);

            memcpy(buf, &nkey, sizeof(uint32_t));
            buf += sizeof(uint32_t);

            memcpy(buf, key, nkey);
            buf += nkey;
            // string representation of nval, including the request type.
            uint64_t nval_str_len = int_to_str(buf, nval + 1);
            buf += nval_str_len;

            buf[0] = RequestType::EXISTS;
            buf += 1;

            memcpy(buf, val, nval);
            buf += nval;
            uint64_t checksum = cityhash(start,
                                         sizeof(uint32_t) + nval_str_len + 1 +
                                         nkey + nval);
            memcpy(buf, &checksum, sizeof(uint64_t));
        }

        static DataEntry chars_to_dataitem(char *buf) {
            DataEntry it{};
            it.data = (uint64_t) buf;
            char *base = buf;
            memcpy(&it.stale, base, sizeof(uint8_t));
            base += sizeof(uint8_t);
            memcpy(&it.refs, base, sizeof(uint32_t));
            base += sizeof(uint32_t);
            memcpy(&it.nkey, base, sizeof(uint32_t));
            base += sizeof(uint32_t);
            base += it.nkey;
            uint64_t nval = 0;
            uint32_t nval_str_len = str_to_int(base, &nval);
            it.nval = static_cast<uint32_t>(nval - 1);
            base += nval_str_len;
            base += 1;
            base += it.nval;
            memcpy(&it.checksum, base, sizeof(uint64_t));
            return it;
        }

        void write_stale(char *base) {
            stale = 1;
            memcpy(base, &stale, sizeof(uint8_t));
        }

        uint32_t get_refs() const {
            char *base = (char *) data;
            base += sizeof(uint8_t);
            uint32_t refs = 0;
            memcpy(&refs, base, sizeof(uint32_t));
            return refs;
        }

        uint32_t increment_ref_count() const {
            char *base = (char *) data;
            base += sizeof(uint8_t);
            uint32_t refs = 0;
            memcpy(&refs, base, sizeof(uint32_t));
            refs += 1;
            memcpy(base, &refs, sizeof(uint32_t));
            return refs;
        }

        uint32_t decrement_ref_count() const {
            char *base = (char *) data;
            base += sizeof(uint8_t);
            uint32_t refs = 0;
            memcpy(&refs, base, sizeof(uint32_t));
            refs -= 1;
            memcpy(base, &refs, sizeof(uint32_t));
            return refs;
        }

        bool empty() {
            return nkey == 0;
        }
    };

    inline uint32_t
    LogRecordSize(const leveldb::LevelDBLogRecord &record) {
        uint32_t size = 0;
        size += 4;
        size += 4;
        size += record.key.size();
        size += 4;
        size += record.value.size();
        size += 8;
        size += 1;
        return size;
    }

    inline uint32_t
    LogRecordsSize(const std::vector<leveldb::LevelDBLogRecord> &log_records) {
        uint32_t size = 0;
        for (const auto &record : log_records) {
            size += LogRecordSize(record);
        }
        return size;
    }

    inline uint32_t
    EncodeLogRecord(char *buf,
                    const leveldb::LevelDBLogRecord &record) {
        uint32_t size = 0;
        uint32_t record_size = LogRecordSize(record);
        size += leveldb::EncodeFixed32(buf + size, record_size);
        size += leveldb::EncodeSlice(buf + size, record.key);
        size += leveldb::EncodeSlice(buf + size, record.value);
        size += leveldb::EncodeFixed64(buf + size, record.sequence_number);
        // The last byte is 1.
        buf[size] = 1;
        size++;
        return size;
    }

    inline bool DecodeLogRecord(leveldb::Slice *buf,
                                leveldb::LevelDBLogRecord *log_record) {
        uint32_t record_size;
        if (!leveldb::DecodeFixed32(buf, &record_size)) {
            return false;
        }
        if (!leveldb::DecodeStr(buf, &log_record->key, false)) {
            return false;
        }
        if (!leveldb::DecodeStr(buf, &log_record->value, false)) {
            return false;
        }
        if (!leveldb::DecodeFixed64(buf, &log_record->sequence_number)) {
            return false;
        }
        if ((*buf)[0] != 1) {
            return false;
        }
        if (record_size != LogRecordSize(*log_record)) {
            return false;
        }
        *buf = leveldb::Slice(buf->data() + 1, buf->size() - 1);
        return true;
    }
}
#endif //NOVA_COMMON_H
