
//
// Created by Haoyu Huang on 4/1/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef RLIB_NOVA_COMMON_H
#define RLIB_NOVA_COMMON_H


#pragma once

#include <stddef.h>
#include <stdint.h>
#include <assert.h>

#include <string>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <event.h>
#include "logging.hpp"
#include "table/format.h"
#include "util/coding.h"

namespace nova {
    using namespace std;

    enum ConnState {
        READ, WRITE
    };

    enum SocketState {
        INCOMPLETE, COMPLETE, CLOSED
    };


    class Connection {
    public:
        int fd;
        int req_size;
        int response_ind;
        uint32_t response_size;
//        char *request_buf;
//        int req_ind;
//        char *buf; // buf used for responses.

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
        REQ_SCAN = 'r',
        ALLOCATE_LOG_BUFFER = 'a',
        ALLOCATE_LOG_BUFFER_SUCC = 'A',
        DELETE_LOG_FILE = 'd',
        DELETE_LOG_FILE_SUCC = 'D',
        REPLICATE_LOG_RECORD = 'l',
        REPLICATE_LOG_RECORD_SUCC = 'L',
        REDIRECT = 'r',
        GET_INDEX = 'i',
        EXISTS = 'h',
        MISS = 'm',
        FORCE_GET = 'G',
        STATS = 's',
        DRAIN = 'S',
    };


    std::vector<std::string>
    SplitByDelimiter(std::string *s, std::string delimiter);

    std::string ToString(const std::vector<uint32_t> &x);

    std::string DBName(const std::string &dbname, uint32_t sid, uint32_t index);

    void
    ParseDBName(const std::string &logname, uint32_t *sid, uint32_t *index);

    uint64_t LogFileHash(const std::string &logname);

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

#define SLAB_SIZE_MB 2
#define CUCKOO_SEGMENT_SIZE_MB 1

#define MAX_NUMBER_OF_SLAB_CLASSES 64
#define SLAB_SIZE_FACTOR 1.25
#define CHUNK_ALIGN_BYTES 8
#define MAX_EVICT_CANDIDATES 8
#define MAX_CUCKOO_BUMPS 5

/* Initial power multiplier for the hash table */
#define HASHPOWER_DEFAULT 16

    class Semaphore {
    public:
        Semaphore(int count_ = 0)
                : count(count_) {}

        inline void notify() {
            std::unique_lock<std::mutex> lock(mtx);
            count++;
            cv.notify_all();
        }

        inline void wait() {
            std::unique_lock<std::mutex> lock(mtx);
            while (count == 0) {
                cv.wait(lock);
            }
            count--;
        }

    private:
        std::mutex mtx;
        std::condition_variable cv;
        int count = 0;
    };

    uint32_t nint_to_str(uint64_t x);

    uint32_t int_to_str(char *str, uint64_t x);

    uint32_t str_to_int(const char *str, uint64_t *out, uint32_t nkey = 0);

    struct Host {
        string ip;
        int port;
    };

    struct QPEndPoint {
        Host host;
        uint32_t thread_id;
    };

    Host convert_host(string host_str);

    vector<Host> convert_hosts(string hosts_str);

}
#endif //RLIB_NOVA_COMMON_H
