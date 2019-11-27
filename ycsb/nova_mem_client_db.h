
//
// Created by Haoyu Huang on 2/20/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef YCSB_C_NOVA_MEM_CLIENT_DB_H
#define YCSB_C_NOVA_MEM_CLIENT_DB_H

#include "db.h"

#include <iostream>
#include <string>
#include <mutex>
#include <vector>
#include <random>
#include "properties.h"

using std::cout;
using std::endl;

#define REQ_TERMINATER_CHAR '!'
#define REQ_DELIMITER_CHAR '?'
#define NOVA_REQ_GET 'g'
#define NOVA_REQ_GET_MISS 'M'
#define NOVA_REQ_GET_HIT 'H'
#define NOVA_REQ_SET 's'
#define NOVA_REQ_SET_RESPONSE 'S'

#define NOVA_BUFFER_SIZE 8192

namespace ycsbc {

    enum RDMAMode {
        NONE, INGEST
    };

    class NovaMemClientDB : public DB {
    public:
        NovaMemClientDB(utils::Properties &props) : props_(props) {}

        NovaMemClientDB() = default;

        ~NovaMemClientDB() = default;

        void Init();

        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Delete(const std::string &table, const std::string &key);

    private:
        int hash(const std::string &key);

        int read_response(int fd);

        int readYCSB(int serverId, int key, int offset, int size);

        utils::Properties props_;
        std::vector<std::string> nova_cache_ips_;
        std::vector<int> nova_client_sockets_;
        int expected_value_size;
        char *data_;
        RDMAMode mode_;
        int recordcount_per_server_;
        double *shedLoad;
    };

} // ycsbc

#endif //YCSB_C_NOVA_MEM_CLIENT_DB_H
