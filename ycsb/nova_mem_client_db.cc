
//
// Created by Haoyu Huang on 2/20/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "nova_mem_client_db.h"
#include "logging.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sstream>
#include <strings.h>
#include <iostream>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <arpa/inet.h>

void error(const char *msg) {
    perror(msg);
    exit(0);
}

using namespace std;

int str_to_int(char *str, int *out) {
    int len = 0, x = 0;
    while (str[0] != '!') {
        if (str[0] > '9' || str[0] < '0') {
            break;
        }
        x = x * 10 + (str[0] - '0');
        len += 1;
        str += 1;
    }
    *out = x;
    return len + 1;
}

int nint_to_str(int x) {
    int len = 0;
    do {
        x = x / 10;
        len++;
    } while (x);
    return len;
}

//inline __attribute__ ((always_inline))
int int_to_str(char *str, int x) {
    int len = 0, p = 0;
    do {
        str[len] = (x % 10) + '0';
        x = x / 10;
        len++;
    } while (x);
    int q = len - 1, temp;
    while (p < q) {
        temp = str[p];
        str[p] = str[q];
        str[q] = temp;
        p++;
        q--;
    }
    str[len] = '!';
    return len + 1;
}

namespace ycsbc {
    using namespace rdmaio;

    int NovaMemClientDB::hash(const std::string &key) {
        if (nova_client_sockets_.size() == 1) {
            return 0;
        }
        int sid = min((int) nova_client_sockets_.size() - 1,
                      stoi(key, nullptr) / recordcount_per_server_);
        if (mode_ == INGEST) {
            if (sid == 0) {
                int randV = (rand() % 100) + 1;
                for (int i = 0; i < nova_client_sockets_.size(); i++) {
                    if (randV <= shedLoad[i]) {
                        sid = i;
                        break;
                    }
                }
            }
        }
        RDMA_ASSERT(sid >= 0 && sid <= nova_client_sockets_.size());
        return sid;
    }

    int NovaMemClientDB::read_response(int fd) {
        char *buf = data_;
        // read at much as possible for the first time.
        int len = -1;
        int readBytes = read(fd, data_, NOVA_BUFFER_SIZE);
        int count = 0;
        for (int i = 0; i < readBytes; i++) {
            if (data_[i] == '!' && len == -1) {
                str_to_int(data_, &len);
                continue;
            }
            if (len != -1) {
                data_[count] = data_[i];
                count++;
            }
        }
        if (len != -1) {
            buf += count;
        } else {
            if (readBytes > 0) {
                buf += readBytes;
            }
            // read one byte at a time until we know the length.
            do {
                int ret = read(fd, buf, 1);
                if (ret == 1) {
                    readBytes++;
                    buf++;
                }
            } while (data_[readBytes - 1] != '!');
            str_to_int(data_, &len);
        }

        // read the remaining bytes.
        while (count < len) {
            int cnt = read(fd, buf, len - count);
            if (cnt > 0) {
                count += cnt;
                buf += cnt;
            }
        }
        return len;
    }

    void NovaMemClientDB::Init() {
        string client_mode = props_["nova_client_mode"];
        int recordcount = stoi(props_["recordcount"], nullptr);
        int nShedLoad = stoi(props_["shedload"], nullptr);
        expected_value_size = stoi(props_["valuesize"], nullptr);
        std::string ips = props_["nova_servers"];

        if (client_mode.find("client_redirect") != string::npos) {
            mode_ = RDMAMode::INGEST;
        } else {
            mode_ = RDMAMode::NONE;
        }

        data_ = (char *) malloc(NOVA_BUFFER_SIZE);
        memset(data_, 0, NOVA_BUFFER_SIZE);
        std::stringstream ss(ips);
        while (ss.good()) {
            std::string substr;
            getline(ss, substr, ',');

            if (substr.size() == 0) {
                continue;
            }
            nova_cache_ips_.push_back(substr);
            RDMA_LOG(INFO) << substr;
        }


        recordcount_per_server_ = recordcount / nova_cache_ips_.size();
        RDMA_LOG(INFO) << "records per server " << recordcount_per_server_;

        for (int i = 0; i < nova_cache_ips_.size(); i++) {
            string cache = nova_cache_ips_[i];
            std::vector<std::string> ip_port;
            std::stringstream ss_ip_port(cache);
            while (ss_ip_port.good()) {
                std::string substr;
                getline(ss_ip_port, substr, ':');
                ip_port.push_back(substr);
            }

//            for (int j = 0; j < ip_port
//                    .size(); j++std::string &ip : ip_port) {
//
//                RDMA_LOG(INFO) << ip.c_str();
//            }
            int sockfd, portno;
            struct sockaddr_in serv_addr;
            portno = stoi(ip_port[1], nullptr);
            RDMA_ASSERT((sockfd = socket(AF_INET, SOCK_STREAM, 0)) >= 0)
                    << "socket creation error";
            memset(&serv_addr, 0, sizeof(serv_addr));
            serv_addr.sin_family = AF_INET;
            {
                auto server = gethostbyname(ip_port[0].c_str());
                memcpy((char *) &serv_addr.sin_addr.s_addr,
                       (char *) server->h_addr,
                       server->h_length);
            }
            serv_addr.sin_port = htons(portno);
            int ret = connect(sockfd, (struct sockaddr *) &serv_addr,
                              sizeof(serv_addr));
            while (ret != 0) {
                usleep(10000);
                ret = connect(sockfd, (struct sockaddr *) &serv_addr,
                              sizeof(serv_addr));
            }
            RDMA_ASSERT(ret >= 0) << "ERROR connecting " << ret;
            RDMA_LOG(INFO) << "connected to " << cache << " " << sockfd;
            nova_client_sockets_.push_back(sockfd);
        }

        shedLoad = new double[nova_client_sockets_.size()];
        shedLoad[0] = 100.0 - nShedLoad;
        if (nova_client_sockets_.size() > 1) {
            double averageLoadPerServer =
                    nShedLoad / (nova_client_sockets_.size() - 1);
            for (int i = 1; i < nova_client_sockets_.size(); i++) {
                shedLoad[i] = shedLoad[i - 1];
                shedLoad[i] += averageLoadPerServer;
            }
        }
        shedLoad[nova_client_sockets_.size() - 1] = 100;

//        rng_.seed(std::random_device()());
//        dist_ = new std::uniform_int_distribution<std::mt19937::result_type>(1, nova_cache_ips_.size() - 1);
        RDMA_LOG(INFO) << "init completed";
    }

    int NovaMemClientDB::readYCSB(int serverId, int key, int offset,
                                  int valuesize) {
        RDMA_LOG(DEBUG) << "read " << key << " from " << serverId;
        int fd = nova_client_sockets_[serverId];
        int size = 0;
        char *buf = data_;
        buf[0] = 'g';
        buf++;
        size++;
        int n = int_to_str(buf, key);
        buf += n;
        size += n;
        if (offset != -1) {
            n = int_to_str(buf, offset);
            buf += n;
            size += n;
            n = int_to_str(buf, valuesize);
            buf += n;
            size += n;
        }
        buf[0] = '\n';
        size++;
        int written = 0;
        do {
            char *buf = data_ + written;
            int n = write(fd, buf, size - written);
            if (n > 0) {
                written += n;
            }
        } while (written != size);

        int response = read_response(fd);
        RDMA_LOG(DEBUG) << "read " << key << " from " << serverId
                        << " response " << response;
        return response;
    }


    int NovaMemClientDB::Read(const std::string &table, const std::string &key,
                              const std::vector<std::string> *fields,
                              std::vector<KVPair> &result) {
        int store_id = hash(key);
        int intkey = stoi(key, nullptr);
        int len = readYCSB(store_id, intkey, -1, -1);
        char *buf = data_;
        if (buf[0] == 'r') {
            buf++;
            RDMA_ASSERT(store_id == 0);
            int redirect_server_id;
            int remote_offset;
            int remote_size;
            buf += str_to_int(buf, &redirect_server_id);
            buf += str_to_int(buf, &remote_offset);
            buf += str_to_int(buf, &remote_size);

            RDMA_LOG(DEBUG) << " remote offset " << redirect_server_id << " "
                            << remote_offset << " " << remote_size;
            len = readYCSB(redirect_server_id, intkey, remote_offset,
                           remote_size);
        }

        RDMA_ASSERT(len == expected_value_size);
        return DB::kOK;
    }

    int NovaMemClientDB::Scan(const std::string &table, const std::string &key,
                              int len, const std::vector<std::string> *fields,
                              std::vector<std::vector<KVPair>> &result) {
        assert(false);
        return DB::kOK;
    }

    int
    NovaMemClientDB::Update(const std::string &table, const std::string &key,
                            std::vector<KVPair> &values) {

        return DB::kOK;
    }

    int
    NovaMemClientDB::Insert(const std::string &table, const std::string &key,
                            std::vector<KVPair> &values) {

        return DB::kOK;
    }

    int
    NovaMemClientDB::Delete(const std::string &table, const std::string &key) {
        assert(false);
        return DB::kOK;
    }
}